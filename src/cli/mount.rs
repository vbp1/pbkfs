//! Implementation of `pbkfs mount` subcommand.

use std::{
    fs,
    io::Read,
    os::unix::io::{AsRawFd, FromRawFd},
    path::{Path, PathBuf},
    process,
    sync::atomic::{AtomicBool, AtomicI32, Ordering},
    sync::mpsc,
    time::Duration,
};

use clap::{Args, ValueEnum};
use serde_json;
use tracing::{debug, info, instrument, warn};

use crate::{
    backup::{
        chain::{BackupChain, ChainIntegrity},
        metadata::BackupStore,
        BackupType,
    },
    binding::{BindingRecord, DiffDir, LockMarker, LOCK_FILE},
    env_lock,
    fs::{
        fuse,
        overlay::{Layer as OverlayLayer, Overlay},
        MountSession, MountSessionState, MountTarget,
    },
    Error, Result,
};
use ctrlc;

const DEFAULT_LOG_FILE_REL: &str = ".pbkfs/pbkfs.log";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MountMode {
    Console,
    Background,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitMode {
    /// Foreground mode; waiting is implicit and `--wait/--timeout` are ignored.
    Foreground,
    /// Background mode; wait only for phase-1 "worker started".
    StartOnly,
    /// Background mode; wait for mount completion.
    Wait,
    /// Background mode; wait for completion up to the timeout.
    Timeout(Duration),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum LogSink {
    #[default]
    File,
    Journald,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, ValueEnum)]
pub enum LogFormatArg {
    #[default]
    Text,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonDefaults {
    pub pid_visible: PathBuf,
    pub stat_visible: PathBuf,
    pub log_visible: PathBuf,
    pub pid_diff: PathBuf,
    pub stat_diff: PathBuf,
    pub log_diff: PathBuf,
}

#[derive(Debug, Clone, Args, Default)]
pub struct MountArgs {
    /// Path to pg_probackup backup store (pbk_store)
    #[arg(short = 'B', long = "pbk-store")]
    pub pbk_store: Option<PathBuf>,

    /// Path to empty mount target directory (pbk_target)
    #[arg(short = 'D', long = "mnt-path")]
    pub mnt_path: Option<PathBuf>,

    /// Path to writable diff directory (pbk_diff)
    #[arg(short = 'd', long = "diff-dir")]
    pub diff_dir: Option<PathBuf>,

    /// pg_probackup instance name
    #[arg(short = 'I', long = "instance")]
    pub instance: Option<String>,

    /// Target backup id
    #[arg(short = 'i', long = "backup-id")]
    pub backup_id: Option<String>,

    /// Allow mounting corrupt backup chains (dangerous)
    #[arg(long, default_value = "false")]
    pub force: bool,

    /// Skip fsync for datafiles (perf only; unsafe). Leaves .pbkfs-dirty until unmount.
    #[arg(long, default_value = "false")]
    pub perf_unsafe: bool,

    /// Do not persist WAL contents; pg_wal writes become virtual and diff dir is non-resumable.
    #[arg(long, default_value = "false")]
    pub no_wal: bool,

    /// Keep the mount in the foreground (interactive / console mode).
    #[arg(long = "console", default_value_t = false)]
    pub console: bool,

    /// In background mode, wait for mount completion (success/failure). Ignored with `--console`.
    #[arg(long = "wait", default_value_t = false)]
    pub wait: bool,

    /// In background mode, wait for mount completion up to the timeout (seconds). Ignored with `--console`.
    #[arg(long = "timeout")]
    pub timeout: Option<u64>,

    /// Background log sink: explicitly select log file path (host filesystem path).
    #[arg(long = "log-file")]
    pub log_file: Option<PathBuf>,

    /// Log format: text (human) or json.
    #[arg(long = "log-format", value_enum, default_value = "text")]
    pub log_format: LogFormatArg,

    /// Background log sink selection: file (default) or journald.
    #[arg(long = "log-sink", value_enum)]
    pub log_sink: Option<LogSink>,

    /// Override PID file path (relative to mount root or absolute under mnt-path).
    #[arg(long = "pid-file")]
    pub pid_file: Option<PathBuf>,

    /// Force TRACE level regardless of RUST_LOG.
    #[arg(long = "debug", default_value_t = false)]
    pub debug: bool,
}

#[derive(Debug)]
pub struct MountContext {
    pub store: BackupStore,
    pub chain: BackupChain,
    pub diff_dir: DiffDir,
    pub binding: BindingRecord,
    pub overlay: Overlay,
    pub session: MountSession,
    pub fuse_handle: Option<fuse::MountHandle>,
}

impl MountContext {
    pub fn perf_unsafe(&self) -> bool {
        self.overlay.perf_unsafe()
    }

    pub fn flush_dirty(&self) -> Result<()> {
        self.overlay.flush_dirty(false)
    }
}

impl MountArgs {
    pub fn mode(&self) -> MountMode {
        if self.console {
            MountMode::Console
        } else {
            MountMode::Background
        }
    }

    pub fn wait_mode(&self) -> WaitMode {
        if self.console {
            return WaitMode::Foreground;
        }
        if let Some(secs) = self.timeout {
            return WaitMode::Timeout(Duration::from_secs(secs));
        }
        if self.wait {
            return WaitMode::Wait;
        }
        WaitMode::StartOnly
    }

    pub fn daemon_defaults(&self) -> Result<DaemonDefaults> {
        let mnt_path = self
            .mnt_path
            .as_ref()
            .ok_or_else(|| Error::Cli("mnt_path is required".into()))?;
        let diff_dir = self
            .diff_dir
            .as_ref()
            .ok_or_else(|| Error::Cli("diff_dir is required".into()))?;

        let (pid_visible, pid_rel) = resolve_under_mount(
            mnt_path,
            self.pid_file.as_deref(),
            crate::cli::pid::DEFAULT_PID_FILE_REL,
        )?;
        let stat_rel = PathBuf::from(crate::cli::pid::DEFAULT_STAT_FILE_REL);
        let log_rel = PathBuf::from(DEFAULT_LOG_FILE_REL);

        let stat_visible = mnt_path.join(&stat_rel);
        let log_visible = mnt_path.join(&log_rel);

        let diff_data = diff_dir.join("data");
        Ok(DaemonDefaults {
            pid_visible,
            stat_visible,
            log_visible,
            pid_diff: diff_data.join(pid_rel),
            stat_diff: diff_data.join(stat_rel),
            log_diff: diff_data.join(log_rel),
        })
    }
}

fn resolve_under_mount(
    mnt_path: &Path,
    override_path: Option<&Path>,
    default_rel: &str,
) -> Result<(PathBuf, PathBuf)> {
    let rel = match override_path {
        None => PathBuf::from(default_rel),
        Some(path) if path.is_absolute() => {
            if !path.starts_with(mnt_path) {
                return Err(
                    Error::Cli(format!("path must be under mnt_path: {}", path.display())).into(),
                );
            }
            path.strip_prefix(mnt_path)
                .unwrap_or(Path::new(""))
                .strip_prefix("/")
                .unwrap_or(path.strip_prefix(mnt_path).unwrap_or(Path::new("")))
                .to_path_buf()
        }
        Some(path) => path.to_path_buf(),
    };
    Ok((mnt_path.join(&rel), rel))
}

pub fn execute(args: MountArgs) -> Result<()> {
    match args.mode() {
        MountMode::Console => {
            // Foreground: console logging only.
            crate::logging::init_logging(crate::logging::LoggingConfig {
                format: match args.log_format {
                    LogFormatArg::Text => crate::logging::LogFormat::Human,
                    LogFormatArg::Json => crate::logging::LogFormat::Json,
                },
                sink: crate::logging::LogSink::Console,
                debug: args.debug,
            })?;
            run_mount_worker(args, None, false)
        }
        MountMode::Background => run_launcher(args),
    }
}

/// Check if a path is currently mounted (Linux-only, /proc/mounts).
fn is_mounted(path: &Path) -> bool {
    if let Ok(contents) = fs::read_to_string("/proc/mounts") {
        let target = path.to_string_lossy();
        return contents
            .lines()
            .filter_map(|line| line.split_whitespace().nth(1))
            .any(|p| p == target);
    }
    false
}

/// Perform mount orchestration used by both the CLI and tests.
#[instrument(skip(args), fields(mnt = ?args.mnt_path, diff = ?args.diff_dir, store = ?args.pbk_store))]
pub fn mount(args: MountArgs) -> Result<MountContext> {
    let pbk_store = args
        .pbk_store
        .ok_or_else(|| Error::Cli("pbk_store is required".into()))
        .map(canonicalize_path)?;

    let mnt_path = args
        .mnt_path
        .ok_or_else(|| Error::Cli("mnt_path is required".into()))
        .map(canonicalize_path)?;

    let diff_dir_path = args
        .diff_dir
        .ok_or_else(|| Error::Cli("diff_dir is required".into()))
        .map(canonicalize_path)?;

    let dirty_marker = diff_dir_path.join(crate::fs::overlay::DIRTY_MARKER);
    if dirty_marker.exists() {
        if !args.force {
            return Err(Error::PerfUnsafeDirtyMarker {
                path: dirty_marker.display().to_string(),
            }
            .into());
        }
        warn!(
            path = %dirty_marker.display(),
            "forcing mount despite existing perf-unsafe marker"
        );
        let _ = fs::remove_file(&dirty_marker);
    }

    let target = MountTarget::new(&mnt_path);
    target.validate_empty()?;
    info!("validated target directory");

    fs::create_dir_all(&diff_dir_path)?;
    let diff_dir = DiffDir::new(&diff_dir_path)?;
    diff_dir.ensure_writable()?;
    info!(diff = %diff_dir.path.display(), "diff directory prepared");

    {
        let _guard = env_lock().lock();
        if args.perf_unsafe {
            std::env::set_var("PBKFS_PERF_UNSAFE", "1");
        } else {
            std::env::set_var("PBKFS_PERF_UNSAFE", "0");
        }
        if args.no_wal {
            std::env::set_var("PBKFS_NO_WAL", "1");
        } else {
            std::env::set_var("PBKFS_NO_WAL", "0");
        }
    }

    let existing_binding = diff_dir.load_binding()?;
    let mut recovered_stale = false;
    let binding = if let Some(mut binding) = existing_binding {
        if binding.no_wal_used {
            return Err(Error::Cli(
                "diff directory was previously mounted with --no-wal and cannot be reused; choose a fresh pbk_diff"
                    .into(),
            )
            .into());
        }
        info!(binding_id=%binding.binding_id, "reusing existing binding from diff");
        binding.validate_store_path(&pbk_store)?;
        binding.validate_binding_args(args.instance.as_deref(), args.backup_id.as_deref())?;

        if !paths_match(&binding.pbk_target_path, &mnt_path) {
            warn!(
                expected=%binding.pbk_target_path.display(),
                actual=%mnt_path.display(),
                "mount path differs from binding; proceeding with reuse"
            );
        }

        let lock_path = diff_dir.lock_path();
        if lock_path.exists() {
            if binding.is_owner_alive() {
                return Err(Error::BindingInUse(binding.owner_pid).into());
            }
            warn!(
                owner_pid = binding.owner_pid,
                "stale lock detected; recovering"
            );
            let _ = fs::remove_file(&lock_path);
            binding.mark_stale();
            recovered_stale = true;
        }

        binding.refresh_for_reuse(
            &mnt_path,
            std::process::id() as i32,
            std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into()),
        );
        // If the user opts into --no-wal on reuse, mark the binding as non-resumable
        // so subsequent mounts are rejected.
        binding.no_wal_used |= args.no_wal;
        binding
    } else {
        let instance = args
            .instance
            .ok_or_else(|| Error::Cli("instance is required".into()))?;
        let backup_id = args
            .backup_id
            .ok_or_else(|| Error::Cli("backup_id is required".into()))?;

        if args.no_wal {
            warn!(
                diff_dir = %diff_dir.path.display(),
                "mounting with --no-wal: WAL changes will not persist; this diff directory cannot be reused later"
            );
        }

        let mut binding = BindingRecord::new(
            instance,
            backup_id,
            &pbk_store,
            &mnt_path,
            std::process::id() as i32,
            std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into()),
            env!("CARGO_PKG_VERSION"),
        );
        binding.no_wal_used = args.no_wal;
        binding
    };

    let instance = binding.instance_name.clone();
    let backup_id = binding.backup_id.clone();

    let store = BackupStore::load_from_pg_probackup(&pbk_store, &instance)?;
    let chain = BackupChain::from_binding(&store, &binding)?;
    info!(
        backup = ?backup_id,
        instance = ?instance,
        store_layout = %store.layout,
        "backup chain resolved"
    );

    // Validate chain integrity
    match chain.integrity_state {
        ChainIntegrity::Incomplete => {
            return Err(Error::Cli(
                "Backup chain is incomplete (missing FULL backup). Cannot mount.".into(),
            )
            .into());
        }
        ChainIntegrity::Corrupt if !args.force => {
            return Err(Error::Cli(
                "Backup chain contains corrupted backups. Use --force to override (dangerous)."
                    .into(),
            )
            .into());
        }
        ChainIntegrity::Corrupt => {
            warn!("Mounting CORRUPT backup chain (--force enabled). Data may be invalid!");
        }
        ChainIntegrity::Valid => {}
    }

    binding.write_to_diff(&diff_dir)?;
    if recovered_stale {
        warn!(binding_id=%binding.binding_id, "binding recovered from stale state");
    }
    info!(binding_id=%binding.binding_id, "binding persisted to diff");

    let mut layers = Vec::new();
    for backup in chain.elements.iter().rev() {
        let root = store.backup_data_root(backup)?;
        layers.push(OverlayLayer {
            root,
            compression: backup.compression_algorithm(),
            incremental: backup.backup_type == BackupType::Incremental,
            backup_mode: backup.backup_mode,
        });
    }

    let overlay = Overlay::new_with_layers(&store.path, &diff_dir.path, layers)?;
    let layer_roots = overlay.layer_roots();
    debug!(?layer_roots, "overlay_layers_order");

    // Persist lock markers before mounting so writes hit the real FS, not the FUSE layer.
    let mut session = MountSession::new(binding.binding_id, &mnt_path);
    if overlay.perf_unsafe() {
        fs::write(
            &dirty_marker,
            format!("perf-unsafe mount {}", session.mount_id),
        )?;
        warn!(
            path = %dirty_marker.display(),
            "perf-unsafe mode enabled: fsyncs are deferred until unmount"
        );
    }

    let marker = LockMarker {
        mount_id: session.mount_id,
        diff_dir: diff_dir.path.clone(),
    };
    let marker_bytes = serde_json::to_vec_pretty(&marker)?;
    fs::write(diff_dir.path.join(LOCK_FILE), &marker_bytes)?;
    // Do not write into the mount target; it will be shadowed by the FUSE mount.

    // Start FUSE session; surface errors to caller so tests reveal mount issues.
    let fuse_handle = Some(fuse::spawn_overlay(overlay.clone(), &mnt_path)?);

    session.state = MountSessionState::Ready;
    info!(mount_id=%session.mount_id, "mount ready");

    Ok(MountContext {
        store,
        chain,
        diff_dir,
        binding,
        overlay,
        session,
        fuse_handle,
    })
}

fn canonicalize_path(path: PathBuf) -> PathBuf {
    path.canonicalize().unwrap_or(path)
}

fn paths_match(a: &Path, b: &Path) -> bool {
    canonicalize_path(a.to_path_buf()) == canonicalize_path(b.to_path_buf())
}

fn run_launcher(args: MountArgs) -> Result<()> {
    validate_launcher(&args)?;

    let pipe = crate::cli::daemon::Pipe::new()
        .map_err(|e| Error::Cli(format!("failed to create handshake pipe: {e}")))?;

    let pid = unsafe { libc::fork() };
    if pid < 0 {
        pipe.close_read();
        pipe.close_write();
        return Err(Error::Cli(format!(
            "failed to fork background worker: {}",
            std::io::Error::last_os_error()
        ))
        .into());
    }

    if pid == 0 {
        // Child (worker).
        pipe.close_read();
        let hs = crate::cli::daemon::HandshakeWriter::new(pipe.write_fd);
        if let Err(err) = crate::cli::daemon::daemonize() {
            let _ = hs.mount_error(&format!("daemonize failed: {err}"));
            unsafe { libc::_exit(1) };
        }

        if let Err(err) = run_mount_worker(args, Some(hs), true) {
            // Best-effort: if run_mount_worker failed before sending a status,
            // attempt to notify launcher.
            let _ = hs.mount_error(&format!("{err}"));
            unsafe { libc::_exit(1) };
        }
        unsafe { libc::_exit(0) };
    }

    // Parent (launcher).
    pipe.close_write();
    let cancelled = std::sync::Arc::new(AtomicBool::new(false));
    let _ = ctrlc::set_handler({
        let cancelled = std::sync::Arc::clone(&cancelled);
        move || {
            cancelled.store(true, Ordering::Relaxed);
        }
    });

    let mut reader = unsafe { std::fs::File::from_raw_fd(pipe.read_fd) };

    // Phase 1: wait for "started" (bounded).
    let (code1, msg1) = match read_handshake(&mut reader, Some(Duration::from_secs(10)), &cancelled)
    {
        Ok(v) => v,
        Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {
            eprintln!("pbkfs: cancelled waiting for worker start; worker may still be running");
            return Ok(());
        }
        Err(err) => {
            return Err(Error::Cli(format!("failed waiting for worker start: {err}")).into());
        }
    };
    if cancelled.load(Ordering::Relaxed) {
        eprintln!("pbkfs: cancelled waiting for worker start; worker may still be running");
        return Ok(());
    }
    match code1 {
        crate::cli::daemon::STATUS_STARTED => {}
        crate::cli::daemon::STATUS_ERR => {
            return Err(Error::Cli(format!("worker failed to start: {}", msg1.trim())).into());
        }
        0xFE => {
            return Err(Error::Cli("worker exited before handshake".into()).into());
        }
        other => {
            return Err(Error::Cli(format!("unexpected handshake status: 0x{other:02X}")).into());
        }
    }

    match args.wait_mode() {
        WaitMode::StartOnly => Ok(()),
        WaitMode::Wait => {
            let (code2, msg2) = match read_handshake(&mut reader, None, &cancelled) {
                Ok(v) => v,
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {
                    eprintln!("pbkfs: cancelled waiting for mount completion; worker may still be running");
                    return Ok(());
                }
                Err(err) => {
                    return Err(
                        Error::Cli(format!("failed waiting for mount completion: {err}")).into(),
                    );
                }
            };
            if cancelled.load(Ordering::Relaxed) {
                eprintln!(
                    "pbkfs: cancelled waiting for mount completion; worker may still be running"
                );
                return Ok(());
            }
            match code2 {
                crate::cli::daemon::STATUS_OK => Ok(()),
                crate::cli::daemon::STATUS_ERR => {
                    Err(Error::Cli(format!("mount failed: {}", msg2.trim())).into())
                }
                0xFE => {
                    Err(Error::Cli("worker exited before reporting mount completion".into()).into())
                }
                other => {
                    Err(Error::Cli(format!("unexpected handshake status: 0x{other:02X}")).into())
                }
            }
        }
        WaitMode::Timeout(t) => {
            match read_handshake(&mut reader, Some(t), &cancelled) {
                Ok((crate::cli::daemon::STATUS_OK, _)) => Ok(()),
                Ok((crate::cli::daemon::STATUS_ERR, msg2)) => {
                    Err(Error::Cli(format!("mount failed: {}", msg2.trim())).into())
                }
                Ok((0xFE, _)) => {
                    Err(Error::Cli("worker exited before reporting mount completion".into()).into())
                }
                Ok((other, _)) => {
                    Err(Error::Cli(format!("unexpected handshake status: 0x{other:02X}")).into())
                }
                Err(err) if err.kind() == std::io::ErrorKind::TimedOut => {
                    eprintln!("pbkfs: mount still in progress (timeout reached); worker may still be running");
                    Ok(())
                }
                Err(err) if err.kind() == std::io::ErrorKind::Interrupted => {
                    eprintln!("pbkfs: cancelled waiting for mount completion; worker may still be running");
                    Ok(())
                }
                Err(err) => {
                    Err(Error::Cli(format!("failed waiting for mount completion: {err}")).into())
                }
            }
        }
        WaitMode::Foreground => Ok(()),
    }
}

fn validate_launcher(args: &MountArgs) -> Result<()> {
    let pbk_store = args
        .pbk_store
        .as_ref()
        .ok_or_else(|| Error::Cli("pbk_store is required".into()))?;
    let mnt_path = args
        .mnt_path
        .as_ref()
        .ok_or_else(|| Error::Cli("mnt_path is required".into()))?;
    let diff_dir_path = args
        .diff_dir
        .as_ref()
        .ok_or_else(|| Error::Cli("diff_dir is required".into()))?;

    // Validate target early to avoid daemonizing only to fail immediately.
    MountTarget::new(mnt_path).validate_empty()?;

    fs::create_dir_all(diff_dir_path)?;
    let diff_dir = DiffDir::new(diff_dir_path)?;
    diff_dir.ensure_writable()?;

    // Ensure we can compute daemon paths (validates pid-file override semantics).
    let _ = args.daemon_defaults()?;

    // If binding exists, validate that store matches and optional args align.
    if let Some(binding) = diff_dir.load_binding()? {
        binding.validate_store_path(pbk_store)?;
        binding.validate_binding_args(args.instance.as_deref(), args.backup_id.as_deref())?;
        if diff_dir.lock_path().exists() && binding.is_owner_alive() {
            return Err(Error::BindingInUse(binding.owner_pid).into());
        }
    } else {
        // No binding: require instance + backup id.
        if args.instance.is_none() {
            return Err(Error::Cli("instance is required".into()).into());
        }
        if args.backup_id.is_none() {
            return Err(Error::Cli("backup_id is required".into()).into());
        }
    }

    Ok(())
}

fn read_handshake(
    file: &mut std::fs::File,
    timeout: Option<Duration>,
    cancelled: &std::sync::Arc<AtomicBool>,
) -> std::io::Result<(u8, String)> {
    let start = std::time::Instant::now();
    loop {
        if cancelled.load(Ordering::Relaxed) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "cancelled",
            ));
        }
        if let Some(t) = timeout {
            if start.elapsed() >= t {
                return Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"));
            }
        }

        let mut pfd = libc::pollfd {
            fd: file.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let rc = unsafe { libc::poll(&mut pfd, 1, 100) };
        if rc < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        if rc == 0 {
            continue;
        }
        if (pfd.revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR)) == 0 {
            continue;
        }
        // If the writer closed the pipe, poll may report only POLLHUP without POLLIN.
        // Treat it as EOF for the handshake protocol.
        if (pfd.revents & (libc::POLLHUP | libc::POLLERR)) != 0 && (pfd.revents & libc::POLLIN) == 0
        {
            return Ok((0xFE, String::new()));
        }

        let mut code = [0u8; 1];
        let n = unsafe { libc::read(file.as_raw_fd(), code.as_mut_ptr() as *mut libc::c_void, 1) };
        if n < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(err);
        }
        if n == 0 {
            return Ok((0xFE, String::new()));
        }

        if code[0] == crate::cli::daemon::STATUS_ERR {
            let mut buf = Vec::new();
            let _ = file.read_to_end(&mut buf);
            let msg = String::from_utf8_lossy(&buf).to_string();
            return Ok((code[0], msg));
        }
        return Ok((code[0], String::new()));
    }
}

static SIGNAL_WRITE_FD: AtomicI32 = AtomicI32::new(-1);

extern "C" fn signal_handler(sig: i32) {
    // `Relaxed` is sufficient here: this is the classic self-pipe pattern where the
    // handler only needs a best-effort read of the fd to emit a single byte.
    let fd = SIGNAL_WRITE_FD.load(Ordering::Relaxed);
    if fd < 0 {
        return;
    }
    let byte = sig as u8;
    unsafe {
        let _ = libc::write(fd, &byte as *const u8 as *const libc::c_void, 1);
    }
}

fn install_signal_handlers(write_fd: i32) -> std::io::Result<()> {
    SIGNAL_WRITE_FD.store(write_fd, Ordering::Relaxed);
    unsafe {
        libc::signal(libc::SIGHUP, libc::SIG_IGN);
    }
    for sig in [libc::SIGINT, libc::SIGTERM, libc::SIGUSR1, libc::SIGUSR2] {
        unsafe {
            let mut action: libc::sigaction = std::mem::zeroed();
            action.sa_sigaction = signal_handler as usize;
            action.sa_flags = 0;
            libc::sigemptyset(&mut action.sa_mask);
            if libc::sigaction(sig, &action, std::ptr::null_mut()) != 0 {
                return Err(std::io::Error::last_os_error());
            }
        }
    }
    Ok(())
}

fn run_mount_worker(
    args: MountArgs,
    handshake: Option<crate::cli::daemon::HandshakeWriter>,
    background: bool,
) -> Result<()> {
    let defaults = args.daemon_defaults()?;

    if background {
        // Background: file-by-default logging (unless journald is selected).
        let sink = match args.log_sink.unwrap_or_default() {
            LogSink::File => {
                let path = args
                    .log_file
                    .clone()
                    .unwrap_or_else(|| defaults.log_diff.clone());
                crate::logging::LogSink::File(path)
            }
            LogSink::Journald => crate::logging::LogSink::Journald,
        };
        crate::logging::init_logging(crate::logging::LoggingConfig {
            format: match args.log_format {
                LogFormatArg::Text => crate::logging::LogFormat::Human,
                LogFormatArg::Json => crate::logging::LogFormat::Json,
            },
            sink,
            debug: args.debug,
        })?;
    }

    // Create PID record in diff layer so it is visible through FUSE.
    let diff_dir = args
        .diff_dir
        .as_ref()
        .ok_or_else(|| Error::Cli("diff_dir is required".into()))?;
    crate::cli::pid::prepare_pid_record(&defaults.pid_diff, diff_dir, process::id() as i32)?;

    if let Some(hs) = handshake {
        hs.started()
            .map_err(|e| Error::Cli(format!("failed to write handshake: {e}")))?;
    }

    if background {
        if let Some(delay_ms) = std::env::var("PBKFS_TEST_DAEMON_DELAY_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
        {
            std::thread::sleep(Duration::from_millis(delay_ms));
        }
        if matches!(
            std::env::var("PBKFS_TEST_DAEMON_CRASH_AFTER_STARTED").as_deref(),
            Ok("1")
        ) {
            // Intentionally exit without writing the phase-2 status.
            unsafe { libc::_exit(2) };
        }
    }

    // Mount and hold.
    let mut ctx = match mount(args.clone()) {
        Ok(ctx) => ctx,
        Err(err) => {
            let _ = fs::remove_file(&defaults.pid_diff);
            let _ = fs::remove_file(&defaults.stat_diff);
            if let Some(hs) = handshake {
                let _ = hs.mount_error(&format!("{err}"));
            }
            return Err(err);
        }
    };

    let res = hold_mount(&mut ctx, &defaults, handshake.as_ref());

    // Cleanup PID/stat (best-effort).
    let _ = fs::remove_file(&defaults.pid_diff);
    let _ = fs::remove_file(&defaults.stat_diff);
    let _ = fs::remove_file(ctx.diff_dir.path.join(LOCK_FILE));

    res
}

fn hold_mount(
    ctx: &mut MountContext,
    defaults: &DaemonDefaults,
    handshake: Option<&crate::cli::daemon::HandshakeWriter>,
) -> Result<()> {
    let Some(handle) = ctx.fuse_handle.take() else {
        return Ok(());
    };

    #[derive(Debug)]
    enum Event {
        Signal(i32),
        Unmounted,
    }

    let (tx, rx) = mpsc::channel();

    // Watch external unmounts.
    let mount_path = ctx.session.pbk_target_path.clone();
    std::thread::spawn({
        let tx = tx.clone();
        move || loop {
            std::thread::sleep(Duration::from_millis(500));
            if !is_mounted(&mount_path) {
                let _ = tx.send(Event::Unmounted);
                break;
            }
        }
    });

    // Dedicated signal thread (self-pipe).
    let mut fds = [0i32; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } != 0 {
        return Err(Error::Cli(format!(
            "failed to create signal pipe: {}",
            std::io::Error::last_os_error()
        ))
        .into());
    }
    install_signal_handlers(fds[1])
        .map_err(|e| Error::Cli(format!("failed to install signals: {e}")))?;
    std::thread::spawn({
        let tx = tx.clone();
        move || {
            let mut buf = [0u8; 1];
            loop {
                let n = unsafe { libc::read(fds[0], buf.as_mut_ptr() as *mut _, 1) };
                if n <= 0 {
                    break;
                }
                let sig = buf[0] as i32;
                let _ = tx.send(Event::Signal(sig));
            }
        }
    });

    // Ensure stat file exists (best-effort).
    let _ = fs::create_dir_all(defaults.stat_diff.parent().unwrap_or(Path::new(".")));
    let _ = fs::write(&defaults.stat_diff, b"{}");

    // At this point, the mount is ready and the worker can accept signals.
    if let Some(hs) = handshake {
        hs.mount_ok()
            .map_err(|e| Error::Cli(format!("failed to write handshake: {e}")))?;
    }

    loop {
        match rx.recv() {
            Ok(Event::Unmounted) => {
                info!(
                    "detected external unmount; exiting for {}",
                    ctx.session.pbk_target_path.display()
                );
                handle.unmount();
                break;
            }
            Ok(Event::Signal(sig)) => match sig {
                libc::SIGUSR1 | libc::SIGUSR2 => {
                    // Dump stats in JSON into the diff layer so it's visible through FUSE.
                    let stats = serde_json::json!({
                        "pid": process::id(),
                        "overlay": ctx.overlay.metrics(),
                        "fuse": handle.stats_snapshot(),
                    });
                    let _ = fs::write(
                        &defaults.stat_diff,
                        serde_json::to_vec_pretty(&stats).unwrap_or_default(),
                    );

                    if sig == libc::SIGUSR2 {
                        // Reset counters after dumping.
                        ctx.overlay.reset_metrics();
                        handle.reset_counters();
                    }
                }
                libc::SIGINT | libc::SIGTERM => {
                    info!(
                        "signal received; requesting unmount of {}",
                        ctx.session.pbk_target_path.display()
                    );

                    // IMPORTANT: do not force-detach from the worker on SIGTERM.
                    // If the mount is busy (e.g. PostgreSQL still running), we
                    // must leave the mount in place and keep the worker alive.
                    match crate::cli::unmount::system_unmount(&ctx.session.pbk_target_path) {
                        Ok(()) => {
                            handle.unmount();
                            break;
                        }
                        Err(err)
                            if matches!(
                                err.downcast_ref::<Error>(),
                                Some(Error::Cli(msg)) if msg.to_lowercase().contains("busy")
                            ) =>
                        {
                            warn!(
                                path = %ctx.session.pbk_target_path.display(),
                                error = %err,
                                "unmount requested but mount is busy; keeping worker running"
                            );
                        }
                        Err(err) => {
                            warn!(
                                path = %ctx.session.pbk_target_path.display(),
                                error = %err,
                                "unmount requested but failed; keeping worker running"
                            );
                        }
                    }
                }
                _ => {}
            },
            Err(_) => {
                handle.unmount();
                break;
            }
        }
    }

    if let Err(err) = ctx.overlay.flush_dirty(false) {
        warn!(error = ?err, "failed to flush perf-unsafe dirty set");
    }

    Ok(())
}
