//! Implementation of `pbkfs mount` subcommand.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::mpsc,
    time::Duration,
};

use clap::Args;
use serde_json;
use tracing::{info, instrument, warn};

use crate::{
    backup::{
        chain::{BackupChain, ChainIntegrity},
        metadata::BackupStore,
        BackupMode, BackupType,
    },
    binding::{BindingRecord, DiffDir, LockMarker, LOCK_FILE},
    fs::{
        fuse,
        overlay::{Layer as OverlayLayer, Overlay},
        MountSession, MountSessionState, MountTarget,
    },
    Error, Result,
};
use ctrlc;

#[derive(Debug, Clone, Args)]
pub struct MountArgs {
    /// Path to pg_probackup backup store (pbk_store)
    #[arg(short = 'B', long = "pbk-store")]
    pub pbk_store: Option<PathBuf>,

    /// Path to empty mount target directory (pbk_target)
    #[arg(long = "mnt-path")]
    pub mnt_path: Option<PathBuf>,

    /// Path to writable diff directory (pbk_diff)
    #[arg(long = "diff-dir")]
    pub diff_dir: Option<PathBuf>,

    /// pg_probackup instance name
    #[arg(long = "instance")]
    pub instance: Option<String>,

    /// Target backup id
    #[arg(short = 'i', long = "backup-id")]
    pub backup_id: Option<String>,

    /// Allow mounting corrupt backup chains (dangerous)
    #[arg(long, default_value = "false")]
    pub force: bool,
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

pub fn execute(args: MountArgs) -> Result<()> {
    // Execute the mount and hold it until a termination signal is received.
    let mut ctx = mount(args)?;

    if let Some(handle) = ctx.fuse_handle.take() {
        info!("pbkfs mount active; press Ctrl+C to unmount");

        #[derive(Debug)]
        enum Event {
            Signal,
            Unmounted,
        }

        let (tx, rx) = mpsc::channel();

        // Handle SIGINT/SIGTERM.
        ctrlc::set_handler({
            let tx = tx.clone();
            move || {
                let _ = tx.send(Event::Signal);
            }
        })
        .map_err(|e| Error::Cli(format!("failed to install signal handler: {e}")))?;

        // Watch for external unmounts.
        let mount_path = ctx.session.pbk_target_path.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_millis(500));
            if !is_mounted(&mount_path) {
                let _ = tx.send(Event::Unmounted);
                break;
            }
        });

        // Wait for either event.
        match rx.recv() {
            Ok(Event::Signal) => {
                info!(
                    "signal received; unmounting {}",
                    ctx.session.pbk_target_path.display()
                );
                handle.unmount();
            }
            Ok(Event::Unmounted) => {
                info!(
                    "detected external unmount; exiting for {}",
                    ctx.session.pbk_target_path.display()
                );
                // Join the session to ensure the background thread is cleaned up.
                handle.unmount();
            }
            Err(_) => {
                handle.unmount();
            }
        }

        // Clean lock markers best-effort.
        let _ = std::fs::remove_file(ctx.diff_dir.path.join(LOCK_FILE));
    }

    Ok(())
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

    let target = MountTarget::new(&mnt_path);
    target.validate_empty()?;
    info!("validated target directory");

    fs::create_dir_all(&diff_dir_path)?;
    let diff_dir = DiffDir::new(&diff_dir_path)?;
    diff_dir.ensure_writable()?;
    info!(diff = %diff_dir.path.display(), "diff directory prepared");

    let existing_binding = diff_dir.load_binding()?;
    let mut recovered_stale = false;
    let binding = if let Some(mut binding) = existing_binding {
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
        binding
    } else {
        let instance = args
            .instance
            .ok_or_else(|| Error::Cli("instance is required".into()))?;
        let backup_id = args
            .backup_id
            .ok_or_else(|| Error::Cli("backup_id is required".into()))?;

        BindingRecord::new(
            instance,
            backup_id,
            &pbk_store,
            &mnt_path,
            std::process::id() as i32,
            std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into()),
            env!("CARGO_PKG_VERSION"),
        )
    };

    let instance = binding.instance_name.clone();
    let backup_id = binding.backup_id.clone();

    let store = BackupStore::load_from_pg_probackup(&pbk_store, &instance)?;
    let chain = BackupChain::from_binding(&store, &binding)?;
    info!(backup=?backup_id, instance=?instance, "backup chain resolved");

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
        let root = store.path.join(&backup.backup_id).join("database");
        layers.push(OverlayLayer {
            root,
            compression: backup.compression_algorithm(),
            incremental: backup.backup_type == BackupType::Incremental,
            backup_mode: backup.backup_mode,
        });
    }
    // Fallback for legacy layouts where files sit directly under pbk_store
    let fallback_compression = chain.compression_algorithms.first().copied();
    layers.push(OverlayLayer {
        root: store.path.join("database"),
        compression: fallback_compression,
        incremental: false,
        backup_mode: BackupMode::Full,
    });

    let overlay = Overlay::new_with_layers(&store.path, &diff_dir.path, layers)?;

    // Persist lock markers before mounting so writes hit the real FS, not the FUSE layer.
    let mut session = MountSession::new(binding.binding_id, &mnt_path);
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
