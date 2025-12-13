//! Implementation of `pbkfs unmount` subcommand.

use std::{fs, path::PathBuf};

use clap::Args;

use crate::{Error, Result};
use std::path::Path;
use std::process::Command;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnmountFailureKind {
    Busy,
    NotMounted,
    Other,
}

pub fn classify_unmount_stderr(stderr: &str) -> UnmountFailureKind {
    let s = stderr.to_lowercase();
    if s.contains("device or resource busy") || s.contains("target is busy") {
        return UnmountFailureKind::Busy;
    }
    if s.contains("not mounted") || s.contains("not found in /etc/mtab") {
        return UnmountFailureKind::NotMounted;
    }
    UnmountFailureKind::Other
}

#[derive(Debug, Clone, Args, Default)]
pub struct UnmountArgs {
    /// Path to an existing pbkfs mount target
    #[arg(short = 'D', long = "mnt-path")]
    pub mnt_path: Option<PathBuf>,

    /// Optional diff directory hint (used when mount endpoint is broken and PID file is unreadable).
    #[arg(short = 'd', long = "diff-dir")]
    pub diff_dir: Option<PathBuf>,

    /// Force unmount: SIGKILL + lazy detach if needed.
    #[arg(long = "force", default_value_t = false)]
    pub force: bool,
}

pub fn execute(args: UnmountArgs) -> Result<()> {
    crate::logging::init_logging(crate::logging::LoggingConfig {
        format: crate::logging::LogFormat::Human,
        sink: crate::logging::LogSink::Console,
        debug: false,
    })?;

    let mnt_path = args
        .mnt_path
        .ok_or_else(|| Error::Cli("mnt_path is required".into()))?;

    perform_unmount(&mnt_path, args.diff_dir.as_deref(), args.force)
}

/// Execute an OS-level unmount. Tries `fusermount -u` first, then `umount`.
pub fn system_unmount(mnt_path: &Path) -> Result<()> {
    let path_string = mnt_path.to_string_lossy().to_string();
    let candidates = vec![
        ("fusermount", vec!["-u", path_string.as_str()]),
        ("umount", vec![path_string.as_str()]),
    ];

    let mut saw_not_mounted = false;
    let mut last_stderr: Option<String> = None;

    for (cmd, args) in candidates {
        match Command::new(cmd).args(args).output() {
            Ok(output) => {
                if output.status.success() {
                    return Ok(());
                }

                let stderr = String::from_utf8_lossy(&output.stderr).to_string();
                match classify_unmount_stderr(&stderr) {
                    UnmountFailureKind::NotMounted => {
                        saw_not_mounted = true;
                        last_stderr = Some(stderr);
                        continue;
                    }
                    UnmountFailureKind::Busy => {
                        return Err(Error::Cli(format!(
                            "target is busy: {} ({cmd} failed: {})",
                            mnt_path.display(),
                            stderr.trim()
                        ))
                        .into());
                    }
                    UnmountFailureKind::Other => {}
                }

                // Record the last stderr for generic failures.
                last_stderr = Some(stderr);
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(Error::Io(err).into()),
        }
    }

    if saw_not_mounted {
        return Err(Error::NotMounted(mnt_path.display().to_string()).into());
    }

    if let Some(stderr) = last_stderr {
        return Err(Error::Cli(format!(
            "failed to unmount {}: {}",
            mnt_path.display(),
            stderr.trim()
        ))
        .into());
    }

    Err(Error::NotMounted(mnt_path.display().to_string()).into())
}

/// Shared unmount flow used by CLI and in-process signal handler.
pub fn perform_unmount(mnt_path: &Path, diff_dir_hint: Option<&Path>, force: bool) -> Result<()> {
    perform_unmount_impl(mnt_path, diff_dir_hint, force)
}

fn perform_unmount_impl(mnt_path: &Path, diff_dir_hint: Option<&Path>, force: bool) -> Result<()> {
    // Avoid pre-validating via `Path::exists/is_dir` because broken FUSE mountpoints may return
    // ENOTCONN ("Transport endpoint is not connected") on `stat(2)`. In that case, proceed to
    // best-effort unmount instead of failing fast.
    match std::fs::metadata(mnt_path) {
        Ok(meta) => {
            if !meta.is_dir() {
                return Err(Error::InvalidTargetDir(mnt_path.display().to_string()).into());
            }
        }
        Err(e) => {
            if e.raw_os_error() == Some(libc::ENOTCONN)
                || e.to_string()
                    .to_lowercase()
                    .contains("transport endpoint is not connected")
            {
                system_unmount_with_force(mnt_path, force)?;
                cleanup_lock_markers(diff_dir_hint);
                return Ok(());
            }
            if e.kind() == std::io::ErrorKind::NotFound {
                return Err(Error::InvalidTargetDir(mnt_path.display().to_string()).into());
            }
            return Err(Error::Io(e).into());
        }
    }

    let pid_path = mnt_path.join(crate::cli::pid::DEFAULT_PID_FILE_REL);
    let stat_path = mnt_path.join(crate::cli::pid::DEFAULT_STAT_FILE_REL);
    let record = match crate::cli::pid::read_pid_record(&pid_path) {
        Ok(record) => record,
        Err(err) => {
            // Broken mount endpoints may error with ENOTCONN ("Transport endpoint is not connected").
            if is_transport_endpoint_error(&err) {
                system_unmount_with_force(mnt_path, force)?;
                cleanup_lock_markers(diff_dir_hint);
                return Ok(());
            }

            // If pid file is missing, try best-effort system unmount anyway.
            if let Some(io) = err.downcast_ref::<std::io::Error>() {
                if io.kind() == std::io::ErrorKind::NotFound {
                    system_unmount_with_force(mnt_path, force)?;
                    cleanup_lock_markers(diff_dir_hint);
                    return Ok(());
                }
            }

            // Unknown failure reading pid file: fall back to system unmount.
            system_unmount_with_force(mnt_path, force)?;
            cleanup_lock_markers(diff_dir_hint);
            return Ok(());
        }
    };

    let diff_dir = diff_dir_hint
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| record.diff_dir.clone());

    let pid = record.pid;
    if crate::cli::pid::pid_alive(pid) {
        // Best-effort safety: ensure binding owner matches PID before signalling.
        if let Ok(diff_dir_obj) = crate::binding::DiffDir::new(&diff_dir) {
            if let Ok(Some(binding)) = diff_dir_obj.load_binding() {
                if binding.owner_pid != pid {
                    return Err(Error::Cli(format!(
                        "refusing to signal pid {pid}: PID file does not match binding owner"
                    ))
                    .into());
                }
                if !paths_match(mnt_path, binding.pbk_target_path.as_path()) {
                    // Canonicalization differences are handled by mount reuse; be strict here.
                    return Err(Error::Cli(
                        "refusing to unmount: mount path does not match binding".into(),
                    )
                    .into());
                }
            }
        }

        // Graceful SIGTERM.
        if unsafe { libc::kill(pid, libc::SIGTERM) } != 0 {
            // If signalling fails, fall back to system unmount.
            system_unmount_with_force(mnt_path, force)?;
        } else {
            // Wait for process exit.
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
            while crate::cli::pid::pid_alive(pid) && std::time::Instant::now() < deadline {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }

            if crate::cli::pid::pid_alive(pid) {
                if !force {
                    return Err(Error::Cli(format!(
                        "target is busy: {} (worker did not exit after SIGTERM; retry with --force)",
                        mnt_path.display()
                    ))
                    .into());
                }
                let _ = unsafe { libc::kill(pid, libc::SIGKILL) };
                system_unmount_lazy(mnt_path)?;
            } else {
                // Worker exited; ensure mount is gone (best-effort).
                let _ = system_unmount(mnt_path);
            }
        }
    } else {
        // Stale PID: fallback to system unmount (and force-detach if requested).
        system_unmount_with_force(mnt_path, force)?;
    }

    // Cleanup lock/pid/stat files best-effort.
    cleanup_lock_markers(Some(diff_dir.as_path()));
    let _ = fs::remove_file(pid_path);
    let _ = fs::remove_file(stat_path);

    Ok(())
}

fn paths_match(a: &Path, b: &Path) -> bool {
    a.canonicalize().unwrap_or_else(|_| a.to_path_buf())
        == b.canonicalize().unwrap_or_else(|_| b.to_path_buf())
}

fn is_transport_endpoint_error(err: &anyhow::Error) -> bool {
    if let Some(io) = err.downcast_ref::<std::io::Error>() {
        if io.raw_os_error() == Some(libc::ENOTCONN) {
            return true;
        }
    }
    err.to_string()
        .to_lowercase()
        .contains("transport endpoint is not connected")
}

fn cleanup_lock_markers(diff_dir: Option<&Path>) {
    if let Some(diff_dir) = diff_dir {
        let _ = fs::remove_file(diff_dir.join(crate::binding::LOCK_FILE));
    }
}

fn system_unmount_with_force(mnt_path: &Path, force: bool) -> Result<()> {
    if force {
        // Try regular unmount first; if it reports busy, lazily detach.
        match system_unmount(mnt_path) {
            Ok(()) => Ok(()),
            Err(err) => {
                if matches!(err.downcast_ref::<Error>(), Some(Error::Cli(msg)) if msg.contains("busy"))
                {
                    system_unmount_lazy(mnt_path)
                } else {
                    Err(err)
                }
            }
        }
    } else {
        system_unmount(mnt_path)
    }
}

fn system_unmount_lazy(mnt_path: &Path) -> Result<()> {
    // Prefer fusermount -uz when available.
    let path_string = mnt_path.to_string_lossy().to_string();
    let candidates = vec![
        ("fusermount", vec!["-u", "-z", path_string.as_str()]),
        ("fusermount3", vec!["-u", "-z", path_string.as_str()]),
        ("umount", vec!["-l", path_string.as_str()]),
    ];

    for (cmd, args) in candidates {
        match Command::new(cmd).args(args).output() {
            Ok(output) if output.status.success() => return Ok(()),
            Ok(_) => continue,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => return Err(Error::Io(err).into()),
        }
    }

    // Final fallback: libc::umount2(MNT_DETACH).
    let c_path = std::ffi::CString::new(path_string).map_err(|e| Error::Cli(e.to_string()))?;
    let rc = unsafe { libc::umount2(c_path.as_ptr(), libc::MNT_DETACH) };
    if rc != 0 {
        return Err(Error::Cli(format!(
            "failed to force-detach {}: {}",
            mnt_path.display(),
            std::io::Error::last_os_error()
        ))
        .into());
    }
    Ok(())
}
