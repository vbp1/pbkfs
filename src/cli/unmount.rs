//! Implementation of `pbkfs unmount` subcommand.

use std::{fs, path::PathBuf};

use clap::Args;

use crate::{binding::LockMarker, Error, Result};
use std::path::Path;
use std::process::Command;

#[derive(Debug, Clone, Args)]
pub struct UnmountArgs {
    /// Path to an existing pbkfs mount target
    #[arg(long = "mnt-path")]
    pub mnt_path: Option<PathBuf>,

    /// Optional diff directory to resolve binding without reading mnt lock
    #[arg(long = "diff-dir")]
    pub diff_dir: Option<PathBuf>,
}

pub fn execute(args: UnmountArgs) -> Result<()> {
    let mnt_path = args
        .mnt_path
        .ok_or_else(|| Error::Cli("mnt_path is required".into()))?;

    perform_unmount(&mnt_path, args.diff_dir.as_deref())
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
                let stderr_lower = stderr.to_lowercase();

                // Detect "not mounted" cases so we can surface a precise error.
                if stderr_lower.contains("not mounted")
                    || stderr_lower.contains("not found in /etc/mtab")
                {
                    saw_not_mounted = true;
                    last_stderr = Some(stderr);
                    continue;
                }

                // Detect busy mounts and report a clearer message instead of
                // incorrectly claiming that the target is not mounted.
                if stderr_lower.contains("device or resource busy")
                    || stderr_lower.contains("target is busy")
                {
                    return Err(Error::Cli(format!(
                        "target is busy: {} ({cmd} failed: {})",
                        mnt_path.display(),
                        stderr.trim()
                    ))
                    .into());
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
pub fn perform_unmount(mnt_path: &Path, diff_dir_hint: Option<&Path>) -> Result<()> {
    if !mnt_path.exists() || !mnt_path.is_dir() {
        return Err(Error::InvalidTargetDir(mnt_path.display().to_string()).into());
    }

    // Best-effort system unmount; propagate only if clearly not mounted.
    if let Err(err) = system_unmount(mnt_path) {
        // If not mounted, surface the error; otherwise allow post-cleanup.
        if matches!(err.downcast_ref::<Error>(), Some(Error::NotMounted(_))) {
            return Err(err);
        }
    }

    let marker = if let Some(diff_dir) = diff_dir_hint {
        LockMarker::from_diff_dir(diff_dir)?
    } else {
        let target_lock = mnt_path.join(crate::binding::LOCK_FILE);
        if !target_lock.exists() {
            return Err(Error::NotMounted(mnt_path.display().to_string()).into());
        }
        let marker = LockMarker::read(&target_lock)?;
        let _ = fs::remove_file(target_lock);
        marker
    };

    // Remove diff-side lock marker if present
    let diff_lock = marker.diff_dir.join(crate::binding::LOCK_FILE);
    if diff_lock.exists() {
        let _ = fs::remove_file(diff_lock);
    }

    Ok(())
}
