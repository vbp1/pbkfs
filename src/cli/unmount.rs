//! Implementation of `pbkfs unmount` subcommand.

use std::{fs, path::PathBuf};

use clap::Args;

use crate::{binding::LockMarker, Error, Result};

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

    if !mnt_path.exists() || !mnt_path.is_dir() {
        return Err(Error::InvalidTargetDir(mnt_path.display().to_string()).into());
    }

    let marker = if let Some(diff_dir) = args.diff_dir.clone() {
        LockMarker::from_diff_dir(&diff_dir)?
    } else {
        let target_lock = mnt_path.join(crate::binding::LOCK_FILE);
        if !target_lock.exists() {
            return Err(Error::NotMounted(mnt_path.display().to_string()).into());
        }
        let marker = LockMarker::read(&target_lock)?;
        // Remove target-side marker (post-unmount this file will reappear)
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
