//! Implementation of `pbkfs unmount` subcommand.

use std::{fs, path::PathBuf};

use clap::Args;

use crate::{binding::LOCK_FILE, binding::LockMarker, Error, Result};

#[derive(Debug, Clone, Args)]
pub struct UnmountArgs {
    /// Path to an existing pbkfs mount target
    #[arg(long = "mnt-path")]
    pub mnt_path: Option<PathBuf>,
}

pub fn execute(args: UnmountArgs) -> Result<()> {
    let mnt_path = args
        .mnt_path
        .ok_or_else(|| Error::Cli("mnt_path is required".into()))?;

    if !mnt_path.exists() || !mnt_path.is_dir() {
        return Err(Error::InvalidTargetDir(mnt_path.display().to_string()).into());
    }

    let target_lock = mnt_path.join(LOCK_FILE);
    if !target_lock.exists() {
        return Err(Error::NotMounted(mnt_path.display().to_string()).into());
    }

    let marker = LockMarker::read(&target_lock)?;
    // Remove target-side marker
    fs::remove_file(&target_lock)?;
    // Remove diff-side lock marker if present
    let diff_lock = marker.diff_dir.join(LOCK_FILE);
    if diff_lock.exists() {
        let _ = fs::remove_file(diff_lock);
    }

    Ok(())
}
