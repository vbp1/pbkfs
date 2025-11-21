//! Placeholder for `pbkfs cleanup` subcommand.

use std::path::PathBuf;

use clap::Args;

use crate::Result;

#[derive(Debug, Clone, Args, Default)]
pub struct CleanupArgs {
    /// Path to a diff directory to clean up
    #[arg(long = "diff-dir")]
    pub diff_dir: Option<PathBuf>,

    /// Force removal even if populated
    #[arg(long = "force")]
    pub force: bool,
}

pub fn execute(_args: CleanupArgs) -> Result<()> {
    // Cleanup is implemented in a later phase; keep argument validation minimal for now.
    Ok(())
}
