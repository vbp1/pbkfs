//! Implementation of `pbkfs cleanup` subcommand.

use std::path::PathBuf;

use clap::Args;

use crate::{binding::lock::cleanup_diff_dir, binding::DiffDir, Error, Result};

#[derive(Debug, Clone, Args, Default)]
pub struct CleanupArgs {
    /// Path to a diff directory to clean up
    #[arg(short = 'd', long = "diff-dir")]
    pub diff_dir: Option<PathBuf>,

    /// Force removal even if populated
    #[arg(long = "force")]
    pub force: bool,
}

pub fn execute(args: CleanupArgs) -> Result<()> {
    crate::logging::init_logging(crate::logging::LoggingConfig {
        format: crate::logging::LogFormat::Human,
        sink: crate::logging::LogSink::Console,
        debug: false,
    })?;

    let diff_dir = args
        .diff_dir
        .ok_or_else(|| Error::Cli("diff_dir is required".into()))?;

    let diff_dir = DiffDir::new(diff_dir)?;
    match cleanup_diff_dir(&diff_dir, args.force) {
        Err(e) if matches!(e.downcast_ref::<Error>(), Some(Error::BindingInUse(_))) => {
            let pid = match e.downcast_ref::<Error>() {
                Some(Error::BindingInUse(p)) => *p,
                _ => -1,
            };
            eprintln!(
                "Cleanup blocked: diff directory in use by process {pid}. Hint: unmount first or re-run with --force."
            );
            Err(e)
        }
        other => other,
    }
}
