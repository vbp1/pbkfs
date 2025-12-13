use thiserror::Error;

use parking_lot::ReentrantMutex;
use std::sync::OnceLock;

pub mod backup;
pub mod binding;
pub mod cli;
pub mod fs;
pub mod logging;

pub type Result<T> = anyhow::Result<T>;

/// Global reentrant lock to serialize reads/writes of pbkfs-related env vars.
/// Reentrancy prevents deadlocks when a caller that already holds the lock
/// invokes helpers that also consult env vars.
pub fn env_lock() -> &'static ReentrantMutex<()> {
    static LOCK: OnceLock<ReentrantMutex<()>> = OnceLock::new();
    LOCK.get_or_init(ReentrantMutex::default)
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid target directory: {0}")]
    InvalidTargetDir(String),
    #[error("target directory not empty: {0}")]
    TargetNotEmpty(String),
    #[error("invalid backup store path: {0}")]
    InvalidStorePath(String),
    #[error("backup chain contains a cycle near {0}")]
    ChainCycle(String),
    #[error("missing backup: {0}")]
    MissingBackup(String),
    #[error(
        "binding violation: expected {expected}, got {actual}. The diff directory is already bound; mount with the recorded instance/backup or clean it with `pbkfs cleanup --diff-dir <path> --force`."
    )]
    BindingViolation { expected: String, actual: String },
    #[error("diff directory not writable: {0}")]
    DiffDirNotWritable(String),
    #[error("unsupported pg_probackup version: {0}")]
    UnsupportedPgProbackupVersion(String),
    #[error("unsupported PostgreSQL version: {version} (supported: {min}-{max})")]
    UnsupportedPostgresVersion { version: String, min: u16, max: u16 },
    #[error("invalid backup store layout: {0}")]
    InvalidStoreLayout(String),
    #[error("pg_probackup binary not found: {0}")]
    PgProbackupMissingBinary(String),
    #[error("pg_probackup not executable or permission denied: {0}")]
    PgProbackupNotExecutable(String),
    #[error("pg_probackup failed to load shared libraries: {0}")]
    PgProbackupMissingSharedLibs(String),
    #[error("pg_probackup instance not found: {0}")]
    PgProbackupInstanceMissing(String),
    #[error("pg_probackup returned invalid JSON: {0}")]
    PgProbackupInvalidJson(String),
    #[error("pg_probackup exited with code {code:?}: {message}")]
    PgProbackupFailed { code: Option<i32>, message: String },
    #[error("unsupported compression algorithm: {0}")]
    UnsupportedCompressionAlgorithm(String),
    #[error("missing compression metadata for compressed backup {0}")]
    MissingCompressionMetadata(String),
    #[error("compressed incremental backup unsupported without pagemap (algo={0:?})")]
    UnsupportedCompressedIncremental(crate::backup::CompressionAlgorithm),
    #[error("pagemap file missing for incremental: {0}")]
    MissingPagemap(String),
    #[error(
        "incremental page size mismatch for {path} block {block}: expected {expected} got {actual}"
    )]
    InvalidIncrementalPageSize {
        path: String,
        block: u32,
        expected: usize,
        actual: usize,
    },
    #[error("incremental data incomplete for {path}; missing pages {missing:?}")]
    IncompleteIncremental { path: String, missing: Vec<u32> },
    #[error("serialization error")]
    Serde(#[from] serde_json::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("cli error: {0}")]
    Cli(String),
    #[error("target is not mounted: {0}")]
    NotMounted(String),
    #[error("diff directory already in use by pid {0}")]
    BindingInUse(i32),
    #[error("perf-unsafe marker present at {path}; previous mount did not shut down cleanly. Re-run with --force if you accept potential data loss.")]
    PerfUnsafeDirtyMarker { path: String },
    #[error("corrupted patch payload: {reason}")]
    CorruptedPatch { reason: String },
    #[error("invalid patch file: {reason}")]
    InvalidPatchFile { reason: String },
    #[error("invalid full file: {reason}")]
    InvalidFullFile { reason: String },
    #[error("patch payload too large: {len} > {max}")]
    PatchTooLarge { len: usize, max: usize },
}

/// Entry point for the library, called by the CLI thin wrapper.
pub fn run<I, S>(args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let cli_args = cli::parse_args(args.into_iter().map(Into::into))?;
    cli::dispatch(cli_args)
}
