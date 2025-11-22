use thiserror::Error;

pub mod backup;
pub mod binding;
pub mod cli;
pub mod fs;
pub mod logging;

pub type Result<T> = anyhow::Result<T>;

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
    #[error("binding violation: expected {expected}, got {actual}")]
    BindingViolation { expected: String, actual: String },
    #[error("diff directory not writable: {0}")]
    DiffDirNotWritable(String),
    #[error("unsupported pg_probackup version: {0}")]
    UnsupportedPgProbackupVersion(String),
    #[error("unsupported compression algorithm: {0}")]
    UnsupportedCompressionAlgorithm(String),
    #[error("missing compression metadata for compressed backup {0}")]
    MissingCompressionMetadata(String),
    #[error("compressed incremental backup unsupported without pagemap (algo={0:?})")]
    UnsupportedCompressedIncremental(crate::backup::CompressionAlgorithm),
    #[error("pagemap file missing for incremental: {0}")]
    MissingPagemap(String),
    #[error("incremental page size mismatch for {path} block {block}: expected {expected} got {actual}")]
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
}

/// Entry point for the library, called by the CLI thin wrapper.
pub fn run<I, S>(args: I) -> Result<()>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    // Initialize logging before doing anything else. Defaults to human format for the CLI.
    logging::init_logging(logging::LogFormat::Human)?;

    let cli_args = cli::parse_args(args.into_iter().map(Into::into))?;
    cli::dispatch(cli_args)
}
