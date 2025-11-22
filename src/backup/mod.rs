//! Backup domain module scaffolding.
//!
//! Contains metadata loading from pg_probackup output and backup chain
//! construction/validation utilities.

pub mod chain;
pub mod metadata;

pub use chain::{BackupChain, ChainIntegrity};
pub use metadata::{
    BackupMetadata, BackupMode, BackupStatus, BackupStore, BackupType, ChecksumState, Compression,
    CompressionAlgorithm,
};
