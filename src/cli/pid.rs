//! PID record handling for Phase 8 daemonization (`<MNT_PATH>/.pbkfs/worker.pid`).

use std::{
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{Error, Result};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PidRecord {
    pub pid: i32,
    pub diff_dir: PathBuf,
}

pub const DEFAULT_PID_FILE_REL: &str = ".pbkfs/worker.pid";
pub const DEFAULT_STAT_FILE_REL: &str = ".pbkfs/stat";

pub fn pid_alive(pid: i32) -> bool {
    if pid <= 0 {
        return false;
    }
    Path::new("/proc").join(pid.to_string()).exists()
}

pub fn read_pid_record(path: &Path) -> Result<PidRecord> {
    let bytes = fs::read(path)?;
    let record: PidRecord = serde_json::from_slice(&bytes)?;
    Ok(record)
}

pub fn write_pid_record(path: &Path, record: &PidRecord) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec_pretty(record)?;
    fs::write(path, bytes)?;
    Ok(())
}

pub fn prepare_pid_record(path: &Path, diff_dir: &Path, pid: i32) -> Result<()> {
    if path.exists() {
        let existing = read_pid_record(path)?;
        if pid_alive(existing.pid) {
            return Err(Error::Cli(format!(
                "refusing to mount: existing PID file is owned by alive pid {}",
                existing.pid
            ))
            .into());
        }
    }

    write_pid_record(
        path,
        &PidRecord {
            pid,
            diff_dir: diff_dir.to_path_buf(),
        },
    )
}
