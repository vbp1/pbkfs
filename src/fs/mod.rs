//! Filesystem module scaffolding for pbkfs.
//!
//! Provides lightweight types for mount targets and sessions plus the FUSE
//! adapter and copy-on-write overlay support.

use std::path::{Path, PathBuf};

use crate::{Error, Result};

pub mod fuse;
pub mod overlay;
pub mod pending;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MountState {
    Idle,
    Mounted,
    Error,
}

#[derive(Debug, Clone)]
pub struct MountTarget {
    pub path: PathBuf,
    pub state: MountState,
    pub current_mount_id: Option<uuid::Uuid>,
}

impl MountTarget {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            state: MountState::Idle,
            current_mount_id: None,
        }
    }

    pub fn validate_empty(&self) -> Result<()> {
        if !self.path.exists() || !self.path.is_dir() {
            return Err(Error::InvalidTargetDir(self.path.display().to_string()).into());
        }

        if std::fs::read_dir(&self.path)
            .map_err(Error::from)?
            .next()
            .is_some()
        {
            return Err(Error::TargetNotEmpty(self.path.display().to_string()).into());
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MountSessionState {
    Starting,
    Ready,
    Failed,
    Unmounted,
}

#[derive(Debug, Clone)]
pub struct MountSession {
    pub mount_id: uuid::Uuid,
    pub binding_id: uuid::Uuid,
    pub pbk_target_path: PathBuf,
    pub started_at: std::time::SystemTime,
    pub ended_at: Option<std::time::SystemTime>,
    pub state: MountSessionState,
    pub error: Option<String>,
}

impl MountSession {
    pub fn new(binding_id: uuid::Uuid, target: impl AsRef<Path>) -> Self {
        Self {
            mount_id: uuid::Uuid::new_v4(),
            binding_id,
            pbk_target_path: target.as_ref().to_path_buf(),
            started_at: std::time::SystemTime::now(),
            ended_at: None,
            state: MountSessionState::Starting,
            error: None,
        }
    }

    pub fn mark_ready(&mut self) {
        self.state = MountSessionState::Ready;
    }

    pub fn mark_failed(&mut self, msg: impl Into<String>) {
        self.state = MountSessionState::Failed;
        self.error = Some(msg.into());
        self.ended_at = Some(std::time::SystemTime::now());
    }

    pub fn mark_unmounted(&mut self) {
        self.state = MountSessionState::Unmounted;
        self.ended_at = Some(std::time::SystemTime::now());
    }
}
