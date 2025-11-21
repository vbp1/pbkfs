//! Copy-on-write overlay helpers used by tests and FUSE adapter.

use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::Result;

#[derive(Debug, Clone)]
pub struct Overlay {
    base: PathBuf,
    diff: PathBuf,
}

impl Overlay {
    pub fn new<P: AsRef<Path>, Q: AsRef<Path>>(base: P, diff: Q) -> Result<Self> {
        let base_path = base.as_ref().to_path_buf();
        let diff_root = diff.as_ref().to_path_buf();
        let data_path = diff_root.join("data");
        if !data_path.exists() {
            fs::create_dir_all(&data_path)?;
        }
        Ok(Self {
            base: base_path,
            diff: data_path,
        })
    }

    pub fn read(&self, relative: impl AsRef<Path>) -> Result<Option<Vec<u8>>> {
        let rel = relative.as_ref();
        let diff_path = self.diff.join(rel);
        if diff_path.exists() {
            return Ok(Some(fs::read(diff_path)?));
        }
        let base_path = self.base.join(rel);
        if base_path.exists() {
            return Ok(Some(fs::read(base_path)?));
        }
        Ok(None)
    }

    pub fn write(&self, relative: impl AsRef<Path>, contents: &[u8]) -> Result<()> {
        let path = self.diff.join(relative.as_ref());
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, contents)?;
        Ok(())
    }

    pub fn list_diff_paths(&self) -> Result<Vec<PathBuf>> {
        let mut paths = Vec::new();
        for entry in walkdir::WalkDir::new(&self.diff)
            .into_iter()
            .filter_map(std::result::Result::ok)
        {
            if entry.file_type().is_file() {
                paths.push(entry.path().strip_prefix(&self.diff).unwrap().to_path_buf());
            }
        }
        Ok(paths)
    }

    pub fn base_root(&self) -> &Path {
        &self.base
    }

    pub fn diff_root(&self) -> &Path {
        &self.diff
    }
}
