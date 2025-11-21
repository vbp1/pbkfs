//! Copy-on-write overlay helpers used by tests and FUSE adapter.

use std::{
    collections::HashSet,
    fs, io,
    os::unix::fs::symlink,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use crate::{backup::metadata::CompressionAlgorithm, Error, Result};
use walkdir::WalkDir;

const COPYUP_WAIT_RETRIES: usize = 50;
const COPYUP_WAIT_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Debug, Clone)]
pub struct Overlay {
    inner: Arc<OverlayInner>,
}

#[derive(Debug)]
struct OverlayInner {
    base: PathBuf,
    diff: PathBuf,
    compression: Vec<CompressionAlgorithm>,
    inflight: Mutex<HashSet<PathBuf>>, // tracks in-progress copy-ups per path
}

impl Overlay {
    pub fn new<P: AsRef<Path>, Q: AsRef<Path>>(base: P, diff: Q) -> Result<Self> {
        Self::new_with_compression(base, diff, None)
    }

    pub fn new_with_compression<P: AsRef<Path>, Q: AsRef<Path>>(
        base: P,
        diff: Q,
        compression: Option<CompressionAlgorithm>,
    ) -> Result<Self> {
        let list = compression.map(|c| vec![c]).unwrap_or_default();
        Self::new_with_algorithms(base, diff, list)
    }

    pub fn new_with_algorithms<P: AsRef<Path>, Q: AsRef<Path>>(
        base: P,
        diff: Q,
        compression_algorithms: Vec<CompressionAlgorithm>,
    ) -> Result<Self> {
        let base_path = base.as_ref().to_path_buf();
        let diff_root = diff.as_ref().to_path_buf();
        let data_path = diff_root.join("data");
        if !data_path.exists() {
            fs::create_dir_all(&data_path)?;
        }
        Ok(Self {
            inner: Arc::new(OverlayInner {
                base: base_path,
                diff: data_path,
                compression: compression_algorithms,
                inflight: Mutex::new(HashSet::new()),
            }),
        })
    }

    pub fn read(&self, relative: impl AsRef<Path>) -> Result<Option<Vec<u8>>> {
        let rel = relative.as_ref();
        let diff_path = self.inner.diff.join(rel);
        if diff_path.exists() {
            return Ok(Some(fs::read(diff_path)?));
        }

        let base_path = self.inner.base.join(rel);
        if !base_path.exists() {
            return Ok(None);
        }

        if !self.inner.compression.is_empty() {
            self.ensure_copy_up(rel)?;
            return Ok(Some(fs::read(diff_path)?));
        }

        Ok(Some(fs::read(base_path)?))
    }

    pub fn write(&self, relative: impl AsRef<Path>, contents: &[u8]) -> Result<()> {
        let rel = relative.as_ref();
        let path = self.inner.diff.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, contents)?;
        Ok(())
    }

    pub fn list_diff_paths(&self) -> Result<Vec<PathBuf>> {
        let mut paths = Vec::new();
        for entry in WalkDir::new(&self.inner.diff)
            .into_iter()
            .filter_map(std::result::Result::ok)
        {
            if entry.file_type().is_file() {
                paths.push(
                    entry
                        .path()
                        .strip_prefix(&self.inner.diff)
                        .unwrap()
                        .to_path_buf(),
                );
            }
        }
        Ok(paths)
    }

    pub fn base_root(&self) -> &Path {
        &self.inner.base
    }

    pub fn diff_root(&self) -> &Path {
        &self.inner.diff
    }

    pub fn compression_algorithm(&self) -> Option<CompressionAlgorithm> {
        self.inner.compression.first().copied()
    }

    /// Ensure a diff-side copy exists for the provided relative path, performing
    /// a decompress-on-first-read copy-up when a compression algorithm is set.
    pub fn ensure_copy_up(&self, rel: &Path) -> Result<()> {
        let diff_path = self.inner.diff.join(rel);
        if diff_path.exists() {
            return Ok(());
        }

        let base_path = self.inner.base.join(rel);
        let meta = fs::symlink_metadata(&base_path)?;

        if meta.is_dir() {
            fs::create_dir_all(&diff_path)?;
            return Ok(());
        }

        if meta.file_type().is_symlink() {
            if let Some(parent) = diff_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let target = fs::read_link(&base_path)?;
            symlink(target, &diff_path)?;
            return Ok(());
        }

        if !meta.is_file() {
            return Err(
                io::Error::new(io::ErrorKind::Other, "unsupported file type for copy-up").into(),
            );
        }

        if !self.inner.compression.is_empty() {
            return self.decompress_file(rel, &base_path, &diff_path);
        }

        if let Some(parent) = diff_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(&base_path, &diff_path)?;
        Ok(())
    }

    fn decompress_file(&self, rel: &Path, base_path: &Path, diff_path: &Path) -> Result<()> {
        {
            let mut inflight = self.inner.inflight.lock().unwrap();
            if inflight.contains(rel) {
                drop(inflight);
                return self.wait_for_existing(diff_path);
            }
            inflight.insert(rel.to_path_buf());
        }

        let result = self.try_decompress_any(base_path, diff_path);

        let mut inflight = self.inner.inflight.lock().unwrap();
        inflight.remove(rel);
        result
    }

    fn try_decompress_any(&self, base_path: &Path, diff_path: &Path) -> Result<()> {
        let mut errors = Vec::new();
        let algs: Vec<CompressionAlgorithm> = if self.inner.compression.is_empty() {
            vec![
                CompressionAlgorithm::Zstd,
                CompressionAlgorithm::Zlib,
                CompressionAlgorithm::Lz4,
            ]
        } else {
            self.inner.compression.clone()
        };

        for algo in algs {
            match self.decompress_with_algorithm(base_path, diff_path, algo) {
                Ok(()) => return Ok(()),
                Err(e) => errors.push((algo, e)),
            }
        }

        let joined = errors
            .into_iter()
            .map(|(a, e)| format!("{a:?}: {e}"))
            .collect::<Vec<_>>()
            .join("; ");
        Err(Error::UnsupportedCompressionAlgorithm(joined).into())
    }

    fn decompress_with_algorithm(
        &self,
        base_path: &Path,
        diff_path: &Path,
        algo: CompressionAlgorithm,
    ) -> Result<()> {
        if let Some(parent) = diff_path.parent() {
            fs::create_dir_all(parent)?;
        }

        match algo {
            CompressionAlgorithm::Zlib => {
                let reader = fs::File::open(base_path)?;
                let mut decoder = flate2::read::ZlibDecoder::new(reader);
                let mut out = fs::File::create(diff_path)?;
                io::copy(&mut decoder, &mut out)?;
                Ok(())
            }
            CompressionAlgorithm::Lz4 => {
                let bytes = fs::read(base_path)?;
                let decompressed = lz4_flex::block::decompress_size_prepended(&bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                fs::write(diff_path, decompressed)?;
                Ok(())
            }
            CompressionAlgorithm::Zstd => {
                let reader = fs::File::open(base_path)?;
                let mut decoder = zstd::stream::Decoder::new(reader)?;
                let mut out = fs::File::create(diff_path)?;
                io::copy(&mut decoder, &mut out)?;
                Ok(())
            }
        }
    }

    fn wait_for_existing(&self, diff_path: &Path) -> Result<()> {
        for _ in 0..COPYUP_WAIT_RETRIES {
            if diff_path.exists() {
                return Ok(());
            }
            thread::sleep(COPYUP_WAIT_INTERVAL);
        }
        Err(Error::Io(io::Error::new(
            io::ErrorKind::TimedOut,
            "waiting for copy-up to finish",
        ))
        .into())
    }
}
