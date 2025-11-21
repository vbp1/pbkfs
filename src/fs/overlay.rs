//! Copy-on-write overlay helpers used by tests and FUSE adapter.

use std::os::fd::AsRawFd;
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

#[derive(Debug, Clone)]
pub struct Layer {
    pub root: PathBuf,
    pub compression: Option<CompressionAlgorithm>,
    pub incremental: bool,
}

#[derive(Debug)]
struct OverlayInner {
    base: PathBuf,
    diff: PathBuf,
    layers: Vec<Layer>,                // newest -> oldest
    inflight: Mutex<HashSet<PathBuf>>, // tracks in-progress copy-ups per path
}

impl Overlay {
    pub fn new<P: AsRef<Path>, Q: AsRef<Path>>(base: P, diff: Q) -> Result<Self> {
        Self::new_with_layers(
            base.as_ref().to_path_buf(),
            diff,
            vec![Layer {
                root: base.as_ref().to_path_buf(),
                compression: None,
                incremental: false,
            }],
        )
    }

    pub fn new_with_compression<P: AsRef<Path>, Q: AsRef<Path>>(
        base: P,
        diff: Q,
        compression: Option<CompressionAlgorithm>,
    ) -> Result<Self> {
        Self::new_with_layers(
            base.as_ref().to_path_buf(),
            diff,
            vec![Layer {
                root: base.as_ref().to_path_buf(),
                compression,
                incremental: false,
            }],
        )
    }

    pub fn new_with_algorithms<P: AsRef<Path>, Q: AsRef<Path>>(
        base: P,
        diff: Q,
        compression_algorithms: Vec<CompressionAlgorithm>,
    ) -> Result<Self> {
        let layer = Layer {
            root: base.as_ref().to_path_buf(),
            compression: compression_algorithms.first().copied(),
            incremental: false,
        };
        Self::new_with_layers(base, diff, vec![layer])
    }

    pub fn new_with_layers<P: AsRef<Path>, Q: AsRef<Path>>(
        base: P,
        diff: Q,
        layers: Vec<Layer>,
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
                layers,
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

        let (base_path, compression, incremental) = match self.find_layer_path(rel) {
            Some(v) => v,
            None => return Ok(None),
        };

        if compression.is_some() {
            self.ensure_copy_up(rel)?;
            return Ok(Some(fs::read(diff_path)?));
        }

        if incremental {
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
        self.inner.layers.iter().find_map(|l| l.compression)
    }

    pub fn layer_roots(&self) -> Vec<(PathBuf, Option<CompressionAlgorithm>)> {
        self.inner
            .layers
            .iter()
            .map(|l| (l.root.clone(), l.compression))
            .collect()
    }

    /// Ensure a diff-side copy exists for the provided relative path, performing
    /// a decompress-on-first-read copy-up using the layer's compression algorithm.
    pub fn ensure_copy_up(&self, rel: &Path) -> Result<()> {
        let diff_path = self.inner.diff.join(rel);
        if diff_path.exists() {
            return Ok(());
        }

        let (base_path, compression, incremental) = self
            .find_layer_path(rel)
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
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

        if let Some(algo) = compression {
            return self.decompress_file(rel, &base_path, &diff_path, algo);
        }

        if incremental {
            return self.materialize_incremental(rel, &base_path, &diff_path);
        }

        if let Some(parent) = diff_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(&base_path, &diff_path)?;
        Ok(())
    }

    fn decompress_file(
        &self,
        rel: &Path,
        base_path: &Path,
        diff_path: &Path,
        algo: CompressionAlgorithm,
    ) -> Result<()> {
        {
            let mut inflight = self.inner.inflight.lock().unwrap();
            if inflight.contains(rel) {
                drop(inflight);
                return self.wait_for_existing(diff_path);
            }
            inflight.insert(rel.to_path_buf());
        }

        let result = match algo {
            CompressionAlgorithm::Zlib => {
                let reader = fs::File::open(base_path)?;
                let mut decoder = flate2::read::ZlibDecoder::new(reader);
                if let Some(parent) = diff_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                let mut out = fs::File::create(diff_path)?;
                io::copy(&mut decoder, &mut out)?;
                Ok(())
            }
            CompressionAlgorithm::Lz4 => {
                let bytes = fs::read(base_path)?;
                let decompressed = lz4_flex::block::decompress_size_prepended(&bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
                if let Some(parent) = diff_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(diff_path, decompressed)?;
                Ok(())
            }
            CompressionAlgorithm::Zstd => {
                let reader = fs::File::open(base_path)?;
                let mut decoder = zstd::stream::Decoder::new(reader)?;
                if let Some(parent) = diff_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                let mut out = fs::File::create(diff_path)?;
                io::copy(&mut decoder, &mut out)?;
                Ok(())
            }
        };

        let mut inflight = self.inner.inflight.lock().unwrap();
        inflight.remove(rel);
        result
    }

    fn materialize_incremental(&self, rel: &Path, inc_path: &Path, diff_path: &Path) -> Result<()> {
        // Find closest ancestor file in older layers.
        let ancestor = self
            .inner
            .layers
            .iter()
            .skip(1) // older layers after the current one
            .find_map(|layer| {
                let candidate = layer.root.join(rel);
                candidate
                    .exists()
                    .then_some((candidate, layer.compression, layer.incremental))
            })
            .ok_or_else(|| Error::MissingBackup(rel.display().to_string()))?;

        // Start from ancestor content.
        if let Some(parent) = diff_path.parent() {
            fs::create_dir_all(parent)?;
        }
        match fs::copy(&ancestor.0, diff_path) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // Create empty if ancestor missing (should not happen)
                fs::File::create(diff_path)?;
            }
            Err(e) => return Err(e.into()),
        }

        // Overlay changed extents from incremental file using sparse extents where possible.
        let mut inc_file = fs::File::open(inc_path)?;
        let mut out = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(diff_path)?;

        // Try SEEK_DATA/SEEK_HOLE to preserve holes; fallback to full copy.
        #[cfg(target_os = "linux")]
        {
            use libc::{loff_t, SEEK_DATA, SEEK_HOLE};
            use std::os::unix::fs::FileExt;

            let mut off: loff_t = 0;
            let inc_fd = inc_file.as_raw_fd();
            let mut buf = vec![0u8; 8192 * 4];
            loop {
                let data_off = unsafe { libc::lseek(inc_fd, off, SEEK_DATA) };
                if data_off == -1 {
                    break;
                }
                let hole_off = unsafe { libc::lseek(inc_fd, data_off, SEEK_HOLE) };
                let end = if hole_off == -1 { data_off } else { hole_off };
                let mut cursor = data_off as u64;
                while cursor < end as u64 {
                    let to_read = std::cmp::min(buf.len() as u64, end as u64 - cursor) as usize;
                    let n = inc_file.read_at(&mut buf[..to_read], cursor)?;
                    if n == 0 {
                        break;
                    }
                    out.write_all_at(&buf[..n], cursor)?;
                    cursor += n as u64;
                }
                off = end;
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            io::copy(&mut inc_file, &mut out)?;
        }

        Ok(())
    }

    pub fn find_layer_path(
        &self,
        rel: &Path,
    ) -> Option<(PathBuf, Option<CompressionAlgorithm>, bool)> {
        for layer in &self.inner.layers {
            let candidate = layer.root.join(rel);
            if candidate.exists() {
                return Some((candidate, layer.compression, layer.incremental));
            }
        }
        None
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
