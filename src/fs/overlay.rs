//! Copy-on-write overlay helpers used by tests and FUSE adapter.

use std::{
    collections::HashSet,
    fs, io,
    io::{BufReader, Read},
    os::unix::fs::{symlink, FileExt},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use crate::{
    backup::metadata::{BackupMode, CompressionAlgorithm},
    Error, Result,
};
use walkdir::WalkDir;

const COPYUP_WAIT_RETRIES: usize = 50;
const COPYUP_WAIT_INTERVAL: Duration = Duration::from_millis(10);
const BLCKSZ: usize = 8192;
const PAGE_TRUNCATED: i32 = -2;

#[repr(C)]
struct BackupPageHeader {
    block: u32,
    compressed_size: i32,
}

#[derive(Debug, Clone)]
pub struct Overlay {
    inner: Arc<OverlayInner>,
}

#[derive(Debug, Clone)]
pub struct Layer {
    pub root: PathBuf,
    pub compression: Option<CompressionAlgorithm>,
    pub incremental: bool,
    pub backup_mode: BackupMode,
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
                backup_mode: BackupMode::Full,
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
                backup_mode: BackupMode::Full,
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
            backup_mode: BackupMode::Full,
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

        if incremental {
            self.ensure_copy_up(rel)?;
            return Ok(Some(fs::read(diff_path)?));
        }

        if compression.is_some() {
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

        let matches = self.matching_layers(rel);
        if matches.is_empty() {
            return Err(io::Error::from(io::ErrorKind::NotFound).into());
        }

        let (top_idx, top_path) = &matches[0];
        let top_layer = &self.inner.layers[*top_idx];
        let meta = fs::symlink_metadata(top_path)?;

        if meta.is_dir() {
            fs::create_dir_all(&diff_path)?;
            return Ok(());
        }

        if meta.file_type().is_symlink() {
            if let Some(parent) = diff_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let target = fs::read_link(top_path)?;
            symlink(target, &diff_path)?;
            return Ok(());
        }

        if !meta.is_file() {
            return Err(
                io::Error::new(io::ErrorKind::Other, "unsupported file type for copy-up").into(),
            );
        }

        if top_layer.incremental {
            return self.materialize_incremental_chain(rel, &diff_path, matches);
        }

        if let Some(algo) = top_layer.compression {
            return self.decompress_file(rel, top_path, &diff_path, algo);
        }

        if let Some(parent) = diff_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(top_path, &diff_path)?;
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

        let result = self.decompress_file_inner(base_path, diff_path, algo);

        let mut inflight = self.inner.inflight.lock().unwrap();
        inflight.remove(rel);
        result
    }

    fn decompress_file_inner(
        &self,
        base_path: &Path,
        diff_path: &Path,
        algo: CompressionAlgorithm,
    ) -> Result<()> {
        match algo {
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
        }
    }

    fn materialize_incremental_chain(
        &self,
        rel: &Path,
        diff_path: &Path,
        matches: Vec<(usize, PathBuf)>,
    ) -> Result<()> {
        {
            let mut inflight = self.inner.inflight.lock().unwrap();
            if inflight.contains(rel) {
                drop(inflight);
                return self.wait_for_existing(diff_path);
            }
            inflight.insert(rel.to_path_buf());
        }

        let result = self.do_materialize_incremental_chain(rel, diff_path, matches);

        let mut inflight = self.inner.inflight.lock().unwrap();
        inflight.remove(rel);
        result
    }

    fn do_materialize_incremental_chain(
        &self,
        rel: &Path,
        diff_path: &Path,
        mut matches: Vec<(usize, PathBuf)>,
    ) -> Result<()> {
        // Process oldest -> newest to build the final file.
        matches.reverse();

        let base_pos = matches
            .iter()
            .position(|(idx, _)| !self.inner.layers[*idx].incremental);
        let base_pos = base_pos.ok_or_else(|| Error::MissingBackup(rel.display().to_string()))?;
        let (base_idx, base_path) = matches.remove(base_pos);

        self.copy_base(&base_path, diff_path, self.inner.layers[base_idx].compression)?;

        // Apply incremental layers in chronological order after the base.
        for (idx, path) in matches.into_iter() {
            let layer = &self.inner.layers[idx];
            if !layer.incremental {
                continue;
            }
            self.apply_incremental_layer(rel, diff_path, &path, layer)?;
        }

        Ok(())
    }

    fn copy_base(
        &self,
        base_path: &Path,
        diff_path: &Path,
        compression: Option<CompressionAlgorithm>,
    ) -> Result<()> {
        let meta = fs::symlink_metadata(base_path)?;
        if !meta.is_file() {
            return Err(
                io::Error::new(io::ErrorKind::Other, "unsupported base type for incremental").into(),
            );
        }

        if let Some(parent) = diff_path.parent() {
            fs::create_dir_all(parent)?;
        }

        match compression {
            Some(algo) => self.decompress_file_inner(base_path, diff_path, algo),
            None => {
                fs::copy(base_path, diff_path)?;
                Ok(())
            }
        }
    }

    fn apply_incremental_layer(
        &self,
        rel: &Path,
        diff_path: &Path,
        inc_path: &Path,
        layer: &Layer,
    ) -> Result<()> {
        if !inc_path.exists() {
            return Ok(()); // nothing to apply
        }

        let expected = match layer.backup_mode {
            BackupMode::Delta | BackupMode::Page => self.load_pagemap(inc_path)?,
            _ => None,
        };

        let mut reader = self.open_incremental_reader(inc_path, layer.compression)?;
        let out = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(diff_path)?;

        let mut seen = HashSet::new();
        loop {
            let mut hdr = [0u8; std::mem::size_of::<BackupPageHeader>()];
            match reader.read_exact(&mut hdr) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let block = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
            let compressed_size = i32::from_le_bytes(hdr[4..8].try_into().unwrap());

            if compressed_size == PAGE_TRUNCATED {
                out.set_len(block as u64 * BLCKSZ as u64)?;
                break;
            }

            if compressed_size <= 0 {
                return Err(Error::InvalidIncrementalPageSize {
                    path: rel.display().to_string(),
                    block,
                    expected: BLCKSZ,
                    actual: compressed_size.max(0) as usize,
                }
                .into());
            }

            let mut buf = vec![0u8; compressed_size as usize];
            reader.read_exact(&mut buf)?;

            let page = if let Some(algo) = layer.compression {
                self.decompress_page(algo, &buf)?
            } else {
                buf
            };

            if page.len() != BLCKSZ {
                return Err(Error::InvalidIncrementalPageSize {
                    path: rel.display().to_string(),
                    block,
                    expected: BLCKSZ,
                    actual: page.len(),
                }
                .into());
            }

            out.write_all_at(&page, block as u64 * BLCKSZ as u64)?;
            seen.insert(block);
        }

        if let Some(expected_blocks) = expected {
            let missing: Vec<u32> = expected_blocks
                .difference(&seen)
                .copied()
                .collect();
            if !missing.is_empty() {
                return Err(Error::IncompleteIncremental {
                    path: rel.display().to_string(),
                    missing,
                }
                .into());
            }
        }

        Ok(())
    }

    fn open_incremental_reader(
        &self,
        path: &Path,
        _compression: Option<CompressionAlgorithm>,
    ) -> Result<Box<dyn Read>> {
        let file = fs::File::open(path)?;
        Ok(Box::new(BufReader::new(file)))
    }

    fn decompress_page(
        &self,
        algo: CompressionAlgorithm,
        data: &[u8],
    ) -> Result<Vec<u8>> {
        match algo {
            CompressionAlgorithm::Zlib => {
                let mut decoder = flate2::read::ZlibDecoder::new(data);
                let mut out = Vec::new();
                decoder.read_to_end(&mut out)?;
                Ok(out)
            }
            CompressionAlgorithm::Lz4 => lz4_flex::block::decompress_size_prepended(data)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e).into()),
            CompressionAlgorithm::Zstd => zstd::stream::decode_all(data)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e).into()),
        }
    }

    fn load_pagemap(&self, inc_path: &Path) -> Result<Option<HashSet<u32>>> {
        let candidate = inc_path.with_extension("pagemap");
        if !candidate.exists() {
            return Ok(None);
        }

        let bytes = fs::read(&candidate)?;
        let mut blocks = HashSet::new();
        for (byte_idx, byte) in bytes.iter().enumerate() {
            for bit in 0..8 {
                if byte & (1 << bit) != 0 {
                    blocks.insert((byte_idx * 8 + bit) as u32);
                }
            }
        }

        Ok(Some(blocks))
    }

    pub fn find_layer_path(
        &self,
        rel: &Path,
    ) -> Option<(PathBuf, Option<CompressionAlgorithm>, bool)> {
        self.matching_layers(rel)
            .into_iter()
            .next()
            .map(|(idx, candidate)| {
                let layer = &self.inner.layers[idx];
                (candidate, layer.compression, layer.incremental)
            })
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

    fn matching_layers(&self, rel: &Path) -> Vec<(usize, PathBuf)> {
        let mut matches = Vec::new();
        for (idx, layer) in self.inner.layers.iter().enumerate() {
            let candidate = layer.root.join(rel);
            if candidate.exists() {
                matches.push((idx, candidate));
            }
        }
        matches
    }
}

/// Remove all contents of a diff directory while leaving the root intact.
///
/// Symlinks are removed as links (no traversal). Directories are removed
/// recursively. The `diff_root` itself must already exist.
pub fn wipe_diff_dir(diff_root: &Path) -> Result<()> {
    if !diff_root.exists() {
        return Ok(());
    }

    if diff_root.parent().is_none() {
        return Err(Error::Cli("refusing to wipe root path".into()).into());
    }

    for entry in fs::read_dir(diff_root)? {
        let entry = entry?;
        let path = entry.path();

        let ft = entry.file_type()?;
        if ft.is_symlink() {
            fs::remove_file(&path)?;
        } else if ft.is_dir() {
            fs::remove_dir_all(&path)?;
        } else {
            fs::remove_file(&path)?;
        }
    }
    Ok(())
}
