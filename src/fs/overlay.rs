//! Copy-on-write overlay helpers used by tests and FUSE adapter.

use std::{
    collections::{HashMap, HashSet},
    fs, io,
    io::{BufReader, Read},
    num::NonZeroUsize,
    os::unix::fs::{symlink, FileExt},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

use crate::{
    backup::metadata::{BackupMode, CompressionAlgorithm},
    Error, Result,
};
use tracing::{debug, warn};
use walkdir::WalkDir;

const COPYUP_WAIT_RETRIES: usize = 50;
const COPYUP_WAIT_INTERVAL: Duration = Duration::from_millis(10);
const DEFAULT_BLCKSZ: usize = 8192;
const PAGE_TRUNCATED: i32 = -2;
// PostgreSQL OID is u32: up to 10 decimal digits (4_294_967_295).
const MAX_OID_DIGITS: usize = 10;
// pg_probackup segment numbers are bounded (pgFileSize): up to 5 digits.
const MAX_SEGMENT_DIGITS: usize = 5;

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

#[derive(Debug, Clone, Copy)]
enum BlockSource {
    Diff,
    Layer { idx: usize },
    Zero,
}

#[derive(Default, Debug, Clone)]
pub struct OverlayMetrics {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub fallback_used: u64,
    pub blocks_copied: u64,
    pub bytes_copied: u64,
}

#[derive(Default, Debug)]
struct OverlayMetricsInner {
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    fallback_used: AtomicU64,
    blocks_copied: AtomicU64,
    bytes_copied: AtomicU64,
}

impl OverlayMetricsInner {
    fn snapshot(&self) -> OverlayMetrics {
        OverlayMetrics {
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            fallback_used: self.fallback_used.load(Ordering::Relaxed),
            blocks_copied: self.blocks_copied.load(Ordering::Relaxed),
            bytes_copied: self.bytes_copied.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default, Debug, Clone)]
struct BlockCacheEntry {
    materialized: HashSet<u64>,
    sources: HashMap<u64, BlockSource>,
}

#[derive(Debug)]
struct OverlayInner {
    base: PathBuf,
    diff: PathBuf,
    layers: Vec<Layer>, // newest -> oldest
    block_size: NonZeroUsize,
    inflight: Mutex<HashSet<PathBuf>>, // tracks in-progress copy-ups per path
    cache: Mutex<HashMap<PathBuf, BlockCacheEntry>>,
    metrics: OverlayMetricsInner,
}

impl Overlay {
    pub fn new<P: AsRef<Path>, Q: AsRef<Path>>(base: P, diff: Q) -> Result<Self> {
        Self::new_with_layers_and_block_size(
            base.as_ref(),
            diff,
            vec![Layer {
                root: base.as_ref().to_path_buf(),
                compression: None,
                incremental: false,
                backup_mode: BackupMode::Full,
            }],
            DEFAULT_BLCKSZ,
        )
    }

    pub fn new_with_block_size<P: AsRef<Path>, Q: AsRef<Path>>(
        base: P,
        diff: Q,
        block_size: usize,
    ) -> Result<Self> {
        Self::new_with_layers_and_block_size(
            base.as_ref(),
            diff,
            vec![Layer {
                root: base.as_ref().to_path_buf(),
                compression: None,
                incremental: false,
                backup_mode: BackupMode::Full,
            }],
            block_size,
        )
    }

    pub fn new_with_compression<P: AsRef<Path>, Q: AsRef<Path>>(
        base: P,
        diff: Q,
        compression: Option<CompressionAlgorithm>,
    ) -> Result<Self> {
        Self::new_with_layers_and_block_size(
            base.as_ref(),
            diff,
            vec![Layer {
                root: base.as_ref().to_path_buf(),
                compression,
                incremental: false,
                backup_mode: BackupMode::Full,
            }],
            DEFAULT_BLCKSZ,
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
        Self::new_with_layers_and_block_size(base, diff, vec![layer], DEFAULT_BLCKSZ)
    }

    pub fn new_with_layers<P: AsRef<Path>, Q: AsRef<Path>>(
        base: P,
        diff: Q,
        layers: Vec<Layer>,
    ) -> Result<Self> {
        Self::new_with_layers_and_block_size(base, diff, layers, DEFAULT_BLCKSZ)
    }

    pub fn new_with_layers_and_block_size<P: AsRef<Path>, Q: AsRef<Path>>(
        base: P,
        diff: Q,
        layers: Vec<Layer>,
        block_size: usize,
    ) -> Result<Self> {
        let block_size = NonZeroUsize::new(block_size)
            .ok_or_else(|| Error::Cli("block size cannot be zero".into()))?;
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
                block_size,
                inflight: Mutex::new(HashSet::new()),
                cache: Mutex::new(HashMap::new()),
                metrics: OverlayMetricsInner::default(),
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

        // pg_probackup data files (FULL+incremental) and compressed layers
        // must always be materialized into the diff directory before reads.
        if incremental || compression.is_some() || self.is_pg_datafile(rel) {
            self.ensure_copy_up(rel)?;
            return Ok(Some(fs::read(diff_path)?));
        }

        Ok(Some(fs::read(base_path)?))
    }

    /// Block-aligned lazy range read. Materializes only the blocks that
    /// intersect the requested range into the diff directory, leaving holes
    /// elsewhere. Returns `None` when the path cannot be resolved in any
    /// layer.
    pub fn read_range(
        &self,
        relative: impl AsRef<Path>,
        offset: u64,
        size: usize,
    ) -> Result<Option<Vec<u8>>> {
        let rel = relative.as_ref();
        let diff_path = self.inner.diff.join(rel);

        // If already in diff and the path is NOT present in any base layer
        // (brand-new file), serve directly without block materialization to
        // avoid copy-up on creation.
        let layers = self.matching_layers(rel);
        if layers.is_empty() && diff_path.exists() {
            let meta_len = fs::metadata(&diff_path)?.len();
            if offset >= meta_len {
                self.inner
                    .metrics
                    .cache_hits
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(Some(Vec::new()));
            }
            let max_len = ((meta_len - offset) as usize).min(size);
            let mut buf = vec![0u8; max_len];
            let file = fs::OpenOptions::new().read(true).open(&diff_path)?;
            let read = file.read_at(&mut buf, offset)?;
            buf.truncate(read.min(max_len));
            self.inner
                .metrics
                .cache_hits
                .fetch_add(1, Ordering::Relaxed);
            return Ok(Some(buf));
        }

        // Quick existence check to short-circuit missing files.
        if layers.is_empty() {
            return Ok(None);
        }

        if let Some(len) = self.logical_len(rel)? {
            if offset >= len {
                return Ok(Some(Vec::new()));
            }
        }

        if let Some(parent) = diff_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let block_size = self.inner.block_size.get() as u64;

        let read_start = Instant::now();

        let end_offset = offset.saturating_add(size as u64);
        if size == 0 {
            return Ok(Some(Vec::new()));
        }

        let start_block = offset / block_size;
        let end_block = end_offset.saturating_sub(1) / block_size;

        debug!(
            path = %rel.display(),
            offset,
            size,
            start_block,
            end_block,
            "read_range_start"
        );

        // Materialize only the requested blocks.
        for block in start_block..=end_block {
            self.materialize_block(rel, block)?;
        }

        // Read requested slice directly from diff (sparse holes read as zeroes).
        let meta_len = fs::metadata(&diff_path)?.len();
        if offset >= meta_len {
            return Ok(Some(Vec::new()));
        }
        let max_len = ((meta_len - offset) as usize).min(size);
        let mut buf = vec![0u8; max_len];
        let file = fs::OpenOptions::new().read(true).open(&diff_path)?;
        let read = file.read_at(&mut buf, offset)?;
        buf.truncate(read.min(max_len));
        debug!(
            path = %rel.display(),
            offset,
            size,
            start_block,
            end_block,
            bytes_returned = buf.len(),
            elapsed_us = read_start.elapsed().as_micros() as u64,
            "read_range_done"
        );
        Ok(Some(buf))
    }

    fn logical_len(&self, rel: &Path) -> Result<Option<u64>> {
        let diff_path = self.inner.diff.join(rel);
        let mut best: u64 = 0;
        if let Ok(meta) = fs::metadata(&diff_path) {
            best = meta.len();
        }

        for (idx, path) in self.matching_layers(rel) {
            if let Ok(meta) = fs::metadata(&path) {
                best = best.max(meta.len());
                if !self.inner.layers[idx].incremental {
                    return Ok(Some(best));
                }
            }
        }

        if best == 0 {
            Ok(None)
        } else {
            Ok(Some(best))
        }
    }

    fn materialize_block(&self, rel: &Path, block_idx: u64) -> Result<()> {
        // Check cache first to avoid rescans.
        if let Ok(mut cache) = self.inner.cache.lock() {
            let entry = cache.entry(rel.to_path_buf()).or_default();
            if entry.materialized.contains(&block_idx) {
                self.inner
                    .metrics
                    .cache_hits
                    .fetch_add(1, Ordering::Relaxed);
                debug!(path = %rel.display(), block = block_idx, "block_cache_hit_materialized");
                return Ok(());
            }
            if let Some(source) = entry.sources.get(&block_idx).copied() {
                self.inner
                    .metrics
                    .cache_hits
                    .fetch_add(1, Ordering::Relaxed);
                debug!(path = %rel.display(), block = block_idx, source = ?source, "block_cache_hit_source");
                drop(cache);
                return self.copy_block(rel, block_idx, source);
            }
            self.inner
                .metrics
                .cache_misses
                .fetch_add(1, Ordering::Relaxed);
            debug!(path = %rel.display(), block = block_idx, "block_cache_miss");
        }

        let source = match self.locate_block_source(rel, block_idx)? {
            Some(s) => s,
            None => return Err(io::Error::from(io::ErrorKind::NotFound).into()),
        };

        if let Ok(mut cache) = self.inner.cache.lock() {
            let entry = cache.entry(rel.to_path_buf()).or_default();
            entry.sources.insert(block_idx, source);
        }

        self.copy_block(rel, block_idx, source)
    }

    fn locate_block_source(&self, rel: &Path, block_idx: u64) -> Result<Option<BlockSource>> {
        self.locate_block_source_from(rel, block_idx, 0)
    }

    fn locate_block_source_from(
        &self,
        rel: &Path,
        block_idx: u64,
        start_layer: usize,
    ) -> Result<Option<BlockSource>> {
        let block_offset = self.block_offset(block_idx);
        let diff_path = self.inner.diff.join(rel);
        let cache = self.inner.cache.lock().unwrap();
        if let Some(entry) = cache.get(rel) {
            if entry.materialized.contains(&block_idx) {
                let src = BlockSource::Diff;
                debug!(path = %rel.display(), block = block_idx, source = ?src, "block_source_cache_materialized");
                return Ok(Some(src));
            }
        } else if let Ok(meta) = fs::metadata(&diff_path) {
            if meta.len() > block_offset {
                // Fresh cache but diff already has data (likely reuse path).
                let src = BlockSource::Diff;
                debug!(path = %rel.display(), block = block_idx, source = ?src, "block_source_diff_reused");
                return Ok(Some(src));
            }
        }
        drop(cache);

        for (idx, layer) in self.inner.layers.iter().enumerate().skip(start_layer) {
            let path = layer.root.join(rel);
            if !path.exists() {
                continue;
            }
            if layer.incremental {
                if let Some(pagemap) = self.load_pagemap(&path)? {
                    if !pagemap.contains(&(block_idx as u32)) {
                        continue;
                    }
                }
                let src = BlockSource::Layer { idx };
                debug!(path = %rel.display(), block = block_idx, layer = idx, source = ?src, "block_source_layer_incremental");
                return Ok(Some(src));
            }

            let src = BlockSource::Layer { idx };
            debug!(path = %rel.display(), block = block_idx, layer = idx, source = ?src, "block_source_layer_full");
            return Ok(Some(src));
        }

        if self.is_pg_datafile(rel) {
            let src = BlockSource::Zero;
            debug!(path = %rel.display(), block = block_idx, source = ?src, "block_source_zero_pgdata");
            return Ok(Some(src));
        }

        Ok(None)
    }

    fn copy_block(&self, rel: &Path, block_idx: u64, source: BlockSource) -> Result<()> {
        let offset = self.block_offset(block_idx);
        let block_size = self.inner.block_size.get();

        let maybe_data = match source {
            BlockSource::Diff => None, // already present
            BlockSource::Zero => Some(vec![0u8; block_size]),
            BlockSource::Layer { idx } => {
                let layer = &self.inner.layers[idx];
                let path = layer.root.join(rel);
                let data = if layer.incremental {
                    self.read_incremental_block(rel, &path, layer, block_idx)?
                } else {
                    self.read_full_block(rel, &path, layer, block_idx)?
                };

                if data.is_none() {
                    if let Some(next) = self.locate_block_source_from(rel, block_idx, idx + 1)? {
                        if let Ok(mut cache) = self.inner.cache.lock() {
                            let entry = cache.entry(rel.to_path_buf()).or_default();
                            entry.sources.insert(block_idx, next);
                        }
                        return self.copy_block(rel, block_idx, next);
                    }
                }

                data
            }
        };

        let mark_materialized = matches!(source, BlockSource::Diff) || maybe_data.is_some();

        if let Some(data) = maybe_data {
            let diff_path = self.inner.diff.join(rel);
            if let Some(parent) = diff_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let out = fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .open(&diff_path)?;
            out.write_all_at(&data, offset)?;
            // Preserve the logical file length reported by source layers without
            // artificially extending to full block size for small files.
            let data_end = offset.saturating_add(data.len() as u64);
            let logical_len = self.logical_len(rel)?.unwrap_or(data_end);
            let required_len = logical_len.max(data_end);
            if out.metadata()?.len() < required_len {
                out.set_len(required_len)?;
            }
            let file_len = out.metadata()?.len();
            debug!(
                path = %rel.display(),
                block = block_idx,
                bytes = data.len(),
                source = ?source,
                logical_len = required_len,
                file_len,
                "block_copy_up"
            );
            self.inner
                .metrics
                .blocks_copied
                .fetch_add(1, Ordering::Relaxed);
            self.inner
                .metrics
                .bytes_copied
                .fetch_add(data.len() as u64, Ordering::Relaxed);
        }

        if !mark_materialized {
            return Ok(());
        }

        // Mark materialized in cache.
        if let Ok(mut cache) = self.inner.cache.lock() {
            let entry = cache.entry(rel.to_path_buf()).or_default();
            entry.materialized.insert(block_idx);
        }

        Ok(())
    }

    fn read_incremental_block(
        &self,
        rel: &Path,
        inc_path: &Path,
        layer: &Layer,
        block_idx: u64,
    ) -> Result<Option<Vec<u8>>> {
        if !inc_path.exists() {
            return Ok(None);
        }

        let mut reader = self.open_incremental_reader(inc_path, layer.compression)?;
        let mut hdr = [0u8; std::mem::size_of::<BackupPageHeader>()];

        loop {
            match reader.read_exact(&mut hdr) {
                Ok(_) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(e.into()),
            }

            let block = u32::from_le_bytes(hdr[0..4].try_into().unwrap());
            let compressed_size = i32::from_le_bytes(hdr[4..8].try_into().unwrap());

            if compressed_size == PAGE_TRUNCATED {
                // Truncation marker means there are no further blocks.
                if block_idx >= block as u64 {
                    return Ok(None);
                }
                break;
            }

            if compressed_size <= 0 {
                return Err(Error::InvalidIncrementalPageSize {
                    path: rel.display().to_string(),
                    block,
                    expected: self.inner.block_size.get(),
                    actual: compressed_size.max(0) as usize,
                }
                .into());
            }

            let mut buf = vec![0u8; compressed_size as usize];
            reader.read_exact(&mut buf)?;

            if block as u64 == block_idx {
                let page =
                    self.decompress_page_if_needed(layer.compression, compressed_size, buf)?;
                if page.len() != self.inner.block_size.get() {
                    return Err(Error::InvalidIncrementalPageSize {
                        path: rel.display().to_string(),
                        block,
                        expected: self.inner.block_size.get(),
                        actual: page.len(),
                    }
                    .into());
                }
                return Ok(Some(page));
            }
        }

        Ok(None)
    }

    fn read_full_block(
        &self,
        rel: &Path,
        base_path: &Path,
        layer: &Layer,
        block_idx: u64,
    ) -> Result<Option<Vec<u8>>> {
        // For WAL files we bypass block-walk and materialize via legacy copy-up.
        if rel.to_string_lossy().contains("pg_wal") && layer.compression.is_some() {
            self.inner
                .metrics
                .fallback_used
                .fetch_add(1, Ordering::Relaxed);
            debug!(path = %rel.display(), block = block_idx, algo = ?layer.compression, "wal_fallback_full_copy");
            self.ensure_copy_up(rel)?;
            return Ok(None);
        }

        if self.is_pg_datafile(rel) {
            return self.read_pg_data_block(rel, base_path, layer.compression, block_idx);
        }

        // Compressed non-data files fallback to full copy-up for now.
        if layer.compression.is_some() {
            self.inner
                .metrics
                .fallback_used
                .fetch_add(1, Ordering::Relaxed);
            debug!(path = %rel.display(), block = block_idx, algo = ?layer.compression, "block_fallback_full_copy");
            self.ensure_copy_up(rel)?;
            return Ok(None);
        }

        if !base_path.exists() {
            return Ok(None);
        }

        let mut buf = vec![0u8; self.inner.block_size.get()];
        let file = fs::File::open(base_path)?;
        let read = file.read_at(&mut buf, self.block_offset(block_idx))?;
        if read == 0 {
            return Ok(None);
        }
        buf.truncate(read);
        Ok(Some(buf))
    }

    fn read_pg_data_block(
        &self,
        rel: &Path,
        src: &Path,
        compression: Option<CompressionAlgorithm>,
        block_idx: u64,
    ) -> Result<Option<Vec<u8>>> {
        let mut reader = fs::File::open(src)?;
        let mut header_buf = [0u8; std::mem::size_of::<BackupPageHeader>()];

        loop {
            match reader.read_exact(&mut header_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(e.into()),
            }

            let block = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
            let compressed_size = i32::from_le_bytes(header_buf[4..8].try_into().unwrap());

            if compressed_size == PAGE_TRUNCATED {
                // Truncation; logical size stops here.
                return Ok(None);
            }

            if compressed_size <= 0 || compressed_size as usize > self.inner.block_size.get() {
                return Err(Error::InvalidIncrementalPageSize {
                    path: rel.display().to_string(),
                    block,
                    expected: self.inner.block_size.get(),
                    actual: compressed_size.max(0) as usize,
                }
                .into());
            }

            let mut buf = vec![0u8; compressed_size as usize];
            reader.read_exact(&mut buf)?;

            if block as u64 == block_idx {
                let page = self.decompress_page_if_needed(compression, compressed_size, buf)?;
                if page.len() != self.inner.block_size.get() {
                    return Err(Error::InvalidIncrementalPageSize {
                        path: rel.display().to_string(),
                        block,
                        expected: self.inner.block_size.get(),
                        actual: page.len(),
                    }
                    .into());
                }
                return Ok(Some(page));
            }
        }
    }

    fn block_offset(&self, block_idx: u64) -> u64 {
        block_idx * self.inner.block_size.get() as u64
    }

    pub fn write(&self, relative: impl AsRef<Path>, contents: &[u8]) -> Result<()> {
        let rel = relative.as_ref();
        let path = self.inner.diff.join(rel);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, contents)?;
        self.invalidate_cache(rel);
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

    pub fn block_size(&self) -> usize {
        self.inner.block_size.get()
    }

    pub fn metrics(&self) -> OverlayMetrics {
        self.inner.metrics.snapshot()
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

    pub(crate) fn invalidate_cache(&self, rel: &Path) {
        if let Ok(mut cache) = self.inner.cache.lock() {
            cache.remove(rel);
        }
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
            // If this looks like a main-fork relation file (datafile) but is
            // absent from all layers, it most likely represents a relation
            // that existed but had no pages at backup time. PostgreSQL and
            // pg_probackup restore such relations as zero-length files, so
            // materialize an empty file in the diff to match that behavior.
            if self.is_pg_datafile(rel) {
                if let Some(parent) = diff_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&diff_path)?;
                return Ok(());
            }

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

        // For non-incremental main-fork relation files produced by pg_probackup,
        // materialize a plain heap file by decoding per-page headers into BLCKSZ
        // pages in the diff directory.
        if self.is_pg_datafile(rel) {
            return self.materialize_full_datafile(
                rel,
                top_path,
                &diff_path,
                top_layer.compression,
            );
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

        self.copy_base(
            &base_path,
            diff_path,
            self.inner.layers[base_idx].compression,
        )?;

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
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "unsupported base type for incremental",
            )
            .into());
        }

        if let Some(parent) = diff_path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Determine the logical relation path relative to diff root so we can
        // decide whether this is a main-fork data file that uses pg_probackup's
        // per-page BackupPageHeader format.
        let rel = diff_path
            .strip_prefix(&self.inner.diff)
            .unwrap_or(diff_path);

        if self.is_pg_datafile(rel) {
            return self.materialize_full_datafile(rel, base_path, diff_path, compression);
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
                out.set_len(block as u64 * self.inner.block_size.get() as u64)?;
                break;
            }

            if compressed_size <= 0 {
                return Err(Error::InvalidIncrementalPageSize {
                    path: rel.display().to_string(),
                    block,
                    expected: self.inner.block_size.get(),
                    actual: compressed_size.max(0) as usize,
                }
                .into());
            }

            let mut buf = vec![0u8; compressed_size as usize];
            reader.read_exact(&mut buf)?;

            let page = self.decompress_page_if_needed(layer.compression, compressed_size, buf)?;

            if page.len() != self.inner.block_size.get() {
                return Err(Error::InvalidIncrementalPageSize {
                    path: rel.display().to_string(),
                    block,
                    expected: self.inner.block_size.get(),
                    actual: page.len(),
                }
                .into());
            }

            out.write_all_at(&page, block as u64 * self.inner.block_size.get() as u64)?;
            seen.insert(block);
        }

        if let Some(expected_blocks) = expected {
            let missing: Vec<u32> = expected_blocks.difference(&seen).copied().collect();
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

    fn decompress_page_if_needed(
        &self,
        compression: Option<CompressionAlgorithm>,
        compressed_size: i32,
        buf: Vec<u8>,
    ) -> Result<Vec<u8>> {
        if compressed_size as usize != self.inner.block_size.get() {
            if let Some(algo) = compression {
                let start = Instant::now();
                let out = self.decompress_page(algo, &buf)?;
                debug!(
                    algo = ?algo,
                    compressed_size,
                    block_size = self.inner.block_size.get(),
                    elapsed_us = start.elapsed().as_micros() as u64,
                    "decompress_page"
                );
                return Ok(out);
            }
        }
        Ok(buf)
    }

    fn decompress_page(&self, algo: CompressionAlgorithm, data: &[u8]) -> Result<Vec<u8>> {
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

    /// Heuristic to detect PostgreSQL main-fork relation files that pg_probackup
    /// stores using per-page BackupPageHeader records.
    ///
    /// Mirrors pg_probackup's `set_forkname` logic for `is_datafile`, but only
    /// for the filename component:
    ///   - `<oid>` or `<oid>.<segno>` where oid/segno are decimal, no leading 0.
    ///   - No fork suffixes like `_vm`, `_fsm`, `_init`, `_ptrack`, or CFS
    ///     variants; any non-digit/`.` suffix causes this to return false.
    fn is_pg_datafile(&self, rel: &Path) -> bool {
        let name = match rel.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => return false,
        };

        let mut chars = name.chars().peekable();
        let first = match chars.next() {
            Some(c) if c.is_ascii_digit() => c,
            _ => return false,
        };
        if first == '0' {
            return false;
        }

        // Parse OID digits, enforcing an upper bound similar to pg_probackup.
        let mut oid_digits = 1usize;
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() {
                oid_digits += 1;
                if oid_digits > MAX_OID_DIGITS {
                    return false;
                }
                chars.next();
            } else {
                break;
            }
        }

        match chars.next() {
            None => true, // pure "<oid>"
            Some('.') => {
                // Optional ".<segno>" with 1..5 digits, no leading zero.
                let mut seg_digits = 0usize;
                let mut first_seg: Option<char> = None;
                for c in chars {
                    if !c.is_ascii_digit() {
                        return false;
                    }
                    if seg_digits == 0 {
                        first_seg = Some(c);
                        if c == '0' {
                            return false;
                        }
                    }
                    seg_digits += 1;
                    if seg_digits > MAX_SEGMENT_DIGITS {
                        return false;
                    }
                }
                seg_digits > 0 && first_seg.is_some()
            }
            // Any other suffix (e.g. "_vm", "_fsm", ".cfm") marks it as non-datafile.
            Some(_) => false,
        }
    }

    /// Materialize a pg_probackup-style FULL data file into a plain PostgreSQL
    /// heap file under the diff directory by decoding BackupPageHeader records.
    ///
    /// The source file layout is:
    ///   [BackupPageHeader {block, compressed_size}] [page_bytes] ...
    /// Pages may be per-page compressed; for `compression = None` they are
    /// stored uncompressed with `compressed_size == block_size`.
    fn materialize_full_datafile(
        &self,
        rel: &Path,
        src: &Path,
        dst: &Path,
        compression: Option<CompressionAlgorithm>,
    ) -> Result<()> {
        use std::io::{ErrorKind, Seek, SeekFrom};

        let mut reader = fs::File::open(src)?;
        let meta = reader.metadata()?;
        let total_len = meta.len();

        if total_len == 0 {
            if let Some(parent) = dst.parent() {
                fs::create_dir_all(parent)?;
            }
            let _ = fs::File::create(dst)?;
            return Ok(());
        }

        if total_len < std::mem::size_of::<BackupPageHeader>() as u64 {
            // Not a pg_probackup data file; fall back to a plain copy.
            if let Some(parent) = dst.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(src, dst)?;
            return Ok(());
        }

        if let Some(parent) = dst.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut out = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(dst)?;

        let mut max_block: Option<u32> = None;
        let mut header_buf = [0u8; std::mem::size_of::<BackupPageHeader>()];

        loop {
            match reader.read_exact(&mut header_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let block = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
            let compressed_size = i32::from_le_bytes(header_buf[4..8].try_into().unwrap());

            if compressed_size == PAGE_TRUNCATED {
                out.set_len(block as u64 * self.inner.block_size.get() as u64)?;
                warn!(
                    "PAGE_TRUNCATED at block {} in {}; remaining data ignored",
                    block,
                    rel.display()
                );
                // Ensure no trailing data after truncation marker; treat any
                // extra bytes as format corruption.
                let mut trailer = [0u8; 1];
                match reader.read(&mut trailer) {
                    Ok(0) => {}
                    Ok(_) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("data found after PAGE_TRUNCATED in {}", rel.display()),
                        )
                        .into());
                    }
                    Err(e) if e.kind() == ErrorKind::UnexpectedEof => {}
                    Err(e) => return Err(e.into()),
                }
                break;
            }

            if compressed_size <= 0 || compressed_size as usize > self.inner.block_size.get() {
                return Err(Error::InvalidIncrementalPageSize {
                    path: rel.display().to_string(),
                    block,
                    expected: self.inner.block_size.get(),
                    actual: compressed_size.max(0) as usize,
                }
                .into());
            }

            let mut buf = vec![0u8; compressed_size as usize];
            reader.read_exact(&mut buf)?;

            let page = self.decompress_page_if_needed(compression, compressed_size, buf)?;

            if page.len() != self.inner.block_size.get() {
                return Err(Error::InvalidIncrementalPageSize {
                    path: rel.display().to_string(),
                    block,
                    expected: self.inner.block_size.get(),
                    actual: page.len(),
                }
                .into());
            }

            out.write_all_at(&page, block as u64 * self.inner.block_size.get() as u64)?;
            max_block = Some(max_block.map_or(block, |b| b.max(block)));
        }

        if let Some(max_block) = max_block {
            let expected_len = (max_block as u64 + 1) * self.inner.block_size.get() as u64;
            let current_len = out.seek(SeekFrom::End(0))?;
            if current_len < expected_len {
                out.set_len(expected_len)?;
            }
        }

        Ok(())
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
