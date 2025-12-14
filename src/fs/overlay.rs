//! Copy-on-write overlay helpers used by tests and FUSE adapter.

use std::{
    collections::{HashMap, HashSet},
    ffi::OsString,
    fs, io,
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    num::NonZeroUsize,
    os::unix::{
        fs::{symlink, FileExt},
        io::AsRawFd,
    },
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
    env_lock,
    fs::delta::{
        apply_patch_v2, compute_delta, hash_path, open_full, open_patch, punch_full_hole,
        read_full_page, read_patch_slot, shrink_full_tail, write_empty_slot, write_full_page,
        write_full_ref_slot, write_patch_slot, DeltaDiff, DeltaIndex, SlotKind, FULL_HEADER_SIZE,
        PAGE_SIZE,
    },
    Error, Result,
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
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
pub const DIRTY_MARKER: &str = ".pbkfs-dirty";

fn parse_bool_env(var: &str, default: bool) -> bool {
    // Tests mutate PBKFS_* env vars concurrently; serialize reads so each test
    // observes a consistent snapshot.
    let _guard = env_lock().lock();
    match std::env::var(var) {
        Ok(v) => matches!(v.as_str(), "1" | "true" | "TRUE" | "True"),
        Err(_) => default,
    }
}

#[repr(C)]
struct BackupPageHeader {
    block: u32,
    compressed_size: i32,
}

#[derive(Debug, Clone, Copy)]
struct FileMeta {
    compression: Option<CompressionAlgorithm>,
    is_datafile: bool,
    external_dir_num: u32,
    n_blocks: Option<u64>,
    full_size: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct BackupContentEntry {
    path: String,
    #[serde(rename = "compress_alg")]
    compress_alg: Option<String>,
    #[serde(rename = "is_datafile")]
    is_datafile: Option<String>,
    #[serde(rename = "external_dir_num")]
    external_dir_num: Option<String>,
    #[serde(rename = "n_blocks")]
    n_blocks: Option<String>,
    #[serde(rename = "full_size")]
    full_size: Option<String>,
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

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct OverlayMetrics {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub fallback_used: u64,
    pub blocks_copied: u64,
    pub bytes_copied: u64,
    pub delta_patch_count: u64,
    pub delta_full_count: u64,
    pub delta_patch_bytes: u64,
    pub delta_patch_max_size: u64,
    pub delta_patch_avg_size: u64,
    pub delta_bitmaps_loaded: u64,
    pub delta_bitmaps_total_bytes: u64,
    pub delta_punch_holes: u64,
    pub delta_punch_hole_failures: u64,
}

#[derive(Default, Debug)]
struct OverlayMetricsInner {
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    fallback_used: AtomicU64,
    blocks_copied: AtomicU64,
    bytes_copied: AtomicU64,
    delta_patch_count: AtomicU64,
    delta_full_count: AtomicU64,
    delta_patch_bytes: AtomicU64,
    delta_patch_max_size: AtomicU64,
    delta_bitmaps_loaded: AtomicU64,
    delta_bitmaps_total_bytes: AtomicU64,
    delta_punch_holes: AtomicU64,
    delta_punch_hole_failures: AtomicU64,
}

impl OverlayMetricsInner {
    fn snapshot(&self) -> OverlayMetrics {
        OverlayMetrics {
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            fallback_used: self.fallback_used.load(Ordering::Relaxed),
            blocks_copied: self.blocks_copied.load(Ordering::Relaxed),
            bytes_copied: self.bytes_copied.load(Ordering::Relaxed),
            delta_patch_count: self.delta_patch_count.load(Ordering::Relaxed),
            delta_full_count: self.delta_full_count.load(Ordering::Relaxed),
            delta_patch_bytes: self.delta_patch_bytes.load(Ordering::Relaxed),
            delta_patch_max_size: self.delta_patch_max_size.load(Ordering::Relaxed),
            delta_patch_avg_size: {
                let count = self.delta_patch_count.load(Ordering::Relaxed);
                if count == 0 {
                    0
                } else {
                    self.delta_patch_bytes.load(Ordering::Relaxed) / count
                }
            },
            delta_bitmaps_loaded: self.delta_bitmaps_loaded.load(Ordering::Relaxed),
            delta_bitmaps_total_bytes: self.delta_bitmaps_total_bytes.load(Ordering::Relaxed),
            delta_punch_holes: self.delta_punch_holes.load(Ordering::Relaxed),
            delta_punch_hole_failures: self.delta_punch_hole_failures.load(Ordering::Relaxed),
        }
    }

    fn reset(&self) {
        self.cache_hits.store(0, Ordering::Relaxed);
        self.cache_misses.store(0, Ordering::Relaxed);
        self.fallback_used.store(0, Ordering::Relaxed);
        self.blocks_copied.store(0, Ordering::Relaxed);
        self.bytes_copied.store(0, Ordering::Relaxed);
        self.delta_patch_count.store(0, Ordering::Relaxed);
        self.delta_full_count.store(0, Ordering::Relaxed);
        self.delta_patch_bytes.store(0, Ordering::Relaxed);
        self.delta_patch_max_size.store(0, Ordering::Relaxed);
        self.delta_bitmaps_loaded.store(0, Ordering::Relaxed);
        self.delta_bitmaps_total_bytes.store(0, Ordering::Relaxed);
        self.delta_punch_holes.store(0, Ordering::Relaxed);
        self.delta_punch_hole_failures.store(0, Ordering::Relaxed);
    }
}

#[derive(Default, Debug, Clone)]
struct BlockCacheEntry {
    materialized: HashSet<u64>,
    sources: HashMap<u64, BlockSource>,
    // Block indexes per physical path (layer or diff file) to avoid mixing
    // incremental/full variants of the same relation.
    pg_block_index: HashMap<PathBuf, Vec<BlockIndexEntry>>,
    // Highest byte offset written via FUSE, used to extend logical length for
    // datafiles even when deltas are empty (no bitmap/full growth).
    max_written_end: Option<u64>,
    // Cached logical length for datafiles, derived from backup layers and
    // delta artifacts. Kept separate from max_written_end so that writes can
    // invalidate it without dropping other cache state.
    logical_len: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct BlockIndexEntry {
    block: u32,
    offset: u64,
    compressed_size: i32,
}

type DeltaFileLockMap = HashMap<u64, Arc<RwLock<()>>>;
type DeltaBlockLockMap = HashMap<(u64, u64), Arc<RwLock<()>>>;

#[derive(Debug)]
struct OverlayInner {
    base: PathBuf,
    /// Root for diff contents (`<diff_dir>/data`).
    diff: PathBuf,
    layers: Vec<Layer>, // newest -> oldest
    block_size: NonZeroUsize,
    inflight: Mutex<HashSet<PathBuf>>, // tracks in-progress copy-ups per path
    cache: Mutex<HashMap<PathBuf, BlockCacheEntry>>,
    metrics: OverlayMetricsInner,
    // Per-layer file metadata loaded from pg_probackup's backup_content.control.
    // Keyed by layer root -> relative path -> metadata.
    file_meta: Mutex<HashMap<PathBuf, HashMap<String, FileMeta>>>,
    perf_unsafe: bool,
    dirty_paths: Mutex<HashSet<PathBuf>>,
    dirty_marker: PathBuf,
    delta_index: DeltaIndex,
    delta_file_locks: RwLock<DeltaFileLockMap>,
    delta_block_locks: RwLock<DeltaBlockLockMap>,
    no_wal: bool,
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

        let overlay = Self {
            inner: Arc::new(OverlayInner {
                base: base_path,
                diff: data_path,
                layers,
                block_size,
                inflight: Mutex::new(HashSet::new()),
                cache: Mutex::new(HashMap::new()),
                metrics: OverlayMetricsInner::default(),
                file_meta: Mutex::new(HashMap::new()),
                perf_unsafe: parse_bool_env("PBKFS_PERF_UNSAFE", false),
                dirty_paths: Mutex::new(HashSet::new()),
                dirty_marker: diff_root.join(DIRTY_MARKER),
                delta_index: DeltaIndex::new(),
                delta_file_locks: RwLock::new(HashMap::new()),
                delta_block_locks: RwLock::new(HashMap::new()),
                no_wal: parse_bool_env("PBKFS_NO_WAL", false),
            }),
        };

        if overlay.inner.perf_unsafe {
            // Touch perf-unsafe marker so remounts can detect unclean shutdowns.
            fs::write(&overlay.inner.dirty_marker, b"")?;
        }

        Ok(overlay)
    }

    fn file_lock(&self, rel: &Path) -> Arc<RwLock<()>> {
        let key = hash_path(rel);
        if let Some(lock) = self.inner.delta_file_locks.read().get(&key) {
            return Arc::clone(lock);
        }
        let mut write = self.inner.delta_file_locks.write();
        write
            .entry(key)
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    fn block_lock(&self, rel: &Path, block: u64) -> Arc<RwLock<()>> {
        let key = (hash_path(rel), block);
        if let Some(lock) = self.inner.delta_block_locks.read().get(&key) {
            return Arc::clone(lock);
        }
        let mut write = self.inner.delta_block_locks.write();
        write
            .entry(key)
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    fn cleanup_block_lock(&self, rel: &Path, block: u64, lock: &Arc<RwLock<()>>) {
        if Arc::strong_count(lock) > 1 {
            return;
        }
        let key = (hash_path(rel), block);
        let mut map = self.inner.delta_block_locks.write();
        if let Some(existing) = map.get(&key) {
            if Arc::strong_count(existing) == 1 {
                map.remove(&key);
            }
        }
    }

    fn cleanup_file_lock(&self, rel: &Path, lock: &Arc<RwLock<()>>) {
        if Arc::strong_count(lock) > 1 {
            return;
        }
        let key = hash_path(rel);
        let mut map = self.inner.delta_file_locks.write();
        if let Some(existing) = map.get(&key) {
            if Arc::strong_count(existing) == 1 {
                map.remove(&key);
            }
        }
    }

    fn clear_block_locks_for(&self, rel: &Path) {
        let key = hash_path(rel);
        let mut map = self.inner.delta_block_locks.write();
        map.retain(|(k, _), _| *k != key);
    }

    fn move_block_locks(&self, src: &Path, dst: &Path) {
        let src_key = hash_path(src);
        let dst_key = hash_path(dst);
        let mut map = self.inner.delta_block_locks.write();
        let entries: Vec<_> = map
            .iter()
            .filter(|((k, _), _)| *k == src_key)
            .map(|(k, v)| (*k, Arc::clone(v)))
            .collect();
        for ((_, block), lock) in entries {
            map.insert((dst_key, block), lock);
        }
        map.retain(|(k, _), _| *k != src_key);
    }

    fn move_file_lock(&self, src: &Path, dst: &Path) {
        let src_key = hash_path(src);
        let dst_key = hash_path(dst);
        let mut map = self.inner.delta_file_locks.write();
        if let Some(lock) = map.remove(&src_key) {
            map.insert(dst_key, lock);
        }
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

        // PostgreSQL datafiles are served via block-wise reads that reconstruct
        // pages from backup layers and delta artifacts without fully
        // materializing per-relation copies into the diff directory.
        if self.is_pg_datafile(rel) {
            let logical = self
                .logical_len(rel)?
                .unwrap_or_else(|| fs::metadata(&base_path).map(|m| m.len()).unwrap_or(0));
            return self.read_range(rel, 0, logical as usize);
        }

        // Compressed non-data files (both FULL and incremental) are stored as
        // whole files. Materialize a single decompressed copy in the diff
        // directory from the newest matching layer and serve reads from there.
        if incremental || compression.is_some() {
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
        let is_wal = self.is_pg_wal_child(rel);
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

        let is_datafile = self.is_pg_datafile(rel);

        // Respect logical length for all files (including PostgreSQL datafiles).
        // For heap/index files stored in pg_probackup's per-page format, this
        // uses page indexes rather than raw physical sizes to avoid extending
        // relations past their recorded length.
        let mut end_offset = offset.saturating_add(size as u64);
        let original_end = end_offset;

        // Always compute fresh logical length to avoid stale cached sizes
        // that can cause "unexpected data beyond EOF" or short-read errors.
        let logical_len = self.logical_len(rel)?;

        if let Some(len) = logical_len {
            if offset >= len {
                if is_datafile {
                    // Serve zeroed blocks beyond logical length instead of EOF to
                    // let WAL replay extend relations without raising short reads.
                    debug!(
                        path = %rel.display(),
                        offset,
                        logical_len = len,
                        size,
                        "read_range_logical_eof_zero_pgdata"
                    );
                    return Ok(Some(vec![0u8; size]));
                }
                debug!(
                    path = %rel.display(),
                    offset,
                    logical_len = len,
                    "read_range_logical_eof"
                );
                return Ok(Some(Vec::new()));
            }
            end_offset = end_offset.min(len);
        }

        if end_offset < original_end {
            debug!(
                path = %rel.display(),
                offset,
                original_end,
                clamped_end = end_offset,
                "read_range_clamped_to_logical_len"
            );
        }

        if let Some(parent) = diff_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let block_size = self.inner.block_size.get() as u64;

        if size == 0 {
            return Ok(Some(Vec::new()));
        }

        let read_start = Instant::now();

        let should_materialize = !(is_datafile || (is_wal && self.inner.no_wal));

        if !should_materialize {
            return self.read_range_nonmaterializing(rel, offset, end_offset, size, &layers);
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
            debug!(
                path = %rel.display(),
                offset,
                meta_len,
                "read_range_meta_eof"
            );
            if is_datafile {
                return Ok(Some(vec![0u8; size]));
            }
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
            logical_len = logical_len.unwrap_or(0),
            elapsed_us = read_start.elapsed().as_micros() as u64,
            "read_range_done"
        );
        Ok(Some(buf))
    }

    fn read_range_nonmaterializing(
        &self,
        rel: &Path,
        offset: u64,
        end_offset: u64,
        size: usize,
        _layers: &[(usize, PathBuf)],
    ) -> Result<Option<Vec<u8>>> {
        let diff_path = self.inner.diff.join(rel);
        let diff_preexisting = diff_path.exists();

        let block_size = self.inner.block_size.get() as u64;
        let start_block = offset / block_size;
        let end_block = end_offset.saturating_sub(1) / block_size;

        if end_offset <= offset {
            return Ok(Some(Vec::new()));
        }

        let mut out = vec![0u8; (end_offset - offset) as usize];

        for block in start_block..=end_block {
            let data = self.read_block_data(rel, block)?;
            let block_start = self.block_offset(block);
            let copy_start = offset.max(block_start);
            let copy_end = (block_start + self.inner.block_size.get() as u64).min(end_offset);
            let dst_start = (copy_start - offset) as usize;
            let dst_end = (copy_end - offset) as usize;
            let src_start = (copy_start - block_start) as usize;
            let src_end = src_start + (dst_end - dst_start);
            out[dst_start..dst_end].copy_from_slice(&data[src_start..src_end]);
        }

        // Trim to requested size (end_offset might have been clamped to logical length).
        if out.len() > size {
            out.truncate(size);
        }

        // Guarantee no diff file is left behind purely due to reads when
        // materialize_on_read is disabled. If a file existed beforehand (e.g.,
        // due to writes), we leave it untouched.
        if !diff_preexisting && diff_path.exists() {
            // Best-effort cleanup: remove file if it was created during a
            // non-materializing read. If the path somehow ended up as a
            // directory, remove_dir will clean it up as well.
            let _ = fs::remove_file(&diff_path).or_else(|_| fs::remove_dir(&diff_path));
        }

        Ok(Some(out))
    }

    fn logical_len(&self, rel: &Path) -> Result<Option<u64>> {
        // For PostgreSQL transaction/status files we should not combine lengths
        // from multiple backups: use the diff file if present, otherwise the
        // file from the newest matching layer only.
        if self.is_system_status_file(rel) {
            let diff_path = self.inner.diff.join(rel);
            if let Ok(meta) = fs::metadata(&diff_path) {
                return Ok(Some(meta.len()));
            }

            let matches = self.matching_layers(rel);
            if let Some((_, path)) = matches.first() {
                if let Ok(meta) = fs::metadata(path) {
                    return Ok(Some(meta.len()));
                }
            }
            return Ok(None);
        }

        // For heap/index files stored in page-mode format, physical file size
        // (with per-page headers/compression) is not the logical size. Derive
        // length from the highest page number we can observe and cache the
        // result per relation, invalidating it on writes/truncates.
        if self.is_pg_datafile(rel) {
            // If the top (target) backup does not list this relation as a
            // datafile in backup_content.control, treat it as absent (e.g.,
            // dropped relation) even if older backups still have the file.
            if !self.top_layer_has_datafile(rel) {
                return Ok(None);
            }

            // Fast path: reuse cached logical length when available. Writes and
            // truncates clear this field to avoid stale sizes.
            if let Ok(cache) = self.inner.cache.lock() {
                if let Some(entry) = cache.get(rel) {
                    if let Some(len) = entry.logical_len {
                        return Ok(Some(len));
                    }
                }
            }

            let len = self.datafile_logical_len(rel)?;
            if let Some(v) = len {
                if let Ok(mut cache) = self.inner.cache.lock() {
                    let entry = cache.entry(rel.to_path_buf()).or_default();
                    entry.logical_len = Some(v);
                }
            }
            return Ok(len);
        }

        // Always read current diff file size from disk, not cache.
        // Files can be extended via writes, and cached sizes become stale.
        let diff_len = fs::metadata(self.inner.diff.join(rel))
            .map(|m| m.len())
            .ok();
        let mut best: u64 = diff_len.unwrap_or(0);

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

    /// Compute logical length for PostgreSQL datafiles (tables and indexes)
    /// using page indexes rather than raw compressed file sizes.
    fn datafile_logical_len(&self, rel: &Path) -> Result<Option<u64>> {
        // Always derive logical length from all writable surfaces (diff copy,
        // delta patch/full) plus backup layers. Never rely on cached sizes:
        // PostgreSQL may extend relations via writes, and stale cached lengths
        // lead to "unexpected data beyond EOF".
        let diff_path = self.inner.diff.join(rel);
        let diff_len = fs::metadata(&diff_path).map(|m| m.len()).ok();
        let (patch_path, full_path) = self.delta_paths(rel);
        let patch_exists = patch_path.exists();
        let full_exists = full_path.exists();

        // Start with any plain diff copy length.
        let mut best = diff_len.unwrap_or(0u64);
        debug!(path = %rel.display(), diff_len = best, "logical_len_diff");

        // Delta patch/full lengths: use bitmap to find highest slot, and full
        // file payload size as a fallback.
        if patch_exists {
            let cached = self.inner.delta_index.is_cached(&patch_path);
            let bitmap = self.inner.delta_index.get_or_load_bitmap(&patch_path)?;
            if !cached {
                self.inner
                    .metrics
                    .delta_bitmaps_loaded
                    .fetch_add(1, Ordering::Relaxed);
                self.inner
                    .metrics
                    .delta_bitmaps_total_bytes
                    .fetch_add(bitmap.len_bytes() as u64, Ordering::Relaxed);
            }

            if let Some(max_blk) = bitmap.max_set_block() {
                best = best.max(self.block_offset(max_blk + 1));
                debug!(
                    path = %rel.display(),
                    max_blk,
                    best,
                    "logical_len_delta_bitmap"
                );
            }
        }

        if full_exists {
            if let Ok(meta) = fs::metadata(&full_path) {
                if meta.len() >= FULL_HEADER_SIZE {
                    let payload_bytes = meta.len() - FULL_HEADER_SIZE;
                    if payload_bytes > 0 {
                        let blocks = (payload_bytes + self.inner.block_size.get() as u64 - 1)
                            / self.inner.block_size.get() as u64;
                        best = best.max(self.block_offset(blocks));
                        debug!(
                            path = %rel.display(),
                            payload_bytes,
                            blocks,
                            best,
                            "logical_len_full_file"
                        );
                    }
                }
            }
        }

        // Respect any already materialized blocks tracked in the cache, which
        // can reflect truncation/extension before a diff copy exists on disk.
        if let Ok(cache) = self.inner.cache.lock() {
            if let Some(entry) = cache.get(rel) {
                if let Some(max_blk) = entry.materialized.iter().max() {
                    let inferred = self.block_offset(max_blk + 1);
                    best = best.max(inferred);
                    debug!(
                        path = %rel.display(),
                        max_blk,
                        inferred,
                        best,
                        "logical_len_cache_materialized"
                    );
                }
                if let Some(end) = entry.max_written_end {
                    best = best.max(end);
                    debug!(
                        path = %rel.display(),
                        max_written_end = end,
                        best,
                        "logical_len_cache_written_end"
                    );
                }
            }
        }

        let matches = self.matching_layers(rel);
        if matches.is_empty() {
            return Ok(if best == 0 { None } else { Some(best) });
        }

        for (idx, path) in matches {
            let layer = &self.inner.layers[idx];

            // For per-page stored files (all pg_probackup main-fork datafiles),
            // inspect BackupPageHeader stream to find highest block.
            let stream_len = match self.pg_block_index(rel, &path) {
                Ok(index) if !index.is_empty() => {
                    index.last().map(|e| self.block_offset(e.block as u64 + 1))
                }
                _ => None,
            };

            // Use pg_probackup metadata (n_blocks/full_size) when available to
            // avoid truncating to the limited set of changed pages present in
            // an incremental page stream.
            let meta_len = self
                .file_meta_len(layer, rel)
                .map(|len| self.block_offset(len));

            let logical = match (stream_len, meta_len) {
                (Some(s), Some(m)) => Some(s.max(m)),
                (Some(s), None) => Some(s),
                (None, Some(m)) => Some(m),
                (None, None) => fs::metadata(&path).ok().map(|m| m.len()),
            };

            debug!(
                path = %rel.display(),
                layer = idx,
                stream_len = stream_len.unwrap_or(0),
                meta_len = meta_len.unwrap_or(0),
                logical = logical.unwrap_or(0),
                incremental = layer.incremental,
                "logical_len_layer"
            );

            if let Some(len) = logical {
                if let (Some(m), Some(s)) = (meta_len, stream_len) {
                    if m > s {
                        debug!(
                            path = %rel.display(),
                            layer = idx,
                            meta_blocks = m / self.inner.block_size.get() as u64,
                            stream_blocks = s / self.inner.block_size.get() as u64,
                            "datafile_logical_len_from_metadata"
                        );
                    }
                }

                best = best.max(len);
                // If we hit a full (non-incremental) layer, that's the base;
                // later incrementals cannot extend logical length beyond it.
                if !layer.incremental {
                    break;
                }
            }
        }

        if best == 0 {
            debug!(path = %rel.display(), best, "logical_len_result_none");
            Ok(None)
        } else {
            debug!(path = %rel.display(), best, "logical_len_result");
            Ok(Some(best))
        }
    }

    /// Identify PostgreSQL "status" files where PostgreSQL tracks transaction
    /// state and similar metadata (pg_xact, pg_multixact, pg_subtrans,
    /// pg_commit_ts). For these files pg_probackup restore effectively uses the
    /// version from the destination backup itself; combining lengths from older
    /// backups can extend them beyond what exists in the target backup.
    fn is_system_status_file(&self, rel: &Path) -> bool {
        let mut components = rel.components();
        let first = match components.next() {
            Some(c) => c.as_os_str().to_string_lossy(),
            None => return false,
        };
        matches!(
            first.as_ref(),
            "pg_xact" | "pg_multixact" | "pg_subtrans" | "pg_commit_ts"
        )
    }

    fn materialize_block(&self, rel: &Path, block_idx: u64) -> Result<()> {
        // Check cache first to avoid rescans.
        if let Ok(mut cache) = self.inner.cache.lock() {
            let entry = cache.entry(rel.to_path_buf()).or_default();
            if entry.materialized.contains(&block_idx) {
                // Guard against stale cache after external truncation or
                // unexpected diff cleanup: verify the diff file is large
                // enough to actually contain this block.
                let expected_len = self.block_offset(block_idx + 1);
                let diff_len = fs::metadata(self.inner.diff.join(rel))
                    .map(|m| m.len())
                    .unwrap_or(0);

                if diff_len >= expected_len {
                    self.inner
                        .metrics
                        .cache_hits
                        .fetch_add(1, Ordering::Relaxed);
                    debug!(
                        path = %rel.display(),
                        block = block_idx,
                        diff_len,
                        "block_cache_hit_materialized",
                    );
                    return Ok(());
                }

                // Cached as materialized but backing file is too short.
                // Drop the cached entry so we re-copy the block below.
                entry.materialized.remove(&block_idx);
                entry.sources.remove(&block_idx);
                debug!(
                    path = %rel.display(),
                    block = block_idx,
                    diff_len,
                    expected_len,
                    "block_cache_stale_truncated",
                );
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
            None => {
                if self.is_pg_datafile(rel) {
                    // No layer provides this block; treat as zero page to avoid EIO.
                    let zero = vec![0u8; self.inner.block_size.get()];
                    let diff_path = self.inner.diff.join(rel);
                    if let Some(parent) = diff_path.parent() {
                        fs::create_dir_all(parent)?;
                    }
                    let file = fs::OpenOptions::new()
                        .create(true)
                        .read(true)
                        .write(true)
                        .truncate(false)
                        .open(&diff_path)?;
                    file.write_all_at(&zero, self.block_offset(block_idx))?;
                    if let Ok(mut cache) = self.inner.cache.lock() {
                        let entry = cache.entry(rel.to_path_buf()).or_default();
                        entry.materialized.insert(block_idx);
                    }
                    return Ok(());
                }
                return Err(io::Error::from(io::ErrorKind::NotFound).into());
            }
        };
        let mut source = source;

        loop {
            if let Ok(mut cache) = self.inner.cache.lock() {
                let entry = cache.entry(rel.to_path_buf()).or_default();
                entry.sources.insert(block_idx, source);
            }

            match self.copy_block(rel, block_idx, source) {
                Ok(()) => return Ok(()),
                Err(err) if self.should_retry_missing_block(rel, source, &err) => {
                    debug!(
                        path = %rel.display(),
                        block = block_idx,
                        source = ?source,
                        error = ?err,
                        "block_copy_missing_retry"
                    );
                    if let Some(next) = self.fallback_block_source(rel, block_idx, source)? {
                        if let Ok(mut cache) = self.inner.cache.lock() {
                            if let Some(entry) = cache.get_mut(rel) {
                                entry.sources.remove(&block_idx);
                            }
                        }
                        source = next;
                        continue;
                    }
                    // No fallback available; for datafiles, materialize zeroes to avoid EIO.
                    if self.is_pg_datafile(rel) {
                        let zero = vec![0u8; self.inner.block_size.get()];
                        let diff_path = self.inner.diff.join(rel);
                        if let Some(parent) = diff_path.parent() {
                            fs::create_dir_all(parent)?;
                        }
                        let file = fs::OpenOptions::new()
                            .create(true)
                            .read(true)
                            .write(true)
                            .truncate(false)
                            .open(&diff_path)?;
                        file.write_all_at(&zero, self.block_offset(block_idx))?;
                        if let Ok(mut cache) = self.inner.cache.lock() {
                            let entry = cache.entry(rel.to_path_buf()).or_default();
                            entry.materialized.insert(block_idx);
                        }
                        return Ok(());
                    }
                    return Err(err);
                }
                Err(err) => return Err(err),
            }
        }
    }

    fn read_block_data(&self, rel: &Path, block_idx: u64) -> Result<Vec<u8>> {
        // Delta-based diff for PostgreSQL datafiles.
        if self.is_pg_datafile(rel) {
            if let Some(delta) = self.read_datafile_delta(rel, block_idx)? {
                return Ok(delta);
            }
        }

        // Prefer diff if already materialized.
        let diff_path = self.inner.diff.join(rel);
        if diff_path.exists() {
            if self.is_pg_datafile(rel) {
                if let Ok(meta) = fs::metadata(&diff_path) {
                    let block_sz = self.inner.block_size.get() as u64;
                    // Shadowed zero-length relation created via truncate_pg_datafile(size == 0):
                    // diff file exists with a single zero block and no delta artifacts.
                    let (patch_path, full_path) = self.delta_paths(rel);
                    if meta.len() == block_sz && !patch_path.exists() && !full_path.exists() {
                        return Ok(vec![0u8; self.inner.block_size.get()]);
                    }
                    if meta.len() == 0 {
                        return Ok(vec![0u8; self.inner.block_size.get()]);
                    }
                }
            }
            let file = fs::OpenOptions::new().read(true).open(&diff_path)?;
            let offset = self.block_offset(block_idx);

            let is_hole = if self.is_pg_datafile(rel) {
                match self.is_sparse_hole(&file, offset) {
                    Ok(v) => v,
                    Err(e) => {
                        debug!(
                            path = %rel.display(),
                            block = block_idx,
                            error = ?e,
                            "sparse_probe_failed"
                        );
                        false
                    }
                }
            } else {
                false
            };

            if !is_hole {
                let mut buf = vec![0u8; self.inner.block_size.get()];
                let read = file.read_at(&mut buf, offset)?;
                if read == 0 {
                    return Ok(vec![0u8; self.inner.block_size.get()]);
                }
                if read < buf.len() {
                    for b in buf.iter_mut().skip(read) {
                        *b = 0;
                    }
                }
                return Ok(buf);
            }
        }

        // Cache lookup for known sources.
        let mut source: Option<BlockSource> = None;
        if let Ok(mut cache) = self.inner.cache.lock() {
            if let Some(entry) = cache.get_mut(rel) {
                if let Some(src) = entry.sources.get(&block_idx).copied() {
                    self.inner
                        .metrics
                        .cache_hits
                        .fetch_add(1, Ordering::Relaxed);
                    source = Some(src);
                } else {
                    self.inner
                        .metrics
                        .cache_misses
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        if source.is_none() {
            source = self.locate_block_source(rel, block_idx)?;
        }

        let mut source = match source {
            Some(s) => s,
            None => {
                if self.is_pg_datafile(rel) {
                    return Ok(vec![0u8; self.inner.block_size.get()]);
                }
                return Err(io::Error::from(io::ErrorKind::NotFound).into());
            }
        };

        loop {
            match self.read_block_from_source(rel, block_idx, source) {
                Ok(data) => {
                    if let Ok(mut cache) = self.inner.cache.lock() {
                        cache
                            .entry(rel.to_path_buf())
                            .or_default()
                            .sources
                            .insert(block_idx, source);
                    }
                    return Ok(data);
                }
                Err(err) if self.should_retry_missing_block(rel, source, &err) => {
                    debug!(
                        path = %rel.display(),
                        block = block_idx,
                        source = ?source,
                        error = ?err,
                        "block_read_missing_retry"
                    );
                    if let Some(next_source) = self.fallback_block_source(rel, block_idx, source)? {
                        if let Ok(mut cache) = self.inner.cache.lock() {
                            if let Some(entry) = cache.get_mut(rel) {
                                entry.sources.remove(&block_idx);
                            }
                        }
                        source = next_source;
                        continue;
                    }
                    return Err(err);
                }
                Err(err) => {
                    debug!(
                        path = %rel.display(),
                        block = block_idx,
                        source = ?source,
                        error = ?err,
                        "block_read_error"
                    );
                    return Err(err);
                }
            }
        }
    }

    fn read_block_from_source(
        &self,
        rel: &Path,
        block_idx: u64,
        source: BlockSource,
    ) -> Result<Vec<u8>> {
        let block_size = self.inner.block_size.get();

        match source {
            BlockSource::Diff => {
                let mut buf = vec![0u8; block_size];
                let diff_path = self.inner.diff.join(rel);
                let read =
                    fs::File::open(&diff_path)?.read_at(&mut buf, self.block_offset(block_idx))?;
                if read < buf.len() {
                    for b in buf.iter_mut().skip(read) {
                        *b = 0;
                    }
                }
                Ok(buf)
            }
            BlockSource::Zero => Ok(vec![0u8; block_size]),
            BlockSource::Layer { idx } => {
                let layer = &self.inner.layers[idx];
                let path = layer.root.join(rel);
                let file_algo = self.file_compression_for_layer(idx, rel);
                let data = if layer.incremental {
                    let has_pagemap = path.with_extension("pagemap").exists();
                    if has_pagemap || self.is_pg_datafile(rel) {
                        self.read_pg_data_block(rel, &path, file_algo, block_idx)?
                    } else {
                        self.read_full_block(rel, &path, layer, file_algo, block_idx)?
                    }
                } else {
                    self.read_full_block(rel, &path, layer, file_algo, block_idx)?
                };

                if let Some(d) = data {
                    Ok(d)
                } else if self.is_pg_datafile(rel) {
                    if let Some(next) = self.locate_block_source_from(rel, block_idx, idx + 1)? {
                        return self.read_block_from_source(rel, block_idx, next);
                    }
                    Ok(vec![0u8; self.inner.block_size.get()])
                } else {
                    Err(io::Error::from(io::ErrorKind::NotFound).into())
                }
            }
        }
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
        let cache = self.inner.cache.lock().unwrap();
        if let Some(entry) = cache.get(rel) {
            if entry.materialized.contains(&block_idx) {
                let src = BlockSource::Diff;
                debug!(path = %rel.display(), block = block_idx, source = ?src, "block_source_cache_materialized");
                return Ok(Some(src));
            }
        }
        drop(cache);

        for (idx, layer) in self.inner.layers.iter().enumerate().skip(start_layer) {
            let path = layer.root.join(rel);
            if !path.exists() {
                debug!(path = %rel.display(), layer = idx, candidate = %path.display(), "block_source_layer_missing");
                continue;
            }
            if layer.incremental {
                if let Some(pagemap) = self.load_pagemap(&path)? {
                    if !pagemap.contains(&(block_idx as u32)) {
                        debug!(path = %rel.display(), block = block_idx, layer = idx, "block_source_layer_pagemap_skip");
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

        debug!(path = %rel.display(), block = block_idx, start_layer, "block_source_not_found");
        Ok(None)
    }

    fn should_retry_missing_block(
        &self,
        rel: &Path,
        source: BlockSource,
        err: &anyhow::Error,
    ) -> bool {
        let not_found = err
            .downcast_ref::<io::Error>()
            .map(|e| e.kind() == io::ErrorKind::NotFound)
            .unwrap_or(false);
        if !not_found {
            return false;
        }

        match source {
            BlockSource::Layer { idx } => {
                // For PostgreSQL datafiles in incremental layers, missing blocks
                // mean "unchanged". Always retry against older layers instead of
                // surfacing EIO.
                let layer = &self.inner.layers[idx];
                self.is_pg_datafile(rel) && layer.incremental
            }
            _ => false,
        }
    }

    fn fallback_block_source(
        &self,
        rel: &Path,
        block_idx: u64,
        source: BlockSource,
    ) -> Result<Option<BlockSource>> {
        match source {
            BlockSource::Layer { idx } => self.locate_block_source_from(rel, block_idx, idx + 1),
            _ => Ok(None),
        }
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
                let file_algo = self.file_compression_for_layer(idx, rel);
                let mut data = if layer.incremental {
                    let has_pagemap = path.with_extension("pagemap").exists();
                    if has_pagemap || self.is_pg_datafile(rel) {
                        // Incremental page-mode files use per-page BackupPageHeader records.
                        self.read_pg_data_block(rel, &path, file_algo, block_idx)?
                    } else {
                        // Non-page incremental entries are stored as whole files.
                        self.read_full_block(rel, &path, layer, file_algo, block_idx)?
                    }
                } else {
                    self.read_full_block(rel, &path, layer, file_algo, block_idx)?
                };

                if data.is_none() {
                    if let Some(next) = self.locate_block_source_from(rel, block_idx, idx + 1)? {
                        if let Ok(mut cache) = self.inner.cache.lock() {
                            let entry = cache.entry(rel.to_path_buf()).or_default();
                            entry.sources.insert(block_idx, next);
                        }
                        return self.copy_block(rel, block_idx, next);
                    } else if self.is_pg_datafile(rel) {
                        data = Some(vec![0u8; self.inner.block_size.get()]);
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

            // Compute fresh logical length to avoid stale sizes.
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

    fn read_full_block(
        &self,
        rel: &Path,
        base_path: &Path,
        _layer: &Layer,
        compression: Option<CompressionAlgorithm>,
        block_idx: u64,
    ) -> Result<Option<Vec<u8>>> {
        // For WAL files under --no-wal, avoid copy-up and read directly from compressed source.
        if self.is_pg_wal_child(rel) {
            if let Some(algo) = compression {
                if self.inner.no_wal {
                    let decompressed = self.decompress_full_to_vec(base_path, algo)?;
                    let block_size = self.inner.block_size.get();
                    let start = self.block_offset(block_idx) as usize;
                    if start >= decompressed.len() {
                        return Ok(None);
                    }
                    let end = (start + block_size).min(decompressed.len());
                    let mut buf = vec![0u8; block_size];
                    buf[..end - start].copy_from_slice(&decompressed[start..end]);
                    return Ok(Some(buf));
                } else {
                    self.inner
                        .metrics
                        .fallback_used
                        .fetch_add(1, Ordering::Relaxed);
                    debug!(
                        path = %rel.display(),
                        block = block_idx,
                        algo = ?compression,
                        "wal_fallback_full_copy"
                    );
                    self.ensure_copy_up(rel)?;
                    return Ok(None);
                }
            }
        }

        if self.is_pg_datafile(rel) {
            match self.read_pg_data_block(rel, base_path, compression, block_idx) {
                Ok(Some(page)) => return Ok(Some(page)),
                Ok(None) => return Ok(None),
                Err(err) => {
                    let format_err = err
                        .downcast_ref::<Error>()
                        .map(|e| matches!(e, Error::InvalidIncrementalPageSize { .. }))
                        .unwrap_or(false)
                        || err
                            .downcast_ref::<io::Error>()
                            .map(|e| {
                                matches!(
                                    e.kind(),
                                    io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
                                )
                            })
                            .unwrap_or(false);

                    if !format_err {
                        return Err(err);
                    }

                    debug!(
                        path = %rel.display(),
                        block = block_idx,
                        "pg_page_stream_fallback_plain"
                    );
                }
            }

            return self.read_plain_block(base_path, block_idx);
        }

        // Compressed non-data files (both FULL and incremental) are stored as
        // whole files, not per-page streams. Materialize a single decompressed
        // copy in the diff directory from the newest matching layer and serve
        // reads from there instead of falling back to older layers.
        if compression.is_some() {
            self.inner
                .metrics
                .fallback_used
                .fetch_add(1, Ordering::Relaxed);
            debug!(
                path = %rel.display(),
                block = block_idx,
                algo = ?compression,
                "block_fallback_full_copy"
            );

            self.ensure_copy_up(rel)?;

            let diff_path = self.inner.diff.join(rel);
            if !diff_path.exists() {
                return Ok(None);
            }

            let mut buf = vec![0u8; self.inner.block_size.get()];
            let file = fs::File::open(&diff_path)?;
            let read = file.read_at(&mut buf, self.block_offset(block_idx))?;
            if read == 0 {
                return Ok(None);
            }
            buf.truncate(read);
            return Ok(Some(buf));
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

    fn is_sparse_hole(&self, file: &fs::File, offset: u64) -> io::Result<bool> {
        #[cfg(target_os = "linux")]
        {
            // SEEK_DATA returns the next data offset >= current. If it is
            // beyond the requested offset (or ENXIO), the region is a hole.
            let res = unsafe { libc::lseek64(file.as_raw_fd(), offset as i64, libc::SEEK_DATA) };
            if res == -1 {
                let err = io::Error::last_os_error();
                return match err.raw_os_error() {
                    Some(libc::ENXIO) => Ok(true),
                    Some(libc::ENOSYS) | Some(libc::EINVAL) => Ok(false),
                    _ => Err(err),
                };
            }
            Ok(res as u64 > offset)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (file, offset);
            Ok(false)
        }
    }

    fn read_pg_data_block(
        &self,
        rel: &Path,
        src: &Path,
        compression: Option<CompressionAlgorithm>,
        block_idx: u64,
    ) -> Result<Option<Vec<u8>>> {
        // Build or reuse block index to avoid rescanning the whole file.
        let index = self.pg_block_index(rel, src)?;

        // Block numbers are stored as u32 in the pg_probackup page stream
        // header. Reject obviously out-of-range requests early.
        if block_idx > u32::MAX as u64 {
            return Ok(None);
        }

        // Index is kept sorted by block, so use binary search instead of
        // scanning the whole slice on every page read.
        let target = match index
            .binary_search_by_key(&(block_idx as u32), |e| e.block)
            .ok()
            .map(|i| &index[i])
        {
            Some(v) => v,
            None => return Ok(None),
        };

        let mut reader = fs::File::open(src)?;
        reader.seek(SeekFrom::Start(target.offset))?;
        let mut buf = vec![0u8; target.compressed_size as usize];
        reader.read_exact(&mut buf)?;

        let page = self.decompress_page_if_needed(compression, target.compressed_size, buf)?;
        if page.len() != self.inner.block_size.get() {
            return Err(Error::InvalidIncrementalPageSize {
                path: rel.display().to_string(),
                block: target.block,
                expected: self.inner.block_size.get(),
                actual: page.len(),
            }
            .into());
        }
        Ok(Some(page))
    }

    fn read_plain_block(&self, path: &Path, block_idx: u64) -> Result<Option<Vec<u8>>> {
        let mut buf = vec![0u8; self.inner.block_size.get()];
        let file = fs::File::open(path)?;
        let read = file.read_at(&mut buf, self.block_offset(block_idx))?;
        if read == 0 {
            return Ok(None);
        }
        if read < buf.len() {
            for b in buf.iter_mut().skip(read) {
                *b = 0;
            }
        }
        Ok(Some(buf))
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

    /// Partial write helper used by FUSE to avoid re-reading entire files.
    /// Ensures the diff copy exists, then writes the provided slice at
    /// `offset` without truncating the file.
    pub fn write_at(&self, relative: impl AsRef<Path>, offset: u64, data: &[u8]) -> Result<()> {
        let rel = relative.as_ref();
        let block_size = self.inner.block_size.get() as u64;

        if self.is_pg_datafile(rel)
            && offset % block_size == 0
            && (data.len() as u64) % block_size == 0
        {
            return self.write_datafile_delta(rel, offset, data);
        }

        self.ensure_copy_up(rel)?;

        let path = self.inner.diff.join(rel);
        let file = fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;

        file.write_all_at(data, offset)?;

        let needed_len = offset.saturating_add(data.len() as u64);
        if file.metadata()?.len() < needed_len {
            file.set_len(needed_len)?;
        }

        self.mark_dirty_path(path);
        self.record_write(rel, offset, data.len());
        Ok(())
    }

    fn write_datafile_delta(&self, rel: &Path, offset: u64, data: &[u8]) -> Result<()> {
        let (patch_path, full_path) = self.delta_paths(rel);
        if let Some(parent) = patch_path.parent() {
            fs::create_dir_all(parent)?;
        }
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)?;
        }

        let block_size = self.inner.block_size.get() as u64;
        let file_lock = self.file_lock(rel);
        let _file_guard = file_lock.read();
        let patch_file = open_patch(&patch_path)?;
        let full_file = open_full(&full_path)?;
        let bitmap = self.inner.delta_index.get_or_create_empty(&patch_path);

        for (i, chunk) in data.chunks(self.inner.block_size.get()).enumerate() {
            let block_no = offset / block_size + i as u64;
            let block_lock = self.block_lock(rel, block_no);
            {
                let _guard = block_lock.write();

                let base = self.read_base_block(rel, block_no)?;
                let mut new_page = base;
                new_page[..chunk.len()].copy_from_slice(chunk);

                let delta = compute_delta(&base, &new_page);
                let old_kind = bitmap.get(block_no);
                match delta {
                    DeltaDiff::Empty => {
                        if old_kind != 0 {
                            write_empty_slot(&patch_file, block_no)?;
                            self.mark_dirty_path(patch_path.clone());
                            if old_kind == 0b01 {
                                self.inner
                                    .metrics
                                    .delta_punch_holes
                                    .fetch_add(1, Ordering::Relaxed);
                                if let Err(err) = punch_full_hole(&full_file, block_no) {
                                    self.inner
                                        .metrics
                                        .delta_punch_hole_failures
                                        .fetch_add(1, Ordering::Relaxed);
                                    warn!(
                                        path = %rel.display(),
                                        block = block_no,
                                        error = ?err,
                                        "delta_punch_hole_failed"
                                    );
                                }
                                self.mark_dirty_path(full_path.clone());
                            }
                            bitmap.set(block_no, 0);
                        }
                    }
                    DeltaDiff::PatchV2 { .. } => {
                        let payload = delta.to_patch_bytes().unwrap();
                        write_patch_slot(&patch_file, block_no, &payload)?;
                        self.mark_dirty_path(patch_path.clone());
                        if old_kind == 0b01 {
                            self.inner
                                .metrics
                                .delta_punch_holes
                                .fetch_add(1, Ordering::Relaxed);
                            let punch_res = punch_full_hole(&full_file, block_no);
                            if let Err(err) = &punch_res {
                                self.inner
                                    .metrics
                                    .delta_punch_hole_failures
                                    .fetch_add(1, Ordering::Relaxed);
                                warn!(
                                    path = %rel.display(),
                                    block = block_no,
                                    error = ?err,
                                    "delta_punch_hole_failed"
                                );
                            }
                            // If punch succeeded, try to shrink tail if this was the highest block.
                            if punch_res.is_ok() {
                                if let Err(err) = shrink_full_tail(&full_file, block_no) {
                                    warn!(
                                        path = %rel.display(),
                                        block = block_no,
                                        error = ?err,
                                        "delta_full_shrink_failed"
                                    );
                                }
                            }
                            self.mark_dirty_path(full_path.clone());
                        }
                        bitmap.set(block_no, 0b10);
                        self.inner
                            .metrics
                            .delta_patch_count
                            .fetch_add(1, Ordering::Relaxed);
                        self.inner
                            .metrics
                            .delta_patch_bytes
                            .fetch_add(payload.len() as u64, Ordering::Relaxed);
                        self.inner
                            .metrics
                            .delta_patch_max_size
                            .fetch_max(payload.len() as u64, Ordering::Relaxed);
                    }
                    DeltaDiff::Full(page) => {
                        write_full_page(&full_file, block_no, page.as_ref())?;
                        write_full_ref_slot(&patch_file, block_no)?;
                        self.mark_dirty_path(full_path.clone());
                        self.mark_dirty_path(patch_path.clone());
                        bitmap.set(block_no, 0b01);
                        self.inner
                            .metrics
                            .delta_full_count
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            self.cleanup_block_lock(rel, block_no, &block_lock);

            // Avoid treating datafile blocks as fully materialized diff entries.
            self.record_write(rel, block_no * block_size, self.inner.block_size.get());
        }

        self.cleanup_file_lock(rel, &file_lock);

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

    pub fn reset_metrics(&self) {
        self.inner.metrics.reset();
    }

    pub fn perf_unsafe(&self) -> bool {
        self.inner.perf_unsafe
    }

    pub fn no_wal(&self) -> bool {
        self.inner.no_wal
    }

    fn is_pg_wal_child(&self, rel: &Path) -> bool {
        rel.parent() == Some(Path::new("pg_wal"))
    }

    pub fn dirty_marker_path(&self) -> PathBuf {
        self.inner.dirty_marker.clone()
    }

    fn mark_dirty_path(&self, path: PathBuf) {
        if !self.inner.perf_unsafe {
            return;
        }
        if let Ok(mut guard) = self.inner.dirty_paths.lock() {
            guard.insert(path);
        }
    }

    fn mark_dirty_for_rel(&self, rel: &Path) {
        if !self.inner.perf_unsafe {
            return;
        }
        // Always track the diff path for the relation.
        self.mark_dirty_path(self.inner.diff.join(rel));
        if self.is_pg_datafile(rel) {
            let (patch, full) = self.delta_paths(rel);
            self.mark_dirty_path(patch);
            self.mark_dirty_path(full);
        }
    }

    fn clear_dirty_for_rel(&self, rel: &Path) {
        if let Ok(mut guard) = self.inner.dirty_paths.lock() {
            guard.remove(&self.inner.diff.join(rel));
            let (patch, full) = self.delta_paths(rel);
            guard.remove(&patch);
            guard.remove(&full);
        }
    }

    fn rename_dirty_entries(&self, src: &Path, dst: &Path) {
        if let Ok(mut guard) = self.inner.dirty_paths.lock() {
            let src_diff = self.inner.diff.join(src);
            let dst_diff = self.inner.diff.join(dst);
            if guard.remove(&src_diff) {
                guard.insert(dst_diff);
            }

            let (src_patch, src_full) = self.delta_paths(src);
            let (dst_patch, dst_full) = self.delta_paths(dst);
            if guard.remove(&src_patch) {
                guard.insert(dst_patch);
            }
            if guard.remove(&src_full) {
                guard.insert(dst_full);
            }
        }
    }

    pub fn fsync_path(&self, rel: &Path, datasync: bool) -> Result<()> {
        if self.inner.perf_unsafe {
            self.mark_dirty_for_rel(rel);
            return Ok(());
        }

        if self.is_pg_datafile(rel) {
            return self.fsync_pg_datafile(rel, datasync);
        }

        let path = {
            let diff_path = self.inner.diff.join(rel);
            if diff_path.exists() {
                Some(diff_path)
            } else {
                self.find_layer_path(rel).map(|(p, _, _)| p)
            }
        };

        let Some(target) = path else {
            return Err(Error::NotMounted(rel.display().to_string()).into());
        };

        let file = fs::File::open(&target)?;
        if datasync {
            file.sync_data()?;
        } else {
            file.sync_all()?;
        }
        Ok(())
    }

    fn fsync_pg_datafile(&self, rel: &Path, datasync: bool) -> Result<()> {
        let (patch_path, full_path) = self.delta_paths(rel);
        let mut synced = false;

        if patch_path.exists() {
            let f = fs::File::open(&patch_path)?;
            if datasync {
                f.sync_data()?;
            } else {
                f.sync_all()?;
            }
            synced = true;
        }

        if full_path.exists() {
            let f = fs::File::open(&full_path)?;
            if datasync {
                f.sync_data()?;
            } else {
                f.sync_all()?;
            }
            synced = true;
        }

        let diff_path = self.inner.diff.join(rel);
        if diff_path.exists() {
            let f = fs::File::open(&diff_path)?;
            if datasync {
                f.sync_data()?;
            } else {
                f.sync_all()?;
            }
            synced = true;
        }

        if !synced {
            // Nothing to sync: datafile untouched.
        }
        Ok(())
    }

    /// Flush all dirty paths recorded in perf-unsafe mode and remove the
    /// `.pbkfs-dirty` marker. No-op when the mode is disabled.
    pub fn flush_dirty(&self, datasync: bool) -> Result<()> {
        if !self.inner.perf_unsafe {
            return Ok(());
        }

        let paths: Vec<PathBuf> = {
            let mut guard = self.inner.dirty_paths.lock().unwrap();
            guard.drain().collect()
        };

        for path in paths {
            if !path.exists() {
                continue;
            }
            let file = fs::File::open(&path)?;
            if datasync {
                file.sync_data()?;
            } else {
                file.sync_all()?;
            }
        }

        if self.inner.dirty_marker.exists() {
            let _ = fs::remove_file(&self.inner.dirty_marker);
        }

        Ok(())
    }

    /// Truncate PostgreSQL datafile delta artifacts to `size` bytes.
    /// - size == 0: removes `.patch`/`.full` and diff copy, clears bitmap cache.
    /// - shrink: clears slots >= new length and punches corresponding full pages.
    /// - extend: leaves delta sparse; future reads will serve zeros/base pages.
    pub fn truncate_pg_datafile(&self, rel: &Path, size: u64) -> Result<()> {
        let block_size = self.inner.block_size.get() as u64;
        let (patch_path, full_path) = self.delta_paths(rel);
        let diff_path = self.inner.diff.join(rel);

        let file_lock = self.file_lock(rel);
        let _file_guard = file_lock.write();
        self.mark_dirty_for_rel(rel);

        if let Some(parent) = diff_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let diff_file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&diff_path)?;
        diff_file.set_len(size)?;

        if size == 0 {
            // Shadow base relation with an empty diff file so subsequent reads
            // see a zero-length relation instead of the base backup contents.
            if !diff_path.exists() {
                let _ = fs::OpenOptions::new()
                    .create(true)
                    .truncate(true)
                    .write(true)
                    .open(&diff_path);
            }
            let _ = fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(&diff_path);
            let _ = diff_file.set_len(block_size); // keep first block addressable as zeros
            let _ = fs::remove_file(&patch_path);
            let _ = fs::remove_file(&full_path);
            self.inner.delta_index.invalidate(&patch_path);
            self.clear_block_locks_for(rel);
            self.clear_dirty_for_rel(rel);
            self.cleanup_file_lock(rel, &file_lock);
            return Ok(());
        }

        let new_blocks = (size + block_size - 1) / block_size;
        const FULL_HDR: u64 = 4096;

        let bitmap = self.inner.delta_index.get_or_create_empty(&patch_path);

        if patch_path.exists() {
            let patch_file = open_patch(&patch_path)?;
            let meta = patch_file.metadata()?;
            let total_slots = meta
                .len()
                .saturating_sub(crate::fs::delta::PATCH_SLOT_SIZE as u64)
                / crate::fs::delta::PATCH_SLOT_SIZE as u64;
            for blk in new_blocks..=total_slots {
                let _ = write_empty_slot(&patch_file, blk);
                bitmap.set(blk, 0);
            }
        }

        if full_path.exists() {
            let full_file = open_full(&full_path)?;
            let meta = full_file.metadata()?;
            let total_blocks = meta.len().saturating_sub(FULL_HDR) / PAGE_SIZE as u64;
            for blk in new_blocks..=total_blocks {
                let _ = punch_full_hole(&full_file, blk);
                bitmap.set(blk, 0);
            }
            let expected_len = FULL_HDR + new_blocks * PAGE_SIZE as u64;
            let _ = full_file.set_len(expected_len);
        }

        if let Some(parent) = patch_path.parent() {
            if !patch_path.exists() && !full_path.exists() {
                // Ensure directory exists when we had no prior delta; extension path.
                fs::create_dir_all(parent)?;
            }
        }

        self.inner.delta_index.invalidate(&patch_path);
        // Truncation changes logical length; drop any cached value so it will
        // be recomputed on next access.
        if let Ok(mut cache) = self.inner.cache.lock() {
            if let Some(entry) = cache.get_mut(rel) {
                entry.logical_len = None;
            }
        }
        self.clear_block_locks_for(rel);
        self.cleanup_file_lock(rel, &file_lock);
        Ok(())
    }

    /// Rename PostgreSQL datafile, moving associated delta artifacts and locks.
    pub fn rename_pg_datafile(&self, src: &Path, dst: &Path) -> Result<()> {
        if src == dst {
            return Ok(());
        }

        let (first, second) = if src < dst { (src, dst) } else { (dst, src) };
        let lock_first = self.file_lock(first);
        let lock_second = self.file_lock(second);
        let _g1 = lock_first.write();
        let _g2 = lock_second.write();

        let (src_patch, src_full) = self.delta_paths(src);
        let (dst_patch, dst_full) = self.delta_paths(dst);
        let src_diff = self.inner.diff.join(src);
        let dst_diff = self.inner.diff.join(dst);

        if let Some(parent) = dst_diff.parent() {
            fs::create_dir_all(parent)?;
        }
        if let Some(parent) = dst_patch.parent() {
            fs::create_dir_all(parent)?;
        }

        // Remove existing destination artifacts to allow rename overwrite.
        let _ = fs::remove_file(&dst_patch);
        let _ = fs::remove_file(&dst_full);
        if dst_diff.exists() {
            let meta = fs::symlink_metadata(&dst_diff)?;
            if meta.is_dir() {
                fs::remove_dir_all(&dst_diff)?;
            } else {
                fs::remove_file(&dst_diff)?;
            }
        }

        if src_diff.exists() {
            fs::rename(&src_diff, &dst_diff)?;
        }
        if src_patch.exists() {
            fs::rename(&src_patch, &dst_patch)?;
        }
        if src_full.exists() {
            fs::rename(&src_full, &dst_full)?;
        }

        self.inner.delta_index.invalidate(&src_patch);
        self.inner.delta_index.invalidate(&dst_patch);
        self.clear_block_locks_for(dst);
        self.move_block_locks(src, dst);
        self.move_file_lock(src, dst);
        self.rename_dirty_entries(src, dst);
        self.cleanup_file_lock(src, &lock_first);
        self.cleanup_file_lock(dst, &lock_second);

        Ok(())
    }

    /// Remove PostgreSQL datafile and associated delta artifacts.
    pub fn unlink_pg_datafile(&self, rel: &Path) -> Result<()> {
        let (patch_path, full_path) = self.delta_paths(rel);
        let diff_path = self.inner.diff.join(rel);

        let file_lock = self.file_lock(rel);
        let _guard = file_lock.write();

        if diff_path.exists() {
            fs::remove_file(&diff_path)?;
        }
        if patch_path.exists() {
            fs::remove_file(&patch_path)?;
        }
        if full_path.exists() {
            fs::remove_file(&full_path)?;
        }

        self.inner.delta_index.invalidate(&patch_path);
        self.clear_block_locks_for(rel);
        self.clear_dirty_for_rel(rel);
        self.cleanup_file_lock(rel, &file_lock);
        Ok(())
    }

    /// Debug helper to inspect cached overlay entries.
    pub fn debug_cache_keys(&self) -> Vec<PathBuf> {
        self.inner
            .cache
            .lock()
            .map(|c| c.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub fn compression_algorithm(&self) -> Option<CompressionAlgorithm> {
        self.inner.layers.iter().find_map(|l| l.compression)
    }

    /// Read datafile block using delta storage if present.
    fn read_datafile_delta(&self, rel: &Path, block_idx: u64) -> Result<Option<Vec<u8>>> {
        let (patch_path, full_path) = self.delta_paths(rel);
        if !patch_path.exists() && !full_path.exists() {
            return Ok(None);
        }
        let file_lock = self.file_lock(rel);
        let block_lock = self.block_lock(rel, block_idx);

        let outcome = {
            let _file_guard = file_lock.read();
            let _block_guard = block_lock.read();

            let cached = self.inner.delta_index.is_cached(&patch_path);
            let bitmap = self.inner.delta_index.get_or_load_bitmap(&patch_path)?;
            if !cached {
                self.inner
                    .metrics
                    .delta_bitmaps_loaded
                    .fetch_add(1, Ordering::Relaxed);
                self.inner
                    .metrics
                    .delta_bitmaps_total_bytes
                    .fetch_add(bitmap.len_bytes() as u64, Ordering::Relaxed);
            }

            let direct: Option<Result<Option<Vec<u8>>>> = match bitmap.get(block_idx) {
                0b00 => Some(Ok(None)),
                0b11 => Some(Err(Error::InvalidPatchFile {
                    reason: "reserved bitmap bits 11 encountered".into(),
                }
                .into())),
                0b01 => {
                    let full = open_full(&full_path)?;
                    let page = read_full_page(&full, block_idx)?;
                    Some(Ok(Some(page.to_vec())))
                }
                0b10 => None,
                _ => None,
            };

            if let Some(result) = direct {
                result
            } else {
                let patch_file = open_patch(&patch_path)?;
                let (kind, _flags, payload) = read_patch_slot(&patch_file, block_idx)?;
                match kind {
                    SlotKind::Patch => {
                        let base = self.read_base_block(rel, block_idx)?;
                        let patched = apply_patch_v2(&base, &payload)?;
                        Ok(Some(patched.to_vec()))
                    }
                    SlotKind::FullRef => Err(Error::InvalidPatchFile {
                        reason: "bitmap/slot kind mismatch (expected PATCH)".into(),
                    }
                    .into()),
                    SlotKind::Empty => Ok(None),
                }
            }
        }?;

        self.cleanup_block_lock(rel, block_idx, &block_lock);
        self.cleanup_file_lock(rel, &file_lock);

        Ok(outcome)
    }

    /// Read a base page from pbk_store layers ignoring any diff or delta
    /// materialization. Used by delta writes to compute deltas relative to the
    /// original backup contents.
    fn read_base_block(&self, rel: &Path, block_idx: u64) -> Result<[u8; PAGE_SIZE]> {
        for (idx, layer) in self.inner.layers.iter().enumerate() {
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
                if let Some(data) = self.read_pg_data_block(
                    rel,
                    &path,
                    self.file_compression_for_layer(idx, rel),
                    block_idx,
                )? {
                    return Ok(self.pad_block(&data));
                }
                continue;
            }

            if let Some(data) = self.read_full_block(
                rel,
                &path,
                layer,
                self.file_compression_for_layer(idx, rel),
                block_idx,
            )? {
                return Ok(self.pad_block(&data));
            }
        }

        if self.is_pg_datafile(rel) {
            return Ok([0u8; PAGE_SIZE]);
        }

        Err(io::Error::from(io::ErrorKind::NotFound).into())
    }

    fn pad_block(&self, data: &[u8]) -> [u8; PAGE_SIZE] {
        let mut out = [0u8; PAGE_SIZE];
        let copy_len = data.len().min(PAGE_SIZE);
        out[..copy_len].copy_from_slice(&data[..copy_len]);
        out
    }

    pub fn delta_paths(&self, rel: &Path) -> (PathBuf, PathBuf) {
        let diff = self.inner.diff.join(rel);
        let ext = diff.extension().and_then(|e| e.to_str()).unwrap_or("");

        let mut patch = diff.clone();
        let mut patch_ext = OsString::new();
        if ext.is_empty() {
            patch_ext.push("patch");
        } else {
            patch_ext.push(ext);
            patch_ext.push(".patch");
        }
        patch.set_extension(patch_ext);

        let mut full = diff.clone();
        let mut full_ext = OsString::new();
        if ext.is_empty() {
            full_ext.push("full");
        } else {
            full_ext.push(ext);
            full_ext.push(".full");
        }
        full.set_extension(full_ext);

        (patch, full)
    }

    pub fn layer_roots(&self) -> Vec<(PathBuf, Option<CompressionAlgorithm>)> {
        self.inner
            .layers
            .iter()
            .map(|l| (l.root.clone(), l.compression))
            .collect()
    }

    /// Public wrapper for computing logical length of a path as seen through the
    /// overlay (diff + all layers).
    ///
    /// Used by the FUSE adapter to report file sizes that match what PostgreSQL
    /// and `pg_probackup restore` see, rather than raw on-disk sizes of backup
    /// files (e.g. pg_probackup page streams with per-page headers).
    pub fn logical_len_for(&self, relative: impl AsRef<Path>) -> Result<Option<u64>> {
        // Always compute fresh logical length to avoid stale cached sizes.
        // Caching file sizes is risky: writes can extend files, and the async
        // worker pool makes cache invalidation difficult. The overhead of
        // fs::metadata() per getattr is minimal (~1-5μs) compared to correctness.
        self.logical_len(relative.as_ref())
    }

    pub fn top_layer_has_datafile(&self, relative: impl AsRef<Path>) -> bool {
        let rel = relative.as_ref();

        if !self.is_pg_datafile(rel) {
            return false;
        }

        let top_root = match self.inner.layers.first() {
            Some(layer) => layer.root.clone(),
            None => return false,
        };

        let mut cache = match self.inner.file_meta.lock() {
            Ok(c) => c,
            Err(_) => return false,
        };

        let layer_map = if let Some(m) = cache.get(&top_root) {
            m
        } else {
            let map = Self::load_backup_content_for_root(&top_root);
            cache.insert(top_root.clone(), map);
            cache.get(&top_root).unwrap()
        };

        let key = rel.to_string_lossy();
        if let Some(meta) = layer_map.get(key.as_ref()) {
            meta.is_datafile && meta.external_dir_num == 0
        } else {
            false
        }
    }

    /// Return names of virtual datafiles that exist only in backup metadata
    /// (backup_content.control) for the newest layer but are missing from all
    /// physical layers and diff, scoped to a given directory.
    pub fn virtual_datafile_children(&self, rel: &Path) -> Vec<String> {
        let mut out = Vec::new();

        // We only ever synthesize datafiles for the newest layer (target backup).
        let top_root = match self.inner.layers.first() {
            Some(layer) => layer.root.clone(),
            None => return out,
        };

        // Root directory is always backed by a real directory; we only synthesize
        // children inside existing directories like base/DBOID, global, etc.
        if rel.as_os_str().is_empty() {
            return out;
        }

        let mut cache = match self.inner.file_meta.lock() {
            Ok(c) => c,
            Err(_) => return out,
        };

        let layer_map = if let Some(m) = cache.get(&top_root) {
            m
        } else {
            let map = Self::load_backup_content_for_root(&top_root);
            cache.insert(top_root.clone(), map);
            cache.get(&top_root).unwrap()
        };

        for (path, meta) in layer_map.iter() {
            if !meta.is_datafile || meta.external_dir_num != 0 {
                continue;
            }
            let entry_path = Path::new(path);

            // Only consider direct children of `rel`.
            if entry_path.parent() != Some(rel) {
                continue;
            }

            // Skip files that already exist in diff or any base layer.
            if self.inner.diff.join(entry_path).exists() {
                continue;
            }
            if !self.matching_layers(entry_path).is_empty() {
                continue;
            }

            if let Some(name) = entry_path.file_name().and_then(|n| n.to_str()) {
                out.push(name.to_string());
            }
        }

        out
    }

    /// Determine per-file compression algorithm for a given layer and relative
    /// path using pg_probackup's `backup_content.control`, falling back to
    /// backup-level compression when file-level metadata is missing.
    fn file_compression_for_layer(
        &self,
        layer_idx: usize,
        rel: &Path,
    ) -> Option<CompressionAlgorithm> {
        // Datafiles continue to use backup-level compression; pg_probackup
        // writes page streams for them and we already interpret those at the
        // page level.
        if self.is_pg_datafile(rel) {
            return self.inner.layers.get(layer_idx).and_then(|l| l.compression);
        }

        let layer_comp = self.inner.layers.get(layer_idx).and_then(|l| l.compression);

        let root = match self.inner.layers.get(layer_idx) {
            Some(layer) => layer.root.clone(),
            None => return layer_comp,
        };

        let rel_key = rel.to_string_lossy().to_string();

        // Fast path: try to read from cache, populating per-layer metadata map
        // on first access. If there is no entry for `rel_key` in
        // backup_content.control (or the file is not listed there at all),
        // fall back to the backup-level compression algorithm.
        if let Ok(mut cache) = self.inner.file_meta.lock() {
            let layer_map = if let Some(existing) = cache.get(&root) {
                existing
            } else {
                let map = Self::load_backup_content_for_root(&root);
                cache.insert(root.clone(), map);
                cache.get(&root).unwrap()
            };

            if let Some(meta) = layer_map.get(&rel_key) {
                // Honour explicit per-file compression ("zlib", "lz4",
                // "zstd" or "none") without falling back to the backup-level
                // algorithm. When compress_alg = "none" in
                // backup_content.control, pg_probackup stores this file
                // uncompressed even if the backup as a whole is compressed.
                return meta.compression;
            }
        }

        // No per-file metadata: use backup-level compression, if any.
        layer_comp
    }

    /// Return the declared length (in blocks) for a datafile as recorded in
    /// pg_probackup's backup_content.control for the given layer, if present.
    /// Falls back to full_size when n_blocks is absent.
    fn file_meta_len(&self, layer: &Layer, rel: &Path) -> Option<u64> {
        if !self.is_pg_datafile(rel) {
            return None;
        }

        let root = layer.root.clone();
        let rel_key = rel.to_string_lossy().to_string();

        let mut cache = self.inner.file_meta.lock().ok()?;
        let layer_map = if let Some(m) = cache.get(&root) {
            m
        } else {
            let map = Self::load_backup_content_for_root(&root);
            cache.insert(root.clone(), map);
            cache.get(&root).unwrap()
        };

        // backup_content.control stores datafiles as "database/<path>" for the
        // main tablespace or "external_dirN/<path>" for external tablespaces.
        // Our overlay paths are prefix-less (e.g. "base/16389/16414"), so try
        // the canonical database prefix first and then fall back to any entry
        // whose suffix matches and is marked as a datafile. This covers
        // external tablespaces without hardcoding N.
        let meta = layer_map
            .get(&format!("database/{}", rel_key))
            .copied()
            .or_else(|| {
                layer_map.iter().find_map(|(k, v)| {
                    if v.is_datafile && k.ends_with(&rel_key) {
                        Some(*v)
                    } else {
                        None
                    }
                })
            })?;
        if let Some(n) = meta.n_blocks {
            return Some(n);
        }

        if let Some(full) = meta.full_size {
            let bs = self.inner.block_size.get() as u64;
            // Round up to full blocks to avoid truncating when full_size is not
            // an exact multiple of block size (should be exact for datafiles).
            return Some((full + bs - 1) / bs);
        }

        None
    }

    fn load_backup_content_for_root(root: &Path) -> HashMap<String, FileMeta> {
        let mut result = HashMap::new();
        let parent = match root.parent() {
            Some(p) => p,
            None => return result,
        };

        let path = parent.join("backup_content.control");
        let file = match fs::File::open(&path) {
            Ok(f) => f,
            Err(_) => return result,
        };

        let reader = BufReader::new(file);
        for line in reader.lines() {
            let line = match line {
                Ok(s) => s,
                Err(_) => continue,
            };
            let entry: BackupContentEntry = match serde_json::from_str(&line) {
                Ok(e) => e,
                Err(_) => continue,
            };

            let alg = entry.compress_alg.as_ref().and_then(|s| {
                if s.eq_ignore_ascii_case("none") {
                    None
                } else {
                    CompressionAlgorithm::from_pg_probackup(s).ok()
                }
            });

            let is_datafile = entry
                .is_datafile
                .as_ref()
                .map(|s| s == "1")
                .unwrap_or(false);

            let n_blocks = entry.n_blocks.as_ref().and_then(|s| s.parse::<u64>().ok());

            let full_size = entry.full_size.as_ref().and_then(|s| s.parse::<u64>().ok());

            let external_dir_num = entry
                .external_dir_num
                .as_ref()
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(0);

            result.insert(
                entry.path,
                FileMeta {
                    compression: alg,
                    is_datafile,
                    external_dir_num,
                    n_blocks,
                    full_size,
                },
            );
        }

        result
    }

    pub fn invalidate_cache(&self, rel: &Path) {
        if let Ok(mut cache) = self.inner.cache.lock() {
            cache.remove(rel);
            debug!(path = %rel.display(), "cache_invalidated");
        }
    }

    /// Record that a write modified the given byte range in the diff file so
    /// subsequent reads will source blocks from the diff rather than
    /// re-copying from base layers.
    pub(crate) fn record_write(&self, rel: &Path, offset: u64, len: usize) {
        let block_sz = self.inner.block_size.get() as u64;
        let start_block = offset / block_sz;
        let end_block = offset.saturating_add(len as u64).saturating_sub(1) / block_sz;
        let end_offset = offset.saturating_add(len as u64);

        if let Ok(mut cache) = self.inner.cache.lock() {
            let entry = cache.entry(rel.to_path_buf()).or_default();
            if self.is_pg_datafile(rel) {
                // For datafiles keep block-level materialization to extend logical length,
                // but drop cached sources so they will be re-evaluated. Block indexes
                // for base backup layers remain valid because those files are immutable.
                entry.sources.clear();
                for blk in start_block..=end_block {
                    entry.materialized.insert(blk);
                }
                entry.max_written_end = Some(entry.max_written_end.unwrap_or(0).max(end_offset));
                entry.logical_len = None;
                debug!(
                    path = %rel.display(),
                    start_block,
                    end_block,
                    end_offset,
                    max_written_end = entry.max_written_end.unwrap_or(0),
                    "record_write_pgdata"
                );
                return;
            }

            for blk in start_block..=end_block {
                entry.materialized.insert(blk);
                // Drop any remembered upstream source: diff now authoritative.
                entry.sources.remove(&blk);
            }
            entry.max_written_end = Some(entry.max_written_end.unwrap_or(0).max(end_offset));
            debug!(
                path = %rel.display(),
                start_block,
                end_block,
                end_offset,
                max_written_end = entry.max_written_end.unwrap_or(0),
                "record_write_regular"
            );
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
            // absent from all layers, it may represent either:
            //   - a relation that existed but had no pages at backup time
            //     (present in backup_content.control, size == 0), or
            //   - a relation that was dropped before the target backup
            //     (absent from backup_content.control for the top layer).
            // Only the first case should materialize a zero-length file.
            if self.is_pg_datafile(rel) {
                if !self.top_layer_has_datafile(rel) {
                    return Err(io::Error::from(io::ErrorKind::NotFound).into());
                }
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
            let has_pagemap = top_path.with_extension("pagemap").exists();

            // Non-data files in DELTA/PAGE backups are stored as whole files, not
            // per-page BackupPageHeader streams. Treat them as full-file copies to
            // avoid mis-parsing (e.g., WAL segments, config files).
            if !has_pagemap && !self.is_pg_datafile(rel) {
                if let Some(parent) = diff_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                let file_algo = self.file_compression_for_layer(*top_idx, rel);
                if let Some(algo) = file_algo {
                    return self.decompress_file(rel, top_path, &diff_path, algo);
                }
                fs::copy(top_path, &diff_path)?;
                return Ok(());
            }

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

        if let Some(algo) = self.file_compression_for_layer(*top_idx, rel) {
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

    fn decompress_full_to_vec(
        &self,
        base_path: &Path,
        algo: CompressionAlgorithm,
    ) -> Result<Vec<u8>> {
        match algo {
            CompressionAlgorithm::Zlib => {
                let reader = fs::File::open(base_path)?;
                let mut decoder = flate2::read::ZlibDecoder::new(reader);
                let mut out = Vec::new();
                decoder.read_to_end(&mut out)?;
                Ok(out)
            }
            CompressionAlgorithm::Lz4 => {
                let bytes = fs::read(base_path)?;
                lz4_flex::block::decompress_size_prepended(&bytes)
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e).into())
            }
            CompressionAlgorithm::Zstd => {
                let reader = fs::File::open(base_path)?;
                let decoded = zstd::stream::decode_all(reader)?;
                Ok(decoded)
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

        let (base_idx, base_path) = if let Some(pos) = base_pos {
            // Found a FULL backup - use it as the base.
            matches.remove(pos)
        } else {
            // No FULL backup found. Use the oldest incremental layer as the base.
            // For new files created after the last FULL backup, pg_probackup stores
            // the entire file in the first incremental backup where it appears
            // (with use_pagemap=false, all pages are backed up).
            if matches.is_empty() {
                return Err(Error::MissingBackup(rel.display().to_string()).into());
            }
            // After reverse(), the oldest backup is at the end of the vector.
            matches.remove(matches.len() - 1)
        };

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
            CompressionAlgorithm::Lz4 => {
                lz4_flex::block::decompress(data, self.inner.block_size.get())
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e).into())
            }
            CompressionAlgorithm::Zstd => zstd::stream::decode_all(data)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e).into()),
        }
    }

    /// Build or fetch from cache the pg_probackup block index for a given
    /// relation path and physical source file. Shared between logical length
    /// calculation and page reads so that we scan the on-disk page stream at
    /// most once per file.
    fn pg_block_index(&self, rel: &Path, src: &Path) -> Result<Vec<BlockIndexEntry>> {
        let mut cache = self.inner.cache.lock().unwrap();
        let entry = cache.entry(rel.to_path_buf()).or_default();
        if let Some(idx) = entry.pg_block_index.get(src) {
            return Ok(idx.clone());
        }
        drop(cache);

        let built = self.build_pg_block_index(rel, src)?;
        let mut cache = self.inner.cache.lock().unwrap();
        let entry = cache.entry(rel.to_path_buf()).or_default();
        entry
            .pg_block_index
            .insert(src.to_path_buf(), built.clone());
        Ok(built)
    }

    fn build_pg_block_index(&self, rel: &Path, src: &Path) -> Result<Vec<BlockIndexEntry>> {
        let mut reader = BufReader::new(fs::File::open(src)?);
        let mut header_buf = [0u8; std::mem::size_of::<BackupPageHeader>()];
        let mut offset: u64 = 0;
        let mut index = Vec::new();

        loop {
            match reader.read_exact(&mut header_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let block = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
            let compressed_size = i32::from_le_bytes(header_buf[4..8].try_into().unwrap());

            if compressed_size == PAGE_TRUNCATED {
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

            let data_offset = offset + header_buf.len() as u64;
            index.push(BlockIndexEntry {
                block,
                offset: data_offset,
                compressed_size,
            });

            reader.seek(SeekFrom::Current(compressed_size as i64))?;
            offset = data_offset + compressed_size as u64;
        }

        // Sort by block number to enable efficient lookups for individual pages.
        // Consumers only rely on the set of blocks, not on the original order.
        index.sort_by_key(|e| e.block);

        Ok(index)
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

    /// Detect PostgreSQL relation files (main + forks + segments).
    ///
    /// Primary source of truth is pg_probackup's backup_content.control for the
    /// newest layer: if an entry exists, honour its `is_datafile` flag so that
    /// forks that pg_probackup stores as whole files (e.g. *_fsm/_vm) are not
    /// misinterpreted as per-page streams. When metadata is absent we fall back
    /// to the path-based heuristic below.
    ///
    /// Accepts:
    ///   - base/<dboid>/<relfilenode>[.seg][_(vm|fsm|init)]
    ///   - global/<relfilenode>[...]
    ///   - pg_tblspc/<spcoid>/PG_*/<dboid>/<relfilenode>[...]
    ///     Rejects `_ptrack` and any non-digit/unsupported fork names.
    pub fn is_pg_datafile(&self, rel: &Path) -> bool {
        // Validate directory layout.
        let comps_owned: Vec<String> = rel
            .components()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect();
        let mut comps: Vec<&str> = comps_owned.iter().map(|s| s.as_str()).collect();
        if comps.is_empty() {
            return false;
        }
        let fname = comps.pop().unwrap();
        let ok_dir = match comps.as_slice() {
            [] => true, // Allow bare relfilenode paths used in tests/fixtures.
            ["base", _db] => true,
            ["base", ..] => true,
            ["global"] | ["global", ..] => true,
            ["pg_tblspc", _tbl, ver, _db] | ["pg_tblspc", _tbl, ver, _db, ..]
                if ver.starts_with("PG_") =>
            {
                true
            }
            _ => false,
        };
        if !ok_dir {
            return false;
        }

        // Optional segment suffix ".<segno>"
        let (stem, seg) = if let Some((left, right)) = fname.rsplit_once('.') {
            if right.is_empty() || right.starts_with('0') || right.len() > MAX_SEGMENT_DIGITS {
                return false;
            }
            if !right.chars().all(|c| c.is_ascii_digit()) {
                return false;
            }
            (left, Some(right))
        } else {
            (fname, None)
        };

        // Optional fork suffix "_vm"/"_fsm"/"_init"
        let (oid_part, fork) = if let Some((left, right)) = stem.split_once('_') {
            (left, Some(right))
        } else {
            (stem, None)
        };

        // Validate relfilenode
        if oid_part.is_empty() || oid_part.starts_with('0') || oid_part.len() > MAX_OID_DIGITS {
            return false;
        }
        if !oid_part.chars().all(|c| c.is_ascii_digit()) {
            return false;
        }

        if let Some(f) = fork {
            match f {
                "vm" | "fsm" | "init" => {}
                _ => return false, // includes _ptrack and unknown forks
            }
        }

        if let Some(segno) = seg {
            if !segno.chars().all(|c| c.is_ascii_digit()) {
                return false;
            }
        }

        // If we have metadata for the newest layer, prefer that over the
        // heuristic. This fixes cases where pg_probackup marks forks like
        // *_fsm/_vm as regular files (is_datafile = 0) while the pathname alone
        // would otherwise be treated as a relation fork.
        if let Some(top_layer) = self.inner.layers.first() {
            if let Ok(mut cache) = self.inner.file_meta.lock() {
                let layer_map = cache
                    .entry(top_layer.root.clone())
                    .or_insert_with(|| Self::load_backup_content_for_root(&top_layer.root));
                let key = rel.to_string_lossy().to_string();
                if let Some(meta) = layer_map.get(&key) {
                    return meta.is_datafile && meta.external_dir_num == 0;
                }
            }
        }

        true
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
