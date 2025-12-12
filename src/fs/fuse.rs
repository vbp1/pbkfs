//! FUSE adapter that projects a read-only base (`pbk_store`) with a
//! copy-on-write diff (`pbk_diff`).

use std::{
    collections::{HashMap, HashSet},
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io,
    os::unix::fs::{FileExt, MetadataExt, PermissionsExt},
    panic::{self, AssertUnwindSafe},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use anyhow::Error as AnyhowError;
use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use fuser::{
    BackgroundSession, FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate,
    ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite,
    Request,
};
use libc::{EACCES, EEXIST, EIO, EISDIR, ENOENT, ENOTEMPTY};
use tracing::{debug, error};

use crate::fs::overlay::Overlay;
use crate::fs::pending::PendingOps;
use crate::Result;

const TTL: Duration = Duration::from_secs(1);
const GENERATION: u64 = 0;
// PostgreSQL OID is u32: up to 10 decimal digits (4_294_967_295).
const MAX_OID_DIGITS: usize = 10;
// pg_probackup segment numbers are bounded (pgFileSize): up to 5 digits.
const MAX_SEGMENT_DIGITS: usize = 5;

#[derive(Debug)]
struct FsWorkerMetricsInner {
    queue_depth: AtomicUsize,
    max_queue_depth: AtomicUsize,
    tasks_total: AtomicU64,
    tasks_failed: AtomicU64,
    rejected: AtomicU64,
    last_task_latency_us: AtomicU64,
}

impl Default for FsWorkerMetricsInner {
    fn default() -> Self {
        Self {
            queue_depth: AtomicUsize::new(0),
            max_queue_depth: AtomicUsize::new(0),
            tasks_total: AtomicU64::new(0),
            tasks_failed: AtomicU64::new(0),
            rejected: AtomicU64::new(0),
            last_task_latency_us: AtomicU64::new(0),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct FsWorkerMetrics {
    pub queue_depth: usize,
    pub max_queue_depth: usize,
    pub tasks_total: u64,
    pub tasks_failed: u64,
    pub rejected: u64,
    pub last_task_latency_us: u64,
}

impl FsWorkerMetricsInner {
    fn snapshot(&self) -> FsWorkerMetrics {
        FsWorkerMetrics {
            queue_depth: self.queue_depth.load(Ordering::Relaxed),
            max_queue_depth: self.max_queue_depth.load(Ordering::Relaxed),
            tasks_total: self.tasks_total.load(Ordering::Relaxed),
            tasks_failed: self.tasks_failed.load(Ordering::Relaxed),
            rejected: self.rejected.load(Ordering::Relaxed),
            last_task_latency_us: self.last_task_latency_us.load(Ordering::Relaxed),
        }
    }

    fn increment_queued(&self) {
        let depth = self.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
        self.max_queue_depth.fetch_max(depth, Ordering::Relaxed);
    }

    fn decrement_queued(&self) {
        self.queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    fn record_task(&self, ok: bool, latency_us: u64) {
        self.tasks_total.fetch_add(1, Ordering::Relaxed);
        if !ok {
            self.tasks_failed.fetch_add(1, Ordering::Relaxed);
        }
        self.last_task_latency_us
            .store(latency_us, Ordering::Relaxed);

        let total = self.tasks_total.load(Ordering::Relaxed);
        if !ok || total % 1000 == 0 {
            let snapshot = self.snapshot();
            crate::logging::log_fs_worker_pool_metrics(
                crate::logging::FsWorkerPoolSnapshot {
                    queue_depth: snapshot.queue_depth,
                    max_queue_depth: snapshot.max_queue_depth,
                    tasks_total: snapshot.tasks_total,
                    tasks_failed: snapshot.tasks_failed,
                    rejected: snapshot.rejected,
                    last_task_latency_us: snapshot.last_task_latency_us,
                },
                false,
            );
        }
    }

    fn record_rejected(&self) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
        let snapshot = self.snapshot();
        crate::logging::log_fs_worker_pool_metrics(
            crate::logging::FsWorkerPoolSnapshot {
                queue_depth: snapshot.queue_depth,
                max_queue_depth: snapshot.max_queue_depth,
                tasks_total: snapshot.tasks_total,
                tasks_failed: snapshot.tasks_failed,
                rejected: snapshot.rejected,
                last_task_latency_us: snapshot.last_task_latency_us,
            },
            true,
        );
    }
}

enum FsTask {
    Read {
        overlay: Overlay,
        rel: PathBuf,
        offset: u64,
        size: usize,
        reply: ReplyData,
        handle: Option<Arc<File>>,
        wal_state: Option<WalFileState>, // Some(state) for WAL under --no-wal
    },
    Write {
        ino: u64,
        seq: u64,
        overlay: Overlay,
        rel: PathBuf,
        offset: i64,
        data: Vec<u8>,
        reply: ReplyWrite,
        _handle: Option<Arc<File>>, // reserved for future direct-diff writes
    },
    WalWrite {
        ino: u64,
        seq: u64,
        rel: PathBuf,
        offset: u64,
        len: usize,
        reply: ReplyWrite,
        wal_state: Arc<Mutex<HashMap<PathBuf, WalFileState>>>,
        base_len: u64,
        mode: u32,
    },
    Fsync {
        ino: u64,
        seq: u64,
        overlay: Overlay,
        rel: PathBuf,
        datasync: bool,
        reply: ReplyEmpty,
    },
}

#[derive(Debug, Clone)]
struct OpenHandle {
    file: Option<Arc<File>>, // diff or base file handle
    from_diff: bool,
}

#[derive(Debug, Clone)]
struct WalFileState {
    logical_size: u64,
    mode: u32,
    present: bool,
}

impl FsTask {
    fn run(self, metrics: &FsWorkerMetricsInner, pending_ops: &PendingOps) {
        let start = Instant::now();
        let mut ok = true;

        match self {
            FsTask::Read {
                overlay,
                rel,
                offset,
                size,
                reply,
                handle,
                wal_state,
            } => {
                let is_wal = overlay.no_wal() && OverlayFs::is_pg_wal_child(&rel);
                // For PostgreSQL datafiles, always route through overlay.read_range()
                // to honor logical lengths, sparse holes, and delta storage.
                if overlay.is_pg_datafile(&rel) || is_wal {
                    if is_wal {
                        if let Some(state) = wal_state {
                            if !state.present {
                                ok = false;
                                reply.error(ENOENT);
                                // handled
                                metrics.record_task(ok, start.elapsed().as_micros() as u64);
                                return;
                            }
                            let logical = state.logical_size;
                            if offset >= logical {
                                reply.data(&[]);
                                metrics.record_task(ok, start.elapsed().as_micros() as u64);
                                return;
                            }
                            let end = (offset + size as u64).min(logical);
                            let buf_len = (end - offset) as usize;
                            let buf = vec![0u8; buf_len];
                            reply.data(&buf);
                            metrics.record_task(ok, start.elapsed().as_micros() as u64);
                            return;
                        }
                    }

                    match overlay.read_range(&rel, offset, size) {
                        Ok(Some(bytes)) => {
                            debug!(
                                path = %rel.display(),
                                offset,
                                size,
                                is_wal,
                                returned = bytes.len(),
                                "fuse_read_pgdata"
                            );
                            // PostgreSQL treats short reads as corruption; if we
                            // somehow produced an empty buffer while a positive
                            // size was requested (e.g., length underestimation),
                            // return zeroed bytes instead of EOF to allow WAL
                            // replay to extend the relation.
                            if bytes.is_empty() && size > 0 {
                                debug!(
                                    path = %rel.display(),
                                    offset,
                                    size,
                                    "fuse_pgdata_zero_fill_empty",
                                );
                                reply.data(&vec![0u8; size]);
                            } else {
                                reply.data(&bytes)
                            }
                        }
                        Ok(None) => {
                            ok = false;
                            reply.error(ENOENT);
                        }
                        Err(_) => {
                            ok = false;
                            reply.error(EIO);
                        }
                    }
                } else if let Some(file) = handle {
                    let mut buf = vec![0u8; size];
                    match file.read_at(&mut buf, offset) {
                        Ok(read) => {
                            buf.truncate(read);
                            reply.data(&buf);
                        }
                        Err(err) => {
                            ok = false;
                            reply.error(err.raw_os_error().unwrap_or(EIO));
                        }
                    }
                } else {
                    match overlay.read_range(&rel, offset, size) {
                        Ok(Some(bytes)) => reply.data(&bytes),
                        Ok(None) => {
                            ok = false;
                            reply.error(ENOENT);
                        }
                        Err(_) => {
                            ok = false;
                            reply.error(EIO);
                        }
                    }
                }
            }
            FsTask::Write {
                ino,
                seq,
                overlay,
                rel,
                offset,
                data,
                reply,
                _handle: _,
            } => {
                // Wait for all preceding writes to complete (FIFO order)
                pending_ops.wait_for_preceding(ino, seq);

                let start_off = offset.max(0) as u64;
                if let Err(err) = overlay.write_at(&rel, start_off, &data) {
                    debug!(
                        error = ?err,
                        path = %rel.display(),
                        offset = start_off,
                        len = data.len(),
                        "fs_worker_write_overlay_failed"
                    );
                    reply.error(OverlayFs::err_from_anyhow(err));
                    pending_ops.decrement(ino, seq);
                    return;
                }

                debug!(path = %rel.display(), ino, seq, "WRITE decrement");
                reply.written(data.len() as u32);
                pending_ops.decrement(ino, seq);
            }
            FsTask::WalWrite {
                ino,
                seq,
                rel,
                offset,
                len,
                reply,
                wal_state,
                base_len,
                mode,
            } => {
                pending_ops.wait_for_preceding(ino, seq);

                {
                    let mut guard = wal_state.lock().unwrap();
                    let entry = guard.entry(rel.clone()).or_insert(WalFileState {
                        logical_size: base_len,
                        mode,
                        present: true,
                    });
                    entry.present = true;
                    entry.mode = if entry.mode == 0 { mode } else { entry.mode };
                    let end = offset.saturating_add(len as u64);
                    entry.logical_size = entry.logical_size.max(base_len).max(end);
                }

                reply.written(len as u32);
                pending_ops.decrement(ino, seq);
            }
            FsTask::Fsync {
                ino,
                seq,
                overlay,
                rel,
                datasync,
                reply,
            } => {
                // Wait for all preceding operations to complete (FIFO order)
                pending_ops.wait_for_preceding(ino, seq);

                if let Err(err) = overlay.fsync_path(&rel, datasync) {
                    ok = false;
                    reply.error(OverlayFs::err_from_anyhow(err));
                } else {
                    reply.ok();
                }
                pending_ops.decrement(ino, seq);
            }
        }

        let elapsed_us = start.elapsed().as_micros() as u64;
        metrics.record_task(ok, elapsed_us);
    }
}

struct FsWorkerPool {
    tx: Sender<FsTask>,
    _rx: Receiver<FsTask>,
    metrics: Arc<FsWorkerMetricsInner>,
    pending_ops: Arc<PendingOps>,
}

impl FsWorkerPool {
    fn new(overlay: &Overlay, pending_ops: Arc<PendingOps>) -> Self {
        let workers = std::env::var("PBKFS_FS_WORKERS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|n| n.get().max(2))
                    .unwrap_or(4)
            });
        let queue_capacity = std::env::var("PBKFS_FS_QUEUE")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(workers * 32);

        let (tx, rx) = bounded::<FsTask>(queue_capacity);
        let shared_rx = rx.clone();
        let metrics = Arc::new(FsWorkerMetricsInner::default());

        for idx in 0..workers {
            let worker_rx = shared_rx.clone();
            let metrics = metrics.clone();
            let pending_ops = pending_ops.clone();
            let overlay_clone = overlay.clone();
            std::thread::Builder::new()
                .name(format!("pbkfs-fs-worker-{idx}"))
                .spawn(move || {
                    for task in worker_rx {
                        let _ = &overlay_clone;
                        let result = panic::catch_unwind(AssertUnwindSafe(|| {
                            FsTask::run(task, &metrics, &pending_ops);
                        }));
                        metrics.decrement_queued();
                        if let Err(panic) = result {
                            metrics.record_task(false, 0);
                            error!(
                                target = "pbkfs::fs_worker",
                                ?panic,
                                "fs_worker_task_panicked"
                            );
                        }
                    }
                })
                .expect("failed to spawn fs worker thread");
        }

        Self {
            tx,
            _rx: rx,
            metrics,
            pending_ops,
        }
    }

    fn submit(&self, task: FsTask) {
        self.metrics.increment_queued();
        match self.tx.try_send(task) {
            Ok(()) => {}
            Err(TrySendError::Full(task)) => {
                self.metrics.record_rejected();
                FsTask::run(task, &self.metrics, &self.pending_ops);
                self.metrics.decrement_queued();
            }
            Err(TrySendError::Disconnected(task)) => {
                FsTask::run(task, &self.metrics, &self.pending_ops);
                self.metrics.decrement_queued();
            }
        }
    }

    // Exposed for Phase 8 (stats dump via SIGUSR1/SIGUSR2) to snapshot worker
    // queue/load metrics without blocking the worker threads.
    #[allow(dead_code)]
    fn metrics(&self) -> FsWorkerMetrics {
        self.metrics.snapshot()
    }
}

pub struct OverlayFs {
    overlay: Overlay,
    no_wal: bool,
    paths: Mutex<HashMap<u64, PathBuf>>,  // ino -> rel path
    inodes: Mutex<HashMap<PathBuf, u64>>, // rel path -> ino
    next_ino: Mutex<u64>,
    handles: Mutex<HashMap<u64, OpenHandle>>, // fh -> handle
    next_fh: AtomicU64,
    handle_hits: AtomicU64,
    handle_misses: AtomicU64,
    pending_ops: Arc<PendingOps>,
    worker_pool: FsWorkerPool,
    wal_state: Arc<Mutex<HashMap<PathBuf, WalFileState>>>,
}

impl OverlayFs {
    pub fn new(overlay: Overlay) -> Self {
        let mut paths = HashMap::new();
        let mut inodes = HashMap::new();
        paths.insert(1, PathBuf::from(""));
        inodes.insert(PathBuf::from(""), 1);
        let pending_ops = Arc::new(PendingOps::new());
        let worker_pool = FsWorkerPool::new(&overlay, pending_ops.clone());
        let no_wal = overlay.no_wal();
        Self {
            overlay,
            no_wal,
            paths: Mutex::new(paths),
            inodes: Mutex::new(inodes),
            next_ino: Mutex::new(2),
            handles: Mutex::new(HashMap::new()),
            next_fh: AtomicU64::new(2),
            handle_hits: AtomicU64::new(0),
            handle_misses: AtomicU64::new(0),
            pending_ops,
            worker_pool,
            wal_state: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn err_code(err: io::Error) -> i32 {
        err.raw_os_error().unwrap_or(EIO)
    }

    fn err_from_anyhow(err: AnyhowError) -> i32 {
        match err.downcast::<io::Error>() {
            Ok(io_err) => Self::err_code(io_err),
            Err(_) => EIO,
        }
    }

    fn rel_for(&self, ino: u64) -> Option<PathBuf> {
        self.paths.lock().unwrap().get(&ino).cloned()
    }

    fn move_inode(&self, from: &Path, to: &Path) {
        let mut paths = self.paths.lock().unwrap();
        let mut inodes = self.inodes.lock().unwrap();
        if let Some(ino) = inodes.remove(from) {
            paths.insert(ino, to.to_path_buf());
            inodes.insert(to.to_path_buf(), ino);
        }
    }

    fn insert_handle(&self, fh: u64, handle: OpenHandle) {
        self.handles.lock().unwrap().insert(fh, handle);
    }

    fn remove_handle(&self, fh: u64) {
        let _ = self.handles.lock().unwrap().remove(&fh);
    }

    fn take_handle_file(&self, fh: u64) -> Option<OpenHandle> {
        self.handles.lock().unwrap().get(&fh).cloned()
    }

    fn get_or_insert_ino(&self, rel: &Path) -> u64 {
        // Hold inodes lock for entire operation to prevent race condition
        let mut inodes = self.inodes.lock().unwrap();
        if let Some(id) = inodes.get(rel).copied() {
            return id;
        }
        // Not found - allocate new ino while still holding inodes lock
        let mut next = self.next_ino.lock().unwrap();
        let ino = *next;
        *next += 1;
        drop(next);
        self.paths.lock().unwrap().insert(ino, rel.to_path_buf());
        inodes.insert(rel.to_path_buf(), ino);
        ino
    }

    fn alloc_fh(&self) -> u64 {
        self.next_fh.fetch_add(1, Ordering::Relaxed)
    }

    fn invalidate_path_caches(&self, rel: &Path) {
        self.overlay.invalidate_cache(rel);
    }

    fn log_handle_snapshot(&self) {
        let overlay_metrics = self.overlay.metrics();
        let snapshot = crate::logging::OverlayIoSnapshot {
            cache_hits: overlay_metrics.cache_hits,
            cache_misses: overlay_metrics.cache_misses,
            handle_hits: self.handle_hits.load(Ordering::Relaxed),
            handle_misses: self.handle_misses.load(Ordering::Relaxed),
            handles_open: self.handles.lock().map(|h| h.len()).unwrap_or(0),
            delta_patch_count: overlay_metrics.delta_patch_count,
            delta_full_count: overlay_metrics.delta_full_count,
            delta_patch_avg_size: overlay_metrics.delta_patch_avg_size,
            delta_patch_max_size: overlay_metrics.delta_patch_max_size,
            delta_bitmaps_loaded: overlay_metrics.delta_bitmaps_loaded,
            delta_bitmaps_total_bytes: overlay_metrics.delta_bitmaps_total_bytes,
            delta_punch_holes: overlay_metrics.delta_punch_holes,
            delta_punch_hole_failures: overlay_metrics.delta_punch_hole_failures,
        };
        crate::logging::log_overlay_io_metrics(snapshot);
    }

    fn child_rel(&self, parent: u64, name: &OsStr) -> Option<PathBuf> {
        let parent_path = self.rel_for(parent)?;
        Some(if parent_path.as_os_str().is_empty() {
            PathBuf::from(name)
        } else {
            parent_path.join(name)
        })
    }

    fn whiteout_rel(&self, rel: &Path) -> Option<PathBuf> {
        let name = rel.file_name()?;
        let mut whiteout = std::ffi::OsString::from(".wh.");
        whiteout.push(name);
        let parent = rel.parent().unwrap_or(Path::new(""));
        Some(parent.join(whiteout))
    }

    fn is_whiteouted(&self, rel: &Path) -> bool {
        self.whiteout_rel(rel)
            .map(|w| self.overlay.diff_root().join(w).exists())
            .unwrap_or(false)
    }

    fn clear_whiteout(&self, rel: &Path) -> io::Result<()> {
        if let Some(wh_rel) = self.whiteout_rel(rel) {
            let wh_path = self.overlay.diff_root().join(wh_rel);
            if wh_path.exists() {
                fs::remove_file(wh_path)?;
            }
        }
        Ok(())
    }

    fn create_whiteout(&self, rel: &Path) -> io::Result<()> {
        if let Some(wh_rel) = self.whiteout_rel(rel) {
            let wh_path = self.overlay.diff_root().join(wh_rel);
            if let Some(parent) = wh_path.parent() {
                fs::create_dir_all(parent)?;
            }
            if !wh_path.exists() {
                fs::write(wh_path, b"")?;
            }
        }
        Ok(())
    }

    fn ensure_parent_dirs(&self, rel: &Path) -> io::Result<()> {
        if let Some(parent) = rel.parent() {
            fs::create_dir_all(self.overlay.diff_root().join(parent))?;
        }
        Ok(())
    }

    fn ensure_diff_copy(&self, rel: &Path) -> io::Result<()> {
        if self.overlay.is_pg_datafile(rel) {
            // Datafiles use delta storage; copy-up of whole files defeats dedup.
            return Ok(());
        }
        self.overlay
            .ensure_copy_up(rel)
            .map_err(|err| io::Error::from_raw_os_error(Self::err_from_anyhow(err)))
    }

    fn is_database_map(rel: &Path) -> bool {
        rel == Path::new("database_map")
    }

    fn is_pg_wal_dir(rel: &Path) -> bool {
        rel == Path::new("pg_wal")
    }

    fn is_pg_wal_child(rel: &Path) -> bool {
        rel.parent() == Some(Path::new("pg_wal"))
    }

    fn wal_state_get(&self, rel: &Path) -> Option<WalFileState> {
        self.wal_state.lock().unwrap().get(rel).cloned()
    }

    fn wal_state_upsert<F>(&self, rel: &Path, f: F)
    where
        F: FnOnce(Option<WalFileState>) -> WalFileState,
    {
        let mut guard = self.wal_state.lock().unwrap();
        let current = guard.get(rel).cloned();
        guard.insert(rel.to_path_buf(), f(current));
    }

    fn wal_state_mark_deleted(&self, rel: &Path) {
        self.wal_state_upsert(rel, |existing| WalFileState {
            present: false,
            logical_size: existing
                .as_ref()
                .map(|s| s.logical_size)
                .unwrap_or_else(|| self.wal_backing_len(rel)),
            mode: existing.as_ref().map(|s| s.mode).unwrap_or(0o600),
        });
    }

    fn wal_backing_len(&self, rel: &Path) -> u64 {
        self.overlay
            .logical_len_for(rel)
            .ok()
            .flatten()
            .or_else(|| {
                self.overlay
                    .find_layer_path(rel)
                    .and_then(|(path, _, _)| fs::symlink_metadata(path).ok().map(|m| m.len()))
            })
            .unwrap_or(0)
    }

    fn wal_state_set_present(&self, rel: &Path, target_len: u64, mode: u32) {
        let mode = mode & 0o777;
        self.wal_state_upsert(rel, |existing| {
            let prev_len = existing
                .as_ref()
                .map(|s| s.logical_size)
                .unwrap_or_else(|| self.wal_backing_len(rel));
            let prev_mode = existing.as_ref().map(|s| s.mode).unwrap_or(mode);
            WalFileState {
                present: true,
                logical_size: prev_len.max(target_len),
                mode: prev_mode,
            }
        });
    }

    fn wal_default_mode(&self, rel: &Path) -> u32 {
        // Try base metadata; fallback to 0o600.
        let layer_roots = self.overlay.layer_roots();
        if let Some((root, _)) = layer_roots.first() {
            let path = root.join(rel);
            if let Ok(meta) = fs::symlink_metadata(&path) {
                return meta.mode() & 0o777;
            }
        }
        0o600
    }

    fn wal_attr_from_state(&self, rel: &Path, state: &WalFileState) -> FileAttr {
        let meta =
            self.overlay
                .find_layer_path(rel)
                .and_then(|(p, _, _)| fs::symlink_metadata(p).ok())
                .or_else(|| fs::symlink_metadata(self.overlay.diff_root().join(rel)).ok())
                .or_else(|| {
                    rel.parent().and_then(|parent| {
                        fs::symlink_metadata(self.overlay.diff_root().join(parent))
                            .ok()
                            .or_else(|| {
                                self.overlay.layer_roots().first().and_then(|(root, _)| {
                                    fs::symlink_metadata(root.join(parent)).ok()
                                })
                            })
                    })
                });

        let (perm, uid, gid, atime, mtime, ctime, blksize, blocks) = if let Some(m) = meta {
            (
                state.mode as u16,
                m.uid(),
                m.gid(),
                m.accessed().unwrap_or(std::time::SystemTime::UNIX_EPOCH),
                m.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH),
                m.created().unwrap_or(std::time::SystemTime::UNIX_EPOCH),
                m.blksize() as u32,
                m.blocks(),
            )
        } else {
            (
                state.mode as u16,
                0,
                0,
                std::time::SystemTime::UNIX_EPOCH,
                std::time::SystemTime::UNIX_EPOCH,
                std::time::SystemTime::UNIX_EPOCH,
                4096,
                0,
            )
        };

        FileAttr {
            ino: self.get_or_insert_ino(rel),
            size: state.logical_size,
            blocks,
            atime,
            mtime,
            ctime,
            crtime: ctime,
            kind: FileType::RegularFile,
            perm,
            nlink: 1,
            uid,
            gid,
            rdev: 0,
            blksize,
            flags: 0,
        }
    }

    /// Heuristic to detect PostgreSQL main-fork relation filenames based on
    /// pg_probackup's `is_datafile` rules: `<oid>` or `<oid>.<segno>` where
    /// both components are decimal without leading zeros and within reasonable
    /// bounds.
    fn is_rel_filename(name: &str) -> bool {
        let mut chars = name.chars().peekable();
        let first = match chars.next() {
            Some(c) if c.is_ascii_digit() => c,
            _ => return false,
        };
        if first == '0' {
            return false;
        }

        // Parse OID digits, enforcing an upper bound.
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
            // Any other suffix marks it as non-datafile.
            Some(_) => false,
        }
    }

    fn visible_dir_entries(&self, rel: &Path) -> io::Result<Vec<String>> {
        let mut names = HashSet::new();
        let diff_dir = self.overlay.diff_root().join(rel);
        if let Ok(read_dir) = fs::read_dir(&diff_dir) {
            for entry in read_dir.flatten() {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if name_str.starts_with(".wh.") {
                    continue;
                }
                names.insert(name_str.to_string());
            }
        }

        let layer_roots = self.overlay.layer_roots();
        for (idx, (root, _)) in layer_roots.iter().enumerate() {
            let base_dir = root.join(rel);
            if let Ok(read_dir) = fs::read_dir(&base_dir) {
                for entry in read_dir.flatten() {
                    let name = entry.file_name().to_string_lossy().to_string();
                    // `database_map` is never restored by pg_probackup, so
                    // hide it from the projected tree to match restore
                    // semantics.
                    if rel.as_os_str().is_empty() && name == "database_map" {
                        continue;
                    }
                    let child_rel = if rel.as_os_str().is_empty() {
                        PathBuf::from(&name)
                    } else {
                        rel.join(&name)
                    };
                    // Hide stale relation files that existed only in older
                    // backups but are absent from the target backup's
                    // metadata. For forks (_fsm, _vm, etc.) we consult the
                    // main-fork relation path.
                    if idx > 0 {
                        let (main_name, main_rel_path) =
                            if let Some((base, _suffix)) = name.split_once('_') {
                                // Handle forks like 16413_fsm, 16413_vm, etc.
                                let mut p = rel.to_path_buf();
                                p.push(base);
                                (base.to_string(), p)
                            } else {
                                (name.clone(), child_rel.clone())
                            };

                        if Self::is_rel_filename(&main_name)
                            && !self.overlay.top_layer_has_datafile(&main_rel_path)
                        {
                            continue;
                        }
                    }
                    if self.is_whiteouted(&child_rel) {
                        continue;
                    }
                    names.insert(name);
                }
            }

            // For WAL we must not union segments from all backups: pg_probackup
            // restore leaves only WAL files of the target backup in PGDATA.
            // Limit directory listing of `pg_wal` to the newest layer only.
            if Self::is_pg_wal_dir(rel) && idx == 0 {
                break;
            }
        }

        // Synthesized zero-length relation files that exist only in
        // backup_content.control for the newest backup but have no physical
        // representation in any layer or diff.
        if !rel.as_os_str().is_empty() {
            for name in self.overlay.virtual_datafile_children(rel) {
                names.insert(name);
            }
        }

        if self.no_wal && Self::is_pg_wal_dir(rel) {
            if let Ok(state) = self.wal_state.lock() {
                for (path, wal) in state.iter() {
                    if path.parent() == Some(rel) {
                        if wal.present {
                            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                                names.insert(name.to_string());
                            }
                        } else if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                            names.remove(name);
                        }
                    }
                }
            }
        }

        let mut sorted: Vec<String> = names.into_iter().collect();
        sorted.sort();
        Ok(sorted)
    }

    fn path_exists_in_base(&self, rel: &Path) -> bool {
        self.overlay.find_layer_path(rel).is_some()
    }

    fn stat_path(&self, rel: &Path) -> Option<(FileAttr, bool)> {
        if self.is_whiteouted(rel) {
            return None;
        }

        // Attr cache disabled: the minor performance benefit (~1-5μs per stat)
        // is not worth the correctness risk from stale sizes after writes.
        // The write() handler invalidates cache BEFORE async worker completes,
        // creating a race where concurrent getattr can cache stale values.

        // `database_map` is never restored into PGDATA by pg_probackup
        // (restore.c skips it), so present it as absent in the mounted view.
        if Self::is_database_map(rel) {
            return None;
        }

        if self.no_wal && Self::is_pg_wal_child(rel) {
            if let Some(state) = self.wal_state_get(rel) {
                if !state.present {
                    return None;
                }
                let attr = self.wal_attr_from_state(rel, &state);
                return Some((attr, false));
            }
        }

        // Prefer diff over base; if neither has the path but this looks like a
        // PostgreSQL datafile, let the overlay materialize an appropriate
        // backing file (including zero-length relations that existed without
        // any pages at backup time).
        let diff_candidate = self.overlay.diff_root().join(rel);
        let (meta, from_diff) = match fs::symlink_metadata(&diff_candidate) {
            Ok(m) => (m, true),
            Err(_) => {
                // For WAL segments we follow pg_probackup restore semantics
                // and only expose files that exist in the newest backup's
                // data directory, ignoring older layers.
                if Self::is_pg_wal_child(rel) {
                    let layer_roots = self.overlay.layer_roots();
                    if let Some((primary_root, _)) = layer_roots.first() {
                        let base_path = primary_root.join(rel);
                        let meta = fs::symlink_metadata(&base_path).ok()?;
                        (meta, false)
                    } else {
                        return None;
                    }
                } else if let Some((base_path, _, _)) = self.overlay.find_layer_path(rel) {
                    let meta = fs::symlink_metadata(base_path).ok()?;
                    (meta, false)
                } else {
                    // Let overlay try to materialize a backing file (e.g., empty
                    // main-fork relation file); if that fails, treat as missing.
                    if self.overlay.ensure_copy_up(rel).is_err() {
                        return None;
                    }
                    let meta = fs::symlink_metadata(&diff_candidate).ok()?;
                    (meta, true)
                }
            }
        };

        let file_type = meta.file_type();
        let kind = if file_type.is_dir() {
            FileType::Directory
        } else if file_type.is_file() {
            FileType::RegularFile
        } else if file_type.is_symlink() {
            FileType::Symlink
        } else {
            FileType::RegularFile
        };

        // Prefer logical length from overlay so that sizes of PostgreSQL
        // relation files and other objects that pg_probackup stores in its own
        // on-disk format match the view produced by `pg_probackup restore`.
        let logical_size = if file_type.is_file() {
            self.overlay
                .logical_len_for(rel)
                .ok()
                .flatten()
                .unwrap_or(meta.len())
        } else {
            meta.len()
        };

        let attr = FileAttr {
            ino: self.get_or_insert_ino(rel),
            size: logical_size,
            blocks: meta.blocks(),
            atime: meta.accessed().unwrap_or(std::time::SystemTime::UNIX_EPOCH),
            mtime: meta.modified().unwrap_or(std::time::SystemTime::UNIX_EPOCH),
            ctime: meta.created().unwrap_or(std::time::SystemTime::UNIX_EPOCH),
            crtime: meta.created().unwrap_or(std::time::SystemTime::UNIX_EPOCH),
            kind,
            perm: meta.mode() as u16,
            nlink: meta.nlink() as u32,
            uid: meta.uid(),
            gid: meta.gid(),
            rdev: 0,
            blksize: meta.blksize() as u32,
            flags: 0,
        };
        Some((attr, from_diff))
    }
}

impl Filesystem for OverlayFs {
    fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let rel = match self.child_rel(parent, name) {
            Some(r) => r,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        match self.stat_path(&rel) {
            Some((attr, _)) => reply.entry(&TTL, &attr, GENERATION),
            None => reply.error(ENOENT),
        };
    }

    fn forget(&mut self, _req: &Request<'_>, ino: u64, _nlookup: u64) {
        // Clean up per-inode state when kernel forgets about this inode.
        // This prevents memory accumulation during long mount sessions.
        self.pending_ops.remove(ino);
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        // Wait for pending writes to complete before getting attributes
        self.pending_ops.wait_barrier(ino);

        match self.rel_for(ino).and_then(|p| self.stat_path(&p)) {
            Some((attr, _)) => reply.attr(&TTL, &attr),
            None => reply.error(ENOENT),
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let rel = match self.rel_for(ino) {
            Some(r) => r,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        if self.is_whiteouted(&rel) || self.stat_path(&rel).is_none() {
            reply.error(ENOENT);
            return;
        }

        // Wait for pending writes to complete before opening the file
        debug!(path = %rel.display(), ino, flags, "OPEN wait_barrier start");
        let barrier = self.pending_ops.wait_barrier(ino);
        debug!(path = %rel.display(), ino, barrier, "OPEN wait_barrier done");

        let mut file = None;
        let mut from_diff = false;
        let is_wal = self.no_wal && Self::is_pg_wal_child(&rel);
        let write_intent = (flags & libc::O_ACCMODE) != libc::O_RDONLY
            || (flags & libc::O_TRUNC) != 0
            || (flags & libc::O_CREAT) != 0;

        if is_wal {
            // Virtual WAL: never copy up or open backing handles.
        } else if write_intent {
            if let Err(err) = self.ensure_diff_copy(&rel) {
                reply.error(Self::err_code(err));
                return;
            }
            // For pg_datafile, do NOT create a file handle even for write intent.
            // Reads must go through overlay.read_range() to correctly handle sparse
            // diff files. Writes will open the diff file on-demand in FsTask::Write.
            if !self.overlay.is_pg_datafile(&rel) {
                let path = self.overlay.diff_root().join(&rel);
                match OpenOptions::new().read(true).write(true).open(&path) {
                    Ok(f) => {
                        file = Some(Arc::new(f));
                        from_diff = true;
                    }
                    Err(err) => {
                        reply.error(Self::err_code(err));
                        return;
                    }
                }
            }
        } else {
            // For pg_datafile, always use overlay.read_range() to correctly handle
            // sparse diff files and block-level materialization from backup layers.
            // Direct file handle reads on sparse diff files return 0 bytes for
            // unmaterialized regions, causing PostgreSQL "read only 0 of 8192 bytes" errors.
            let is_datafile = self.overlay.is_pg_datafile(&rel);
            let diff_path = self.overlay.diff_root().join(&rel);
            if diff_path.exists() && !is_datafile {
                if let Ok(f) = File::open(&diff_path) {
                    file = Some(Arc::new(f));
                    from_diff = true;
                }
            } else if let Some((base_path, _, _)) = self.overlay.find_layer_path(&rel) {
                if !is_datafile {
                    if let Ok(f) = File::open(&base_path) {
                        file = Some(Arc::new(f));
                    }
                }
            }
        }

        let fh = self.alloc_fh();
        self.insert_handle(fh, OpenHandle { file, from_diff });

        reply.opened(fh, 0);
    }

    fn read(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let rel = match self.rel_for(ino) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        if self.is_whiteouted(&rel) {
            reply.error(ENOENT);
            return;
        }

        // Wait for pending writes to complete before reading
        self.pending_ops.wait_barrier(ino);

        let off = if offset < 0 { 0 } else { offset as u64 };
        let is_wal = self.no_wal && Self::is_pg_wal_child(&rel);
        let handle = if is_wal {
            None // force overlay path for WAL when no_wal is on
        } else {
            self.take_handle_file(fh).and_then(|h| h.file.clone())
        };
        let total = if handle.is_some() {
            self.handle_hits.fetch_add(1, Ordering::Relaxed) + 1
        } else {
            self.handle_misses.fetch_add(1, Ordering::Relaxed) + 1
        };
        if total % 256 == 0 {
            self.log_handle_snapshot();
        }

        let rel_for_task = rel.clone();
        let wal_state = if is_wal {
            self.wal_state_get(&rel_for_task)
        } else {
            None
        };

        let task = FsTask::Read {
            overlay: self.overlay.clone(),
            rel: rel_for_task,
            offset: off,
            size: size as usize,
            reply,
            handle,
            wal_state,
        };
        self.worker_pool.submit(task);
    }

    fn write(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let rel = match self.rel_for(ino) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        if self.is_whiteouted(&rel) {
            reply.error(ENOENT);
            return;
        }

        let is_wal = self.no_wal && Self::is_pg_wal_child(&rel);

        // For pg datafiles we skip upfront copy-up: delta slots are written via Overlay::write_at.
        // For other files we keep the existing fast path with an already opened diff file.
        let mut handle_arc: Option<Arc<File>> = None;
        if !self.overlay.is_pg_datafile(&rel) && !is_wal {
            let mut handles = self.handles.lock().unwrap();
            if let Some(entry) = handles.get_mut(&fh) {
                if entry.file.is_none() || !entry.from_diff {
                    if let Err(err) = self.ensure_diff_copy(&rel) {
                        reply.error(Self::err_code(err));
                        return;
                    }
                    let path = self.overlay.diff_root().join(&rel);
                    match OpenOptions::new().read(true).write(true).open(&path) {
                        Ok(f) => {
                            entry.file = Some(Arc::new(f));
                            entry.from_diff = true;
                        }
                        Err(err) => {
                            reply.error(Self::err_code(err));
                            return;
                        }
                    }
                }
                handle_arc = entry.file.clone();
            }
        }

        // Keep cache for PostgreSQL datafiles: Overlay::record_write tracks
        // materialized/max_written_end, and a bulk invalidation breaks
        // logical_len calculation and leads to EOF on new pages.
        if !self.overlay.is_pg_datafile(&rel) {
            self.invalidate_path_caches(&rel);
        }

        if is_wal {
            let seq = self.pending_ops.increment(ino);
            let base_len = self.wal_backing_len(&rel);
            let mode = self.wal_default_mode(&rel);
            let task = FsTask::WalWrite {
                ino,
                seq,
                rel,
                offset: offset.max(0) as u64,
                len: data.len(),
                reply,
                wal_state: self.wal_state.clone(),
                base_len,
                mode,
            };
            self.worker_pool.submit(task);
            return;
        }

        // Get sequence number BEFORE submitting to worker pool
        let seq = self.pending_ops.increment(ino);
        debug!(path = %rel.display(), ino, seq, offset, len = data.len(), "WRITE increment");

        let task = FsTask::Write {
            ino,
            seq,
            overlay: self.overlay.clone(),
            rel,
            offset,
            data: data.to_vec(),
            reply,
            _handle: handle_arc,
        };
        self.worker_pool.submit(task);
    }

    fn release(
        &mut self,
        _req: &Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        self.remove_handle(fh);
        reply.ok();
    }

    fn readdir(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let rel = match self.rel_for(ino) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let mut entries = Vec::new();
        entries.push((ino, FileType::Directory, ".".to_string()));
        let parent_ino = if rel.as_os_str().is_empty() {
            ino
        } else {
            let parent = rel.parent().unwrap_or(Path::new(""));
            self.get_or_insert_ino(parent)
        };
        entries.push((parent_ino, FileType::Directory, "..".to_string()));

        let names = match self.visible_dir_entries(&rel) {
            Ok(n) => n,
            Err(err) => {
                reply.error(Self::err_code(err));
                return;
            }
        };

        for name in names {
            let child_rel = if rel.as_os_str().is_empty() {
                PathBuf::from(&name)
            } else {
                rel.join(&name)
            };
            if let Some((attr, _)) = self.stat_path(&child_rel) {
                entries.push((attr.ino, attr.kind, name));
            }
        }

        for (i, (ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(ino, (i + 1) as i64, kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn readdirplus(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectoryPlus,
    ) {
        let rel = match self.rel_for(ino) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let mut entries = Vec::new();
        entries.push((ino, FileType::Directory, ".".to_string(), rel.clone()));
        let parent_rel = rel.parent().unwrap_or(Path::new("")).to_path_buf();
        let parent_ino = self.get_or_insert_ino(&parent_rel);
        entries.push((
            parent_ino,
            FileType::Directory,
            "..".to_string(),
            parent_rel,
        ));

        let names = match self.visible_dir_entries(&rel) {
            Ok(n) => n,
            Err(err) => {
                reply.error(Self::err_code(err));
                return;
            }
        };

        for name in names {
            let child_rel = if rel.as_os_str().is_empty() {
                PathBuf::from(&name)
            } else {
                rel.join(&name)
            };
            if let Some((attr, _)) = self.stat_path(&child_rel) {
                entries.push((attr.ino, attr.kind, name, child_rel));
            }
        }

        for (i, (ino, _kind, name, child_rel)) in
            entries.into_iter().enumerate().skip(offset as usize)
        {
            if let Some((attr, _)) = self.stat_path(&child_rel) {
                if reply.add(ino, (i + 1) as i64, name, &TTL, &attr, GENERATION) {
                    break;
                }
            }
        }
        reply.ok();
    }

    fn create(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        let rel = match self.child_rel(parent, name) {
            Some(r) => r,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        if let Err(err) = self.clear_whiteout(&rel) {
            reply.error(Self::err_code(err));
            return;
        }
        if self.no_wal && Self::is_pg_wal_child(&rel) {
            self.wal_state_set_present(&rel, 0, mode);
            let _ino = self.get_or_insert_ino(&rel);
            let fh = self.alloc_fh();
            self.insert_handle(
                fh,
                OpenHandle {
                    file: None,
                    from_diff: false,
                },
            );
            match self.stat_path(&rel) {
                Some((attr, _)) => reply.created(&TTL, &attr, GENERATION, fh, 0),
                None => reply.error(ENOENT),
            }
        } else {
            if let Err(err) = self.ensure_parent_dirs(&rel) {
                reply.error(Self::err_code(err));
                return;
            }

            let path = self.overlay.diff_root().join(&rel);
            let mut opts = OpenOptions::new();
            opts.create(true).write(true).read(true).truncate(true);
            match opts.open(&path) {
                Ok(file) => {
                    let _ = file.set_permissions(PermissionsExt::from_mode(mode & 0o777));
                    let _ino = self.get_or_insert_ino(&rel);
                    match self.stat_path(&rel) {
                        Some((attr, _)) => {
                            // Allocate file handle and register it (like open() does)
                            let fh = self.alloc_fh();
                            self.insert_handle(
                                fh,
                                OpenHandle {
                                    file: Some(Arc::new(file)),
                                    from_diff: true,
                                },
                            );
                            reply.created(&TTL, &attr, GENERATION, fh, 0);
                        }
                        None => reply.error(ENOENT),
                    }
                }
                Err(err) => reply.error(Self::err_code(err)),
            }
        }
    }

    fn mknod(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        // Only regular files are supported for now.
        let file_type = mode & libc::S_IFMT;
        if !(file_type == 0 || file_type == libc::S_IFREG) {
            reply.error(EACCES);
            return;
        }

        let rel = match self.child_rel(parent, name) {
            Some(r) => r,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        if let Err(err) = self.clear_whiteout(&rel) {
            reply.error(Self::err_code(err));
            return;
        }
        if self.no_wal && Self::is_pg_wal_child(&rel) {
            self.wal_state_set_present(&rel, 0, mode);
            match self.stat_path(&rel) {
                Some((attr, _)) => reply.entry(&TTL, &attr, GENERATION),
                None => reply.error(ENOENT),
            }
            return;
        }

        if let Err(err) = self.ensure_parent_dirs(&rel) {
            reply.error(Self::err_code(err));
            return;
        }

        let path = self.overlay.diff_root().join(&rel);
        match File::create(&path) {
            Ok(file) => {
                let _ = file.set_permissions(PermissionsExt::from_mode(mode & 0o777));
                match self.stat_path(&rel) {
                    Some((attr, _)) => reply.entry(&TTL, &attr, GENERATION),
                    None => reply.error(ENOENT),
                }
            }
            Err(err) => reply.error(Self::err_code(err)),
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let rel = match self.child_rel(parent, name) {
            Some(r) => r,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        if let Err(err) = self.clear_whiteout(&rel) {
            reply.error(Self::err_code(err));
            return;
        }
        if let Err(err) = self.ensure_parent_dirs(&rel) {
            reply.error(Self::err_code(err));
            return;
        }

        let path = self.overlay.diff_root().join(&rel);
        if path.exists() {
            reply.error(EEXIST);
            return;
        }

        match fs::create_dir(&path) {
            Ok(()) => {
                let _ = fs::set_permissions(&path, PermissionsExt::from_mode(mode & 0o777));
                match self.stat_path(&rel) {
                    Some((attr, _)) => reply.entry(&TTL, &attr, GENERATION),
                    None => reply.error(ENOENT),
                }
            }
            Err(err) => reply.error(Self::err_code(err)),
        }
    }

    fn unlink(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let rel = match self.child_rel(parent, name) {
            Some(r) => r,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        if self.is_whiteouted(&rel) {
            reply.ok();
            return;
        }

        // Wait for pending writes on this file before deleting
        if let Some(ino) = self.inodes.lock().unwrap().get(&rel).copied() {
            self.pending_ops.wait_barrier(ino);
        }

        if self.no_wal && Self::is_pg_wal_child(&rel) {
            self.wal_state_mark_deleted(&rel);
            reply.ok();
            return;
        }

        let diff_path = self.overlay.diff_root().join(&rel);
        if self.overlay.is_pg_datafile(&rel) {
            if let Err(err) = self.overlay.unlink_pg_datafile(&rel) {
                reply.error(Self::err_from_anyhow(err));
                return;
            }
            if self.path_exists_in_base(&rel) {
                if let Err(err) = self.create_whiteout(&rel) {
                    reply.error(Self::err_code(err));
                    return;
                }
            }
            self.invalidate_path_caches(&rel);
            reply.ok();
            return;
        }

        if diff_path.exists() {
            match fs::symlink_metadata(&diff_path) {
                Ok(meta) if meta.is_dir() => {
                    reply.error(EISDIR);
                    return;
                }
                Ok(_) => {
                    if let Err(err) = fs::remove_file(&diff_path) {
                        reply.error(Self::err_code(err));
                        return;
                    }
                }
                Err(err) => {
                    reply.error(Self::err_code(err));
                    return;
                }
            }
        }

        if self.path_exists_in_base(&rel) || diff_path.exists() {
            if let Err(err) = self.create_whiteout(&rel) {
                reply.error(Self::err_code(err));
                return;
            }
        }

        self.invalidate_path_caches(&rel);

        reply.ok();
    }

    fn rmdir(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        let rel = match self.child_rel(parent, name) {
            Some(r) => r,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        if self.is_whiteouted(&rel) {
            reply.ok();
            return;
        }

        // Wait for pending operations on this directory
        if let Some(ino) = self.inodes.lock().unwrap().get(&rel).copied() {
            self.pending_ops.wait_barrier(ino);
        }

        match self.visible_dir_entries(&rel) {
            Ok(entries) if !entries.is_empty() => {
                reply.error(ENOTEMPTY);
                return;
            }
            Ok(_) => {}
            Err(err) => {
                reply.error(Self::err_code(err));
                return;
            }
        }

        let diff_path = self.overlay.diff_root().join(&rel);
        if diff_path.exists() {
            if let Err(err) = fs::remove_dir(&diff_path) {
                reply.error(Self::err_code(err));
                return;
            }
        }

        if self.path_exists_in_base(&rel) || diff_path.exists() {
            if let Err(err) = self.create_whiteout(&rel) {
                reply.error(Self::err_code(err));
                return;
            }
        }

        reply.ok();
    }

    fn rename(
        &mut self,
        _req: &Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        let src_rel = match self.child_rel(parent, name) {
            Some(r) => r,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let dst_rel = match self.child_rel(newparent, newname) {
            Some(r) => r,
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        if self.is_whiteouted(&src_rel) {
            reply.error(ENOENT);
            return;
        }

        let is_src_wal = self.no_wal && Self::is_pg_wal_child(&src_rel);
        let is_dst_wal = self.no_wal && Self::is_pg_wal_child(&dst_rel);

        // Wait for pending writes on source and target before renaming
        {
            let inodes = self.inodes.lock().unwrap();
            if let Some(ino) = inodes.get(&src_rel).copied() {
                drop(inodes);
                self.pending_ops.wait_barrier(ino);
            }
        }
        {
            let inodes = self.inodes.lock().unwrap();
            if let Some(ino) = inodes.get(&dst_rel).copied() {
                drop(inodes);
                self.pending_ops.wait_barrier(ino);
            }
        }

        if self.stat_path(&src_rel).is_none() {
            reply.error(ENOENT);
            return;
        }

        if is_src_wal || is_dst_wal {
            let src_state = if is_src_wal {
                self.wal_state_get(&src_rel)
            } else {
                None
            };
            let base_len = src_state
                .as_ref()
                .map(|s| s.logical_size)
                .unwrap_or_else(|| self.wal_backing_len(&src_rel));

            if is_src_wal {
                self.wal_state_mark_deleted(&src_rel);
            }
            if is_dst_wal {
                let mode = src_state
                    .as_ref()
                    .map(|s| s.mode)
                    .unwrap_or_else(|| self.wal_default_mode(&dst_rel));
                self.wal_state_set_present(&dst_rel, base_len, mode);
            }
            self.move_inode(&src_rel, &dst_rel);
            reply.ok();
            return;
        }

        if let Err(err) = self.ensure_diff_copy(&src_rel) {
            reply.error(Self::err_code(err));
            return;
        }
        if let Err(err) = self.ensure_parent_dirs(&dst_rel) {
            reply.error(Self::err_code(err));
            return;
        }
        if let Err(err) = self.clear_whiteout(&dst_rel) {
            reply.error(Self::err_code(err));
            return;
        }

        if self.overlay.is_pg_datafile(&src_rel) {
            if let Err(err) = self.overlay.rename_pg_datafile(&src_rel, &dst_rel) {
                reply.error(Self::err_from_anyhow(err));
                return;
            }
            self.move_inode(&src_rel, &dst_rel);
            if self.path_exists_in_base(&src_rel) {
                if let Err(err) = self.create_whiteout(&src_rel) {
                    reply.error(Self::err_code(err));
                    return;
                }
            }
            self.invalidate_path_caches(&src_rel);
            self.invalidate_path_caches(&dst_rel);
            reply.ok();
            return;
        }

        let src_path = self.overlay.diff_root().join(&src_rel);
        let dst_path = self.overlay.diff_root().join(&dst_rel);

        if dst_path.exists() {
            match fs::symlink_metadata(&dst_path) {
                Ok(meta) if meta.is_dir() => {
                    if let Err(err) = fs::remove_dir_all(&dst_path) {
                        reply.error(Self::err_code(err));
                        return;
                    }
                }
                Ok(_) => {
                    if let Err(err) = fs::remove_file(&dst_path) {
                        reply.error(Self::err_code(err));
                        return;
                    }
                }
                Err(err) => {
                    reply.error(Self::err_code(err));
                    return;
                }
            }
        }

        if let Some(parent) = dst_path.parent() {
            if let Err(err) = fs::create_dir_all(parent) {
                reply.error(Self::err_code(err));
                return;
            }
        }

        match fs::rename(&src_path, &dst_path) {
            Ok(()) => {
                self.move_inode(&src_rel, &dst_rel);
                if self.path_exists_in_base(&src_rel) {
                    if let Err(err) = self.create_whiteout(&src_rel) {
                        reply.error(Self::err_code(err));
                        return;
                    }
                }
                self.invalidate_path_caches(&src_rel);
                self.invalidate_path_caches(&dst_rel);
                reply.ok();
            }
            Err(err) => reply.error(Self::err_code(err)),
        }
    }

    fn setattr(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let rel = match self.rel_for(ino) {
            Some(r) => r,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        if self.is_whiteouted(&rel) {
            reply.error(ENOENT);
            return;
        }

        let is_wal = self.no_wal && Self::is_pg_wal_child(&rel);

        // Wait for pending writes before modifying attributes
        self.pending_ops.wait_barrier(ino);

        if (size.is_some() || mode.is_some()) && self.stat_path(&rel).is_none() {
            reply.error(ENOENT);
            return;
        }

        if is_wal {
            self.wal_state_upsert(&rel, |existing| {
                let mut state = existing.unwrap_or(WalFileState {
                    logical_size: self.wal_backing_len(&rel),
                    mode: self.wal_default_mode(&rel),
                    present: true,
                });
                state.present = true;
                if let Some(target_size) = size {
                    state.logical_size = target_size;
                }
                if let Some(bits) = mode {
                    state.mode = bits & 0o777;
                }
                state
            });
            match self.stat_path(&rel) {
                Some((attr, _)) => reply.attr(&TTL, &attr),
                None => reply.error(ENOENT),
            }
            return;
        }

        if let Some(target_size) = size {
            tracing::debug!(
                path = %rel.display(),
                target_size,
                "setattr_truncate"
            );
            if self.overlay.is_pg_datafile(&rel) {
                if let Err(err) = self.overlay.truncate_pg_datafile(&rel, target_size) {
                    reply.error(Self::err_from_anyhow(err));
                    return;
                }
                self.invalidate_path_caches(&rel);
            } else {
                if let Err(err) = self.ensure_diff_copy(&rel) {
                    reply.error(Self::err_code(err));
                    return;
                }
                let path = self.overlay.diff_root().join(&rel);
                match OpenOptions::new().write(true).open(&path) {
                    Ok(file) => {
                        if let Err(err) = file.set_len(target_size) {
                            reply.error(Self::err_code(err));
                            return;
                        }
                        self.invalidate_path_caches(&rel);
                    }
                    Err(err) => {
                        reply.error(Self::err_code(err));
                        return;
                    }
                }
            }
        }

        if let Some(bits) = mode {
            if let Err(err) = self.ensure_diff_copy(&rel) {
                reply.error(Self::err_code(err));
                return;
            }
            let path = self.overlay.diff_root().join(&rel);
            if let Err(err) = fs::set_permissions(&path, PermissionsExt::from_mode(bits & 0o777)) {
                reply.error(Self::err_code(err));
                return;
            }
            self.invalidate_path_caches(&rel);
        }

        match self.stat_path(&rel) {
            Some((attr, _)) => reply.attr(&TTL, &attr),
            None => reply.error(ENOENT),
        }
    }

    fn flush(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _lock_owner: u64,
        reply: ReplyEmpty,
    ) {
        if self.rel_for(ino).is_none() {
            reply.error(ENOENT);
            return;
        }
        reply.ok();
    }

    fn fsync(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, datasync: bool, reply: ReplyEmpty) {
        let rel = match self.rel_for(ino) {
            Some(r) => r,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        if self.is_whiteouted(&rel) {
            reply.error(ENOENT);
            return;
        }

        if self.no_wal && Self::is_pg_wal_child(&rel) {
            let seq = self.pending_ops.increment(ino);
            self.pending_ops.wait_for_preceding(ino, seq);
            reply.ok();
            self.pending_ops.decrement(ino, seq);
            return;
        }

        // Get sequence number BEFORE submitting to worker pool
        let seq = self.pending_ops.increment(ino);

        let task = FsTask::Fsync {
            ino,
            seq,
            overlay: self.overlay.clone(),
            rel,
            datasync,
            reply,
        };
        self.worker_pool.submit(task);
    }

    fn fallocate(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _offset: i64,
        _length: i64,
        _mode: i32,
        reply: ReplyEmpty,
    ) {
        let rel = match self.rel_for(ino) {
            Some(r) => r,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        if self.is_whiteouted(&rel) {
            reply.error(ENOENT);
            return;
        }

        if self.no_wal && Self::is_pg_wal_child(&rel) {
            reply.ok();
            return;
        }

        if let Err(err) = self.ensure_diff_copy(&rel) {
            reply.error(Self::err_code(err));
            return;
        }
        reply.ok();
    }
}

/// Handle to a running mount; dropping it will not unmount automatically, so
/// callers should invoke `unmount` explicitly to clean up.
pub struct MountHandle {
    mountpoint: String,
    session: BackgroundSession,
}

impl std::fmt::Debug for MountHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MountHandle")
            .field("mountpoint", &self.mountpoint)
            .finish()
    }
}

impl MountHandle {
    pub fn unmount(self) {
        self.session.join();
    }
}

/// Spawn a background FUSE mount for the given overlay. This is best-effort to
/// avoid failing in environments without /dev/fuse; callers should treat errors
/// as non-fatal for now.
pub fn spawn_overlay<P: AsRef<Path>>(overlay: Overlay, mountpoint: P) -> Result<MountHandle> {
    let mountpoint = mountpoint.as_ref().to_string_lossy().to_string();
    let fs = OverlayFs::new(overlay.clone());
    let options = vec![MountOption::FSName("pbkfs".into())];
    let session = fuser::spawn_mount2(fs, &mountpoint, &options).or_else(|e| {
        if e.raw_os_error() == Some(libc::ENOSYS) {
            let fs_fallback = OverlayFs::new(overlay);
            #[allow(deprecated)]
            fuser::spawn_mount(fs_fallback, &mountpoint, &[])
        } else {
            Err(e)
        }
    })?;

    Ok(MountHandle {
        mountpoint,
        session,
    })
}
