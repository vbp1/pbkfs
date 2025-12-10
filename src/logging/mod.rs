//! Logging initialization using `tracing` and `tracing-subscriber`.

use tracing::{info, warn};
use tracing_subscriber::{fmt, util::SubscriberInitExt, EnvFilter};

use crate::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogFormat {
    #[default]
    Human,
    Json,
}

/// Snapshot of filesystem worker-pool health, emitted periodically from the
/// FUSE adapter so saturation and latency regressions are visible in logs.
#[derive(Debug, Clone, Copy, Default)]
pub struct FsWorkerPoolSnapshot {
    pub queue_depth: usize,
    pub max_queue_depth: usize,
    pub tasks_total: u64,
    pub tasks_failed: u64,
    pub rejected: u64,
    pub last_task_latency_us: u64,
}

/// Snapshot of overlay/cache/handle state for observability.
#[derive(Debug, Clone, Copy, Default)]
pub struct OverlayIoSnapshot {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub handle_hits: u64,
    pub handle_misses: u64,
    pub handles_open: usize,
    pub delta_patch_count: u64,
    pub delta_full_count: u64,
    pub delta_patch_avg_size: u64,
    pub delta_patch_max_size: u64,
    pub delta_bitmaps_loaded: u64,
    pub delta_bitmaps_total_bytes: u64,
    pub delta_punch_holes: u64,
    pub delta_punch_hole_failures: u64,
}

/// Initialize global tracing subscriber. Safe to call multiple times; subsequent
/// calls will no-op.
pub fn init_logging(format: LogFormat) -> Result<()> {
    if tracing::dispatcher::has_been_set() {
        return Ok(());
    }

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = fmt::Subscriber::builder()
        .with_env_filter(filter)
        .with_target(false);

    match format {
        LogFormat::Human => {
            let _ = builder.finish().try_init();
        }
        LogFormat::Json => {
            let _ = builder.json().finish().try_init();
        }
    };

    Ok(())
}

/// Emit structured metrics about the FUSE worker pool. Callers should pass in
/// a periodically sampled snapshot to avoid excessive log volume.
pub fn log_fs_worker_pool_metrics(snapshot: FsWorkerPoolSnapshot, level_warn: bool) {
    if level_warn {
        warn!(
            target = "pbkfs::fs_worker",
            queue_depth = snapshot.queue_depth,
            max_queue_depth = snapshot.max_queue_depth,
            tasks_total = snapshot.tasks_total,
            tasks_failed = snapshot.tasks_failed,
            rejected = snapshot.rejected,
            last_task_latency_us = snapshot.last_task_latency_us,
            "fs_worker_pool_rejected_submission"
        );
    } else {
        info!(
            target = "pbkfs::fs_worker",
            queue_depth = snapshot.queue_depth,
            max_queue_depth = snapshot.max_queue_depth,
            tasks_total = snapshot.tasks_total,
            tasks_failed = snapshot.tasks_failed,
            rejected = snapshot.rejected,
            last_task_latency_us = snapshot.last_task_latency_us,
            "fs_worker_pool_snapshot"
        );
    }
}

/// Emit overlay handle/cache metrics to aid debugging of hot paths.
pub fn log_overlay_io_metrics(snapshot: OverlayIoSnapshot) {
    info!(
        target = "pbkfs::overlay",
        cache_hits = snapshot.cache_hits,
        cache_misses = snapshot.cache_misses,
        handle_hits = snapshot.handle_hits,
        handle_misses = snapshot.handle_misses,
        handles_open = snapshot.handles_open,
        delta_patch_count = snapshot.delta_patch_count,
        delta_full_count = snapshot.delta_full_count,
        delta_patch_avg_size = snapshot.delta_patch_avg_size,
        delta_patch_max_size = snapshot.delta_patch_max_size,
        delta_bitmaps_loaded = snapshot.delta_bitmaps_loaded,
        delta_bitmaps_total_bytes = snapshot.delta_bitmaps_total_bytes,
        delta_punch_holes = snapshot.delta_punch_holes,
        delta_punch_hole_failures = snapshot.delta_punch_hole_failures,
        "overlay_io_metrics"
    );
}
