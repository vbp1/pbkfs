//! Implementation of `pbkfs mount` subcommand.

use std::{fs, path::PathBuf};

use clap::Args;
use serde_json;
use tracing::{info, instrument};

use crate::{
    backup::{chain::BackupChain, metadata::BackupStore},
    binding::{BindingRecord, DiffDir, LockMarker, LOCK_FILE},
    fs::{fuse, overlay::Overlay, MountSession, MountSessionState, MountTarget},
    Error, Result,
};

#[derive(Debug, Clone, Args)]
pub struct MountArgs {
    /// Path to pg_probackup backup store (pbk_store)
    #[arg(short = 'B', long = "pbk-store")]
    pub pbk_store: Option<PathBuf>,

    /// Path to empty mount target directory (pbk_target)
    #[arg(long = "mnt-path")]
    pub mnt_path: Option<PathBuf>,

    /// Path to writable diff directory (pbk_diff)
    #[arg(long = "diff-dir")]
    pub diff_dir: Option<PathBuf>,

    /// pg_probackup instance name
    #[arg(long = "instance")]
    pub instance: Option<String>,

    /// Target backup id
    #[arg(short = 'i', long = "backup-id")]
    pub backup_id: Option<String>,
}

#[derive(Debug)]
pub struct MountContext {
    pub store: BackupStore,
    pub chain: BackupChain,
    pub diff_dir: DiffDir,
    pub binding: BindingRecord,
    pub overlay: Overlay,
    pub session: MountSession,
    pub fuse_handle: Option<fuse::MountHandle>,
}

pub fn execute(args: MountArgs) -> Result<()> {
    // Execute the mount and drop the context; CLI callers only need the side effects.
    let _ctx = mount(args)?;
    Ok(())
}

/// Perform mount orchestration used by both the CLI and tests.
#[instrument(skip(args), fields(mnt = ?args.mnt_path, diff = ?args.diff_dir, store = ?args.pbk_store))]
pub fn mount(args: MountArgs) -> Result<MountContext> {
    let pbk_store = args
        .pbk_store
        .ok_or_else(|| Error::Cli("pbk_store is required".into()))?;
    let mnt_path = args
        .mnt_path
        .ok_or_else(|| Error::Cli("mnt_path is required".into()))?;
    let diff_dir_path = args
        .diff_dir
        .ok_or_else(|| Error::Cli("diff_dir is required".into()))?;
    let instance = args
        .instance
        .ok_or_else(|| Error::Cli("instance is required".into()))?;
    let backup_id = args
        .backup_id
        .ok_or_else(|| Error::Cli("backup_id is required".into()))?;

    let target = MountTarget::new(&mnt_path);
    target.validate_empty()?;
    info!("validated target directory");

    fs::create_dir_all(&diff_dir_path)?;
    let diff_dir = DiffDir::new(&diff_dir_path)?;
    diff_dir.ensure_writable()?;
    info!(diff = %diff_dir.path.display(), "diff directory prepared");

    let store = BackupStore::load_from_pg_probackup(&pbk_store, &instance)?;
    let chain = BackupChain::from_target_backup(&store, &backup_id)?;
    info!(backup=?backup_id, instance=?instance, "backup chain resolved");

    let binding = BindingRecord::new(
        instance,
        backup_id,
        pbk_store,
        &mnt_path,
        std::process::id() as i32,
        std::env::var("HOSTNAME").unwrap_or_else(|_| "unknown".into()),
        env!("CARGO_PKG_VERSION"),
    );
    binding.write_to_diff(&diff_dir)?;
    info!(binding_id=%binding.binding_id, "binding persisted to diff");

    let overlay = Overlay::new(&store.path, &diff_dir.path)?;

    // Persist lock markers before mounting so writes hit the real FS, not the FUSE layer.
    let mut session = MountSession::new(binding.binding_id, &mnt_path);
    let marker = LockMarker {
        mount_id: session.mount_id,
        diff_dir: diff_dir.path.clone(),
    };
    let marker_bytes = serde_json::to_vec_pretty(&marker)?;
    fs::write(diff_dir.path.join(LOCK_FILE), &marker_bytes)?;
    // Do not write into the mount target; it will be shadowed by the FUSE mount.

    // Start FUSE session; if it fails, surface the error.
    let fuse_handle = Some(fuse::spawn_overlay(overlay.clone(), &mnt_path)?);

    session.state = MountSessionState::Ready;
    info!(mount_id=%session.mount_id, "mount ready");

    Ok(MountContext {
        store,
        chain,
        diff_dir,
        binding,
        overlay,
        session,
        fuse_handle,
    })
}
