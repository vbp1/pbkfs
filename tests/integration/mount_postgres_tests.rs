//! Integration-style smoke test that exercises the mount path without
//! requiring a real PostgreSQL instance. It validates that pbkfs:
//! - loads backup metadata,
//! - resolves a chain,
//! - writes binding metadata to the diff directory, and
//! - keeps the base backup immutable while writes land in the diff.

use std::{fs, path::Path};

use pbkfs::{cli::mount::MountArgs, cli::unmount};
use tempfile::tempdir;

fn write_metadata(store: &Path, compressed: bool, compression: Option<(&str, Option<u8>)>) {
    let metadata = serde_json::json!({
        "instance_name": "main",
        "version_pg_probackup": "2.6.0",
        "backups": [
            {
                "backup_id": "FULL1",
                "instance_name": "main",
                "backup_type": "Full",
                "parent_id": null,
                "start_time": "2024-01-01T00:00:00Z",
                "status": "Ok",
                "compressed": compressed,
                "compression": compression.map(|(alg, level)| serde_json::json!({
                    "algorithm": alg,
                    "level": level
                })),
                "size_bytes": 1024u64,
                "checksum_state": "Verified"
            }
        ]
    });
    fs::write(
        store.join("backups.json"),
        serde_json::to_vec_pretty(&metadata).unwrap(),
    )
    .unwrap();
}

#[test]
fn mounts_backup_and_preserves_store_immutability() -> pbkfs::Result<()> {
    let store = tempdir()?;
    let target = tempdir()?;
    let diff = tempdir()?;

    // Base backup content lives under the store path
    let base_file = store.path().join("FULL1").join("data/base.txt");
    fs::create_dir_all(base_file.parent().unwrap())?;
    fs::write(&base_file, b"from-store")?;
    write_metadata(store.path(), false, None);

    let args = MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("FULL1".into()),
    };

    let ctx = pbkfs::cli::mount::mount(args)?;

    // Reads come from base
    let contents = ctx
        .overlay
        .read(Path::new("data/base.txt"))?
        .expect("base file should be visible");
    assert_eq!(b"from-store", contents.as_slice());

    // Writes go to diff and do not mutate store
    ctx.overlay
        .write(Path::new("data/base.txt"), b"from-diff")?;

    let reread = ctx
        .overlay
        .read(Path::new("data/base.txt"))?
        .expect("diff value should be returned");
    assert_eq!(b"from-diff", reread.as_slice());

    let base_after = fs::read(base_file)?;
    assert_eq!(b"from-store", base_after.as_slice());

    // Binding metadata should have been persisted
    let binding_path = diff.path().join(pbkfs::binding::BINDING_FILE);
    assert!(binding_path.exists());

    // Unmount removes the lock marker
    unmount::execute(unmount::UnmountArgs {
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
    })?;

    assert!(!diff.path().join(pbkfs::binding::LOCK_FILE).exists());

    Ok(())
}

#[test]
fn mount_decompresses_compressed_backup_files() -> pbkfs::Result<()> {
    let store = tempdir()?;
    let target = tempdir()?;
    let diff = tempdir()?;

    // Create compressed base content in the backup store
    let base_file = store.path().join("FULL1").join("data/compressed.dat");
    fs::create_dir_all(base_file.parent().unwrap())?;
    let payload = b"postgres-compressed-data";
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::new(6));
    use std::io::Write;
    encoder.write_all(payload)?;
    let compressed = encoder.finish()?;
    fs::write(&base_file, compressed)?;

    write_metadata(store.path(), true, Some(("zlib", Some(6))));

    let args = MountArgs {
        pbk_store: Some(store.path().to_path_buf()),
        mnt_path: Some(target.path().to_path_buf()),
        diff_dir: Some(diff.path().to_path_buf()),
        instance: Some("main".into()),
        backup_id: Some("FULL1".into()),
    };

    let ctx = pbkfs::cli::mount::mount(args)?;

    let contents = ctx
        .overlay
        .read(Path::new("data/compressed.dat"))?
        .expect("decompressed view should exist");
    assert_eq!(payload.as_slice(), contents.as_slice());

    // Base remains compressed bytes
    let base_after = fs::read(&base_file)?;
    assert_ne!(payload.as_slice(), base_after.as_slice());

    // Diff holds a decompressed copy
    let diff_path = diff
        .path()
        .join("data")
        .join(Path::new("data/compressed.dat"));
    assert!(diff_path.exists());
    let diff_bytes = fs::read(diff_path)?;
    assert_eq!(payload.as_slice(), diff_bytes.as_slice());

    Ok(())
}
