use std::fs::File;

use pbkfs::binding::lock::{BindingRecord, BindingState, DiffDir};
use tempfile::tempdir;

#[test]
fn writes_and_reads_binding_record() -> pbkfs::Result<()> {
    let diff_root = tempdir()?;
    let diff = DiffDir::new(diff_root.path())?;
    diff.ensure_writable()?;

    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "localhost".to_string());
    let mut record = BindingRecord::new("main", "FULL1", "/pbk_store", "/pbk_target", std::process::id() as i32, host, "0.1.0");
    record.mark_stale();
    record.touch();
    record.write_to_diff(&diff)?;

    let loaded = BindingRecord::load_from_diff(&diff)?;
    assert_eq!(record.binding_id, loaded.binding_id);
    assert_eq!(BindingState::Stale, loaded.state);
    assert_eq!("FULL1", loaded.backup_id);

    Ok(())
}

#[test]
fn detects_non_writable_diff_dir() {
    let tmp = tempdir().unwrap();
    let file_path = tmp.path().join("not_writable");
    File::create(&file_path).unwrap();

    let diff = DiffDir::new(&file_path).unwrap();
    assert!(diff.ensure_writable().is_err());
}
