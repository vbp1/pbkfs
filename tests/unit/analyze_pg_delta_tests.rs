use std::fs;
use std::process::Command;

use tempfile::tempdir;

#[test]
fn analyze_pg_delta_rejects_unsupported_patch_version() {
    let dir = tempdir().unwrap();
    let base_path = dir.path().join("base");
    let patch_path = dir.path().join("rel.patch");
    let full_path = dir.path().join("rel.full");

    // base page of zeros
    fs::write(&base_path, vec![0u8; 8192]).unwrap();

    // Craft patch header with version=1 (unsupported) and correct magic/size fields.
    let mut hdr = vec![0u8; 512];
    hdr[..8].copy_from_slice(b"PBKPATCH");
    hdr[8..10].copy_from_slice(&1u16.to_le_bytes());
    hdr[12..16].copy_from_slice(&(8192u32).to_le_bytes());
    hdr[16..20].copy_from_slice(&(512u32).to_le_bytes());
    fs::write(&patch_path, &hdr).unwrap();

    // Minimal full file header (contents ignored by script for this test).
    let mut full_hdr = vec![0u8; 4096];
    full_hdr[..8].copy_from_slice(b"PBKFULL\0");
    full_hdr[8..10].copy_from_slice(&1u16.to_le_bytes());
    full_hdr[12..16].copy_from_slice(&(8192u32).to_le_bytes());
    fs::write(&full_path, &full_hdr).unwrap();

    let status = Command::new("python3")
        .arg("utils/analyze_pg_delta.py")
        .arg("--base")
        .arg(&base_path)
        .arg("--patch")
        .arg(&patch_path)
        .arg("--full")
        .arg(&full_path)
        .status()
        .expect("python3 must run");

    assert!(
        !status.success(),
        "script should exit non-zero for unsupported patch version"
    );
}
