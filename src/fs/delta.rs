//! Delta storage primitives for PostgreSQL datafiles.
//!
//! This module provides PATCH/FULL sparse file helpers and in-memory delta
//! computation/apply utilities. Integration with Overlay is done in Phase 12.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::os::fd::AsRawFd;
use std::sync::Arc;
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    os::unix::fs::FileExt,
    path::Path,
};

use parking_lot::RwLock;

use crate::{env_lock, Error, Result};

/// PostgreSQL page size used by pbkfs (matches `BLCKSZ`, 8 KiB).
pub const PAGE_SIZE: usize = 8192;
/// Fixed slot size in the `.patch` sparse file.
pub const PATCH_SLOT_SIZE: usize = 512;
/// Maximum payload bytes that fit into a single slot.
pub const PATCH_PAYLOAD_MAX: usize = PATCH_SLOT_SIZE - 8; // minus kind/flags/payload_len/reserved
const PATCH_MAGIC: &[u8; 8] = b"PBKPATCH";
const PATCH_VERSION: u16 = 1;
const FULL_MAGIC: &[u8; 8] = b"PBKFULL\0";
const FULL_VERSION: u16 = 1;
const FULL_HEADER_SIZE: u64 = 4096;

/// Slot kind, stored in the on-disk `.patch` header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotKind {
    Empty = 0,
    Patch = 1,
    FullRef = 2,
}

/// In-memory PATCH segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatchSegment {
    pub offset: u16,
    pub len: u16,
    pub data: Vec<u8>,
}

impl PatchSegment {
    fn serialized_len(&self) -> usize {
        4 + self.data.len()
    }
}

/// Result of comparing two pages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeltaDiff {
    Empty,
    Patch { segments: Vec<PatchSegment> },
    Full(Box<[u8; PAGE_SIZE]>),
}

impl DeltaDiff {
    /// Total bytes required to encode this delta payload.
    pub fn serialized_len(&self) -> usize {
        match self {
            DeltaDiff::Empty => 0,
            DeltaDiff::Patch { segments } => segments.iter().map(|s| s.serialized_len()).sum(),
            DeltaDiff::Full(_) => PAGE_SIZE,
        }
    }

    /// Serialize patch segments into an on-disk payload.
    pub fn to_patch_bytes(&self) -> Option<Vec<u8>> {
        match self {
            DeltaDiff::Patch { segments } => {
                let mut out = Vec::new();
                for seg in segments {
                    out.extend_from_slice(&seg.offset.to_le_bytes());
                    out.extend_from_slice(&seg.len.to_le_bytes());
                    out.extend_from_slice(&seg.data);
                }
                Some(out)
            }
            _ => None,
        }
    }

    /// Borrow the full page when this delta represents a FULL write.
    pub fn as_full(&self) -> Option<&[u8; PAGE_SIZE]> {
        match self {
            DeltaDiff::Full(ref page) => Some(page),
            _ => None,
        }
    }
}

/// Compute a delta between `base` and `new_page`.
///
/// Returns:
/// - `DeltaDiff::Empty` when pages are identical,
/// - `DeltaDiff::Patch` when changes fit into a single PATCH slot,
/// - `DeltaDiff::Full` when the patch would exceed the slot budget.
pub fn compute_delta(base: &[u8; PAGE_SIZE], new_page: &[u8; PAGE_SIZE]) -> DeltaDiff {
    if base == new_page {
        return DeltaDiff::Empty;
    }

    let mut segments = Vec::new();
    let mut idx = 0usize;

    while idx < PAGE_SIZE {
        if base[idx] == new_page[idx] {
            idx += 1;
            continue;
        }

        let start = idx;
        while idx < PAGE_SIZE && base[idx] != new_page[idx] {
            idx += 1;
        }
        let len = idx - start;

        segments.push(PatchSegment {
            offset: start as u16,
            len: len as u16,
            data: new_page[start..start + len].to_vec(),
        });
    }

    let patch_len: usize = segments.iter().map(|s| s.serialized_len()).sum();
    if patch_len == 0 {
        return DeltaDiff::Empty;
    }

    if patch_len <= PATCH_PAYLOAD_MAX {
        DeltaDiff::Patch { segments }
    } else {
        let mut full = [0u8; PAGE_SIZE];
        full.copy_from_slice(new_page);
        DeltaDiff::Full(Box::new(full))
    }
}

/// Apply a PATCH payload to `base`, returning the reconstructed page.
pub fn apply_patch(base: &[u8; PAGE_SIZE], patch_bytes: &[u8]) -> Result<[u8; PAGE_SIZE]> {
    if patch_bytes.is_empty() {
        return Err(Error::CorruptedPatch {
            reason: "empty patch payload".into(),
        }
        .into());
    }

    let mut out = *base;
    let mut cursor = 0usize;

    while cursor < patch_bytes.len() {
        if cursor + 4 > patch_bytes.len() {
            return Err(Error::CorruptedPatch {
                reason: "truncated patch segment header".into(),
            }
            .into());
        }

        let offset = u16::from_le_bytes([patch_bytes[cursor], patch_bytes[cursor + 1]]);
        let len = u16::from_le_bytes([patch_bytes[cursor + 2], patch_bytes[cursor + 3]]);
        cursor += 4;

        if len == 0 {
            return Err(Error::CorruptedPatch {
                reason: "zero-length patch segment".into(),
            }
            .into());
        }

        let data_end = cursor + len as usize;
        if data_end > patch_bytes.len() {
            return Err(Error::CorruptedPatch {
                reason: "segment overruns payload".into(),
            }
            .into());
        }

        let start = offset as usize;
        let end = start + len as usize;
        if end > PAGE_SIZE {
            return Err(Error::CorruptedPatch {
                reason: "segment overruns page boundary".into(),
            }
            .into());
        }

        out[start..end].copy_from_slice(&patch_bytes[cursor..data_end]);
        cursor = data_end;
    }

    Ok(out)
}

// Layout of the on-disk .patch header. Kept as a spec/for future mmap reads;
// today we parse/write fields manually to stay safe on endianness/alignment
// and avoid unsafe transmute.
#[allow(dead_code)]
#[repr(C)]
struct PatchFileHeader {
    magic: [u8; 8],
    version: u16,
    flags: u16,
    page_size: u32,
    slot_size: u32,
    _reserved: [u8; 492],
}

#[repr(C)]
struct SlotHeader {
    kind: u8,
    _flags: u8,
    payload_len: u16,
    _reserved: [u8; 4],
}

// Same idea for the .full header: documents the format and can be used later
// with a zerocopy/Pod approach if we decide to mmap instead of manual parsing.
#[allow(dead_code)]
#[repr(C)]
struct FullFileHeader {
    magic: [u8; 8],
    version: u16,
    flags: u16,
    page_size: u32,
    _reserved: [u8; 4080],
}

fn patch_offset(block_no: u64) -> u64 {
    PATCH_SLOT_SIZE as u64 + block_no * PATCH_SLOT_SIZE as u64
}

fn full_offset(block_no: u64) -> u64 {
    FULL_HEADER_SIZE + block_no * PAGE_SIZE as u64
}

/// Create or open a `.patch` file, validating header.
pub fn open_patch(path: &Path) -> Result<File> {
    if !path.exists() {
        create_patch(path)?;
    }
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let mut buf = [0u8; PATCH_SLOT_SIZE];
    if let Err(e) = file.read_exact(&mut buf) {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            return Err(Error::InvalidPatchFile {
                reason: "short header".into(),
            }
            .into());
        }
        return Err(e.into());
    }

    if &buf[..PATCH_MAGIC.len()] != PATCH_MAGIC {
        return Err(Error::InvalidPatchFile {
            reason: "bad magic".into(),
        }
        .into());
    }
    let version = u16::from_le_bytes([buf[8], buf[9]]);
    if version != PATCH_VERSION {
        return Err(Error::InvalidPatchFile {
            reason: format!("unsupported version {version}"),
        }
        .into());
    }
    let page_size = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
    if page_size as usize != PAGE_SIZE {
        return Err(Error::InvalidPatchFile {
            reason: format!("page_size {page_size} != {PAGE_SIZE}"),
        }
        .into());
    }
    let slot_size = u32::from_le_bytes([buf[16], buf[17], buf[18], buf[19]]);
    if slot_size as usize != PATCH_SLOT_SIZE {
        return Err(Error::InvalidPatchFile {
            reason: format!("slot_size {slot_size} != {PATCH_SLOT_SIZE}"),
        }
        .into());
    }
    Ok(file)
}

/// Create a new patch file with header.
pub fn create_patch(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .truncate(true)
        .open(path)?;

    let mut header = vec![0u8; PATCH_SLOT_SIZE];
    header[..PATCH_MAGIC.len()].copy_from_slice(PATCH_MAGIC);
    header[8..10].copy_from_slice(&PATCH_VERSION.to_le_bytes());
    header[12..16].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
    header[16..20].copy_from_slice(&(PATCH_SLOT_SIZE as u32).to_le_bytes());
    file.write_all(&header)?;
    file.flush()?;
    Ok(())
}

/// Read a single slot from `.patch`. Returns (kind, payload).
pub fn read_patch_slot(file: &File, block_no: u64) -> Result<(SlotKind, Vec<u8>)> {
    let mut hdr = [0u8; std::mem::size_of::<SlotHeader>()];
    let off = patch_offset(block_no);
    let read = file.read_at(&mut hdr, off)?;
    if read == 0 {
        return Ok((SlotKind::Empty, Vec::new()));
    }
    if read < hdr.len() {
        return Err(Error::InvalidPatchFile {
            reason: "short slot header".into(),
        }
        .into());
    }
    let kind = match hdr[0] {
        0 => SlotKind::Empty,
        1 => SlotKind::Patch,
        2 => SlotKind::FullRef,
        _ => {
            return Err(Error::InvalidPatchFile {
                reason: "unknown slot kind".into(),
            }
            .into())
        }
    };
    let payload_len = u16::from_le_bytes([hdr[2], hdr[3]]) as usize;
    if payload_len > PATCH_PAYLOAD_MAX {
        return Err(Error::InvalidPatchFile {
            reason: "payload too large".into(),
        }
        .into());
    }
    if payload_len > PATCH_SLOT_SIZE - 8 {
        return Err(Error::InvalidPatchFile {
            reason: format!("payload_len {payload_len} exceeds slot size"),
        }
        .into());
    }
    if matches!(kind, SlotKind::Patch) && payload_len == 0 {
        return Err(Error::InvalidPatchFile {
            reason: "zero-length payload in patch slot".into(),
        }
        .into());
    }
    if kind == SlotKind::Empty || payload_len == 0 {
        return Ok((kind, Vec::new()));
    }
    let mut buf = vec![0u8; payload_len];
    let got = file.read_at(&mut buf, off + hdr.len() as u64)?;
    if got != payload_len {
        return Err(Error::InvalidPatchFile {
            reason: "short payload".into(),
        }
        .into());
    }
    Ok((kind, buf))
}

/// Write a PATCH slot (payload must fit in one slot).
pub fn write_patch_slot(file: &File, block_no: u64, payload: &[u8]) -> Result<()> {
    if payload.len() > PATCH_PAYLOAD_MAX {
        return Err(Error::PatchTooLarge {
            len: payload.len(),
            max: PATCH_PAYLOAD_MAX,
        }
        .into());
    }
    let off = patch_offset(block_no);
    let mut buf = vec![0u8; PATCH_SLOT_SIZE];
    buf[0] = SlotKind::Patch as u8;
    buf[2..4].copy_from_slice(&(payload.len() as u16).to_le_bytes());
    buf[8..8 + payload.len()].copy_from_slice(payload);
    file.write_all_at(&buf, off)?;
    Ok(())
}

/// Write a FULL_REF slot to point to `.full`.
pub fn write_full_ref_slot(file: &File, block_no: u64) -> Result<()> {
    let off = patch_offset(block_no);
    let mut buf = [0u8; PATCH_SLOT_SIZE];
    buf[0] = SlotKind::FullRef as u8;
    file.write_all_at(&buf, off)?;
    Ok(())
}

/// Clear a slot to EMPTY (zeros).
pub fn write_empty_slot(file: &File, block_no: u64) -> Result<()> {
    let off = patch_offset(block_no);
    let buf = [0u8; PATCH_SLOT_SIZE];
    file.write_all_at(&buf, off)?;
    Ok(())
}

/// Create or open a `.full` file with validated header.
pub fn open_full(path: &Path) -> Result<File> {
    if !path.exists() {
        create_full(path)?;
    }
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let mut hdr = [0u8; FULL_HEADER_SIZE as usize];
    if let Err(e) = file.read_exact(&mut hdr) {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            return Err(Error::InvalidFullFile {
                reason: "short header".into(),
            }
            .into());
        }
        return Err(e.into());
    }
    if &hdr[..FULL_MAGIC.len()] != FULL_MAGIC {
        return Err(Error::InvalidFullFile {
            reason: "bad magic".into(),
        }
        .into());
    }
    let version = u16::from_le_bytes([hdr[8], hdr[9]]);
    if version != FULL_VERSION {
        return Err(Error::InvalidFullFile {
            reason: format!("unsupported version {version}"),
        }
        .into());
    }
    let page_size = u32::from_le_bytes([hdr[12], hdr[13], hdr[14], hdr[15]]);
    if page_size as usize != PAGE_SIZE {
        return Err(Error::InvalidFullFile {
            reason: format!("page_size {page_size} != {PAGE_SIZE}"),
        }
        .into());
    }
    Ok(file)
}

/// Create a new `.full` file with header.
pub fn create_full(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .truncate(true)
        .open(path)?;

    let mut header = vec![0u8; FULL_HEADER_SIZE as usize];
    header[..FULL_MAGIC.len()].copy_from_slice(FULL_MAGIC);
    header[8..10].copy_from_slice(&FULL_VERSION.to_le_bytes());
    header[12..16].copy_from_slice(&(PAGE_SIZE as u32).to_le_bytes());
    file.write_all(&header)?;
    file.flush()?;
    Ok(())
}

/// Read a FULL page from `.full`. Missing hole returns zero page.
pub fn read_full_page(file: &File, block_no: u64) -> Result<[u8; PAGE_SIZE]> {
    let mut buf = [0u8; PAGE_SIZE];
    let off = full_offset(block_no);
    let read = file.read_at(&mut buf, off)?;
    if read == 0 {
        // hole
        return Ok(buf);
    }
    if read < PAGE_SIZE {
        return Err(Error::InvalidFullFile {
            reason: "short page".into(),
        }
        .into());
    }
    Ok(buf)
}

/// Write a FULL page at deterministic offset.
pub fn write_full_page(file: &File, block_no: u64, page: &[u8]) -> Result<()> {
    if page.len() != PAGE_SIZE {
        return Err(Error::InvalidFullFile {
            reason: "page length mismatch".into(),
        }
        .into());
    }
    let off = full_offset(block_no);
    file.write_all_at(page, off)?;
    Ok(())
}

/// Punch a hole for a FULL block; ignored if unsupported.
pub fn punch_full_hole(file: &File, block_no: u64) -> io::Result<()> {
    let _env = env_lock().lock();
    if std::env::var("PBKFS_TEST_PUNCH_FAIL").is_ok() {
        return Err(io::Error::from_raw_os_error(libc::ENOTSUP));
    }
    let off = full_offset(block_no);
    let len = PAGE_SIZE as u64;
    #[allow(clippy::useless_conversion)]
    let res = unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            off as i64,
            len as i64,
        )
    };
    if res == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

/// Simple bitmap (2 bits per block).
#[derive(Debug, Default)]
pub struct BlockBitmap {
    bits: RwLock<Vec<u8>>,
}

impl BlockBitmap {
    fn ensure(&self, block: u64) {
        let mut guard = self.bits.write();
        let idx = (block as usize) >> 2; // 4 entries per byte (2 bits each)
        if idx >= guard.len() {
            guard.resize(idx + 1, 0);
        }
    }

    pub fn get(&self, block: u64) -> u8 {
        let guard = self.bits.read();
        let idx = (block as usize) >> 2;
        if idx >= guard.len() {
            return 0;
        }
        let shift = ((block as usize) & 0b11) * 2;
        (guard[idx] >> shift) & 0b11
    }

    pub fn set(&self, block: u64, val: u8) {
        self.ensure(block);
        let mut guard = self.bits.write();
        let idx = (block as usize) >> 2;
        let shift = ((block as usize) & 0b11) * 2;
        let mask = !(0b11 << shift);
        guard[idx] = (guard[idx] & mask) | ((val & 0b11) << shift);
    }

    pub fn len_bytes(&self) -> usize {
        self.bits.read().len()
    }
}

/// Thread-safe bitmap cache keyed by hashed file path.
#[derive(Default, Debug)]
pub struct DeltaIndex {
    bitmaps: RwLock<HashMap<u64, Arc<BlockBitmap>>>,
}

impl DeltaIndex {
    pub fn new() -> Self {
        Self {
            bitmaps: RwLock::new(HashMap::new()),
        }
    }

    pub fn invalidate(&self, path: &Path) {
        let key = hash_path(path);
        self.bitmaps.write().remove(&key);
    }

    /// Return cached bitmap if present; otherwise load from `.patch` and cache it.
    pub fn get_or_load_bitmap(&self, patch_path: &Path) -> Result<Arc<BlockBitmap>> {
        let key = hash_path(patch_path);
        if let Some(bm) = self.bitmaps.read().get(&key) {
            return Ok(Arc::clone(bm));
        }
        let mut write = self.bitmaps.write();
        if let Some(bm) = write.get(&key) {
            return Ok(Arc::clone(bm));
        }
        let loaded = Arc::new(load_bitmap_from_patch(patch_path)?);
        write.insert(key, Arc::clone(&loaded));
        Ok(loaded)
    }

    pub fn is_cached(&self, patch_path: &Path) -> bool {
        let key = hash_path(patch_path);
        self.bitmaps.read().contains_key(&key)
    }

    /// Return a cached bitmap if present, otherwise create and cache an empty one.
    pub fn get_or_create_empty(&self, patch_path: &Path) -> Arc<BlockBitmap> {
        let key = hash_path(patch_path);
        if let Some(bm) = self.bitmaps.read().get(&key) {
            return Arc::clone(bm);
        }
        let mut write = self.bitmaps.write();
        if let Some(bm) = write.get(&key) {
            return Arc::clone(bm);
        }
        let bm = Arc::new(BlockBitmap::default());
        write.insert(key, Arc::clone(&bm));
        bm
    }
}

pub fn hash_path(path: &Path) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    path.hash(&mut hasher);
    hasher.finish()
}

/// Build bitmap by scanning all slots in `.patch`.
pub fn load_bitmap_from_patch(path: &Path) -> Result<BlockBitmap> {
    if !path.exists() {
        let full_path = path.with_extension("full");
        if full_path.exists() {
            return Err(Error::InvalidPatchFile {
                reason: "found .full without .patch".into(),
            }
            .into());
        }
        return Ok(BlockBitmap::default());
    }

    let file = open_patch(path)?;
    let meta = file.metadata()?;
    if meta.len() < PATCH_SLOT_SIZE as u64 {
        return Err(Error::InvalidPatchFile {
            reason: "patch file shorter than header".into(),
        }
        .into());
    }

    let bm = BlockBitmap::default();
    let mut block: u64 = 0;
    let mut buf = [0u8; std::mem::size_of::<SlotHeader>()];
    loop {
        let off = patch_offset(block);
        if off >= meta.len() {
            break;
        }
        let read = file.read_at(&mut buf, off)?;
        if read == 0 {
            break; // sparse hole means remaining slots are empty
        }
        let kind = match buf[0] {
            0 => SlotKind::Empty,
            1 => SlotKind::Patch,
            2 => SlotKind::FullRef,
            other => {
                return Err(Error::InvalidPatchFile {
                    reason: format!("unknown slot kind {other} at block {block}"),
                }
                .into())
            }
        };
        match kind {
            SlotKind::Empty => {}
            SlotKind::Patch => bm.set(block, 0b10),
            SlotKind::FullRef => {
                let full_path = path.with_extension("full");
                if !full_path.exists() {
                    return Err(Error::InvalidPatchFile {
                        reason: format!("full file missing for block {block}"),
                    }
                    .into());
                }
                bm.set(block, 0b01)
            }
        }
        block += 1;
    }

    let full_path = path.with_extension("full");
    if full_path.exists() {
        let meta = full_path.metadata()?;
        if meta.len() < FULL_HEADER_SIZE {
            return Err(Error::InvalidFullFile {
                reason: "full file shorter than header".into(),
            }
            .into());
        }

        let payload_bytes = meta.len().saturating_sub(FULL_HEADER_SIZE);
        if payload_bytes > 0 {
            let max_block = (payload_bytes - 1) / PAGE_SIZE as u64;
            let full = open_full(&full_path)?;
            for blk in 0..=max_block {
                let page = read_full_page(&full, blk)?;
                let is_zero = page.iter().all(|b| *b == 0);
                if is_zero {
                    continue;
                }
                if bm.get(blk) != 0b01 {
                    return Err(Error::InvalidPatchFile {
                        reason: format!("full file has block {blk} without FULL_REF slot"),
                    }
                    .into());
                }
            }
        }
    }

    Ok(bm)
}
