//! FUSE adapter that projects a read-only base (`pbk_store`) with a
//! copy-on-write diff (`pbk_diff`).

use std::{
    collections::{HashMap, HashSet},
    ffi::OsStr,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::Mutex,
};

use fuser::{
    BackgroundSession, FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEntry, ReplyOpen, ReplyWrite, Request,
};
use libc::{EIO, ENOENT};

use crate::fs::overlay::Overlay;
use crate::Result;

pub struct OverlayFs {
    overlay: Overlay,
    paths: Mutex<HashMap<u64, PathBuf>>,   // ino -> rel path
    inodes: Mutex<HashMap<PathBuf, u64>>,  // rel path -> ino
    next_ino: Mutex<u64>,
}

impl OverlayFs {
    pub fn new(overlay: Overlay) -> Self {
        let mut paths = HashMap::new();
        let mut inodes = HashMap::new();
        paths.insert(1, PathBuf::from(""));
        inodes.insert(PathBuf::from(""), 1);
        Self {
            overlay,
            paths: Mutex::new(paths),
            inodes: Mutex::new(inodes),
            next_ino: Mutex::new(2),
        }
    }

    fn rel_for(&self, ino: u64) -> Option<PathBuf> {
        self.paths.lock().unwrap().get(&ino).cloned()
    }

    fn get_or_insert_ino(&self, rel: &Path) -> u64 {
        if let Some(id) = self.inodes.lock().unwrap().get(rel).copied() {
            return id;
        }
        let mut next = self.next_ino.lock().unwrap();
        let ino = *next;
        *next += 1;
        self.paths.lock().unwrap().insert(ino, rel.to_path_buf());
        self.inodes.lock().unwrap().insert(rel.to_path_buf(), ino);
        ino
    }

    fn stat_path(&self, rel: &Path) -> Option<(FileAttr, bool)> {
        // Prefer diff over base
        let (meta, from_diff) = match std::fs::symlink_metadata(self.overlay.diff_root().join(rel)) {
            Ok(m) => (m, true),
            Err(_) => std::fs::symlink_metadata(self.overlay.base_root().join(rel)).ok().map(|m| (m, false))?,
        };

        let kind = if meta.is_dir() {
            FileType::Directory
        } else if meta.is_file() {
            FileType::RegularFile
        } else {
            FileType::Symlink
        };

        let attr = FileAttr {
            ino: self.get_or_insert_ino(rel),
            size: meta.len(),
            blocks: meta.blocks(),
            atime: meta.accessed().unwrap_or_else(|_| std::time::SystemTime::UNIX_EPOCH),
            mtime: meta.modified().unwrap_or_else(|_| std::time::SystemTime::UNIX_EPOCH),
            ctime: meta.created().unwrap_or_else(|_| std::time::SystemTime::UNIX_EPOCH),
            crtime: meta.created().unwrap_or_else(|_| std::time::SystemTime::UNIX_EPOCH),
            kind,
            perm: meta.mode() as u16,
            nlink: 1,
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
        let parent_path = match self.rel_for(parent) {
            Some(p) => p,
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let rel = if parent_path.as_os_str().is_empty() {
            PathBuf::from(name)
        } else {
            parent_path.join(name)
        };

        match self.stat_path(&rel) {
            Some((attr, _)) => reply.entry(&std::time::Duration::from_secs(1), &attr, 0),
            None => reply.error(ENOENT),
        }
    }

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        match self.rel_for(ino).and_then(|p| self.stat_path(&p)) {
            Some((attr, _)) => reply.attr(&std::time::Duration::from_secs(1), &attr),
            None => reply.error(ENOENT),
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        if self.rel_for(ino).is_none() {
            reply.error(ENOENT);
            return;
        }
        reply.opened(ino, 0);
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
        if fh != ino {
            reply.error(EIO);
            return;
        }

        match self.overlay.read(&rel) {
            Ok(Some(bytes)) => {
                let start = offset.max(0) as usize;
                let end = bytes.len().min(start + size as usize);
                reply.data(&bytes[start..end]);
            }
            Ok(None) => reply.error(ENOENT),
            Err(_) => reply.error(EIO),
        }
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
        if fh != ino {
            reply.error(EIO);
            return;
        }

        let result = if offset == 0 {
            self.overlay.write(&rel, data)
        } else {
            match self.overlay.read(&rel) {
                Ok(Some(mut bytes)) => {
                    let start = offset as usize;
                    if bytes.len() < start {
                        bytes.resize(start, 0);
                    }
                    if bytes.len() < start + data.len() {
                        bytes.resize(start + data.len(), 0);
                    }
                    bytes[start..start + data.len()].copy_from_slice(data);
                    self.overlay.write(&rel, &bytes)
                }
                _ => self.overlay.write(&rel, data),
            }
        };

        match result {
            Ok(()) => reply.written(data.len() as u32),
            Err(_) => reply.error(EIO),
        }
    }

    fn readdir(&mut self, _req: &Request<'_>, ino: u64, _fh: u64, offset: i64, mut reply: ReplyDirectory) {
        if offset != 0 {
            reply.ok();
            return;
        }
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

        let mut names = HashSet::new();
        let diff_dir = self.overlay.diff_root().join(&rel);
        if let Ok(read_dir) = std::fs::read_dir(&diff_dir) {
            for entry in read_dir.flatten() {
                names.insert(entry.file_name().to_string_lossy().to_string());
            }
        }
        let base_dir = self.overlay.base_root().join(&rel);
        if let Ok(read_dir) = std::fs::read_dir(&base_dir) {
            for entry in read_dir.flatten() {
                names.insert(entry.file_name().to_string_lossy().to_string());
            }
        }

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

        for (i, (ino, kind, name)) in entries.into_iter().enumerate() {
            if reply.add(ino, (i + 1) as i64, kind, name) {
                break;
            }
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
    match fuser::spawn_mount2(fs, &mountpoint, &options) {
        Ok(session) => Ok(MountHandle { mountpoint, session }),
        Err(e) => {
            // Fallback to legacy spawn_mount for environments where spawn_mount2 isn't supported
            // (older fusermount/fuse). Keep the error if fallback also fails.
            if let Some(code) = e.raw_os_error() {
                if code != libc::ENOSYS && code != libc::EPERM && code != libc::EACCES {
                    return Err(e.into());
                }
            }

            let fs_fallback = OverlayFs::new(overlay);
            let opt = std::ffi::OsString::from("fsname=pbkfs");
            let args: [&std::ffi::OsStr; 2] = [std::ffi::OsStr::new("-o"), opt.as_os_str()];
            #[allow(deprecated)]
            let session = fuser::spawn_mount(fs_fallback, &mountpoint, &args)?;
            Ok(MountHandle { mountpoint, session })
        }
    }
}
