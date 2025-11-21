//! FUSE adapter that projects a read-only base (`pbk_store`) with a
//! copy-on-write diff (`pbk_diff`).

use std::{
    collections::{HashMap, HashSet},
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io,
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Mutex,
    time::Duration,
};

use anyhow::Error as AnyhowError;
use fuser::{
    BackgroundSession, FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate,
    ReplyData, ReplyDirectory, ReplyDirectoryPlus, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite,
    Request,
};
use libc::{EACCES, EEXIST, EIO, EISDIR, ENOENT, ENOTEMPTY};

use crate::fs::overlay::Overlay;
use crate::Result;

const TTL: Duration = Duration::from_secs(1);
const GENERATION: u64 = 0;

pub struct OverlayFs {
    overlay: Overlay,
    paths: Mutex<HashMap<u64, PathBuf>>,  // ino -> rel path
    inodes: Mutex<HashMap<PathBuf, u64>>, // rel path -> ino
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
        self.overlay
            .ensure_copy_up(rel)
            .map_err(|err| io::Error::from_raw_os_error(Self::err_from_anyhow(err)))
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

        for (root, _) in self.overlay.layer_roots() {
            let base_dir = root.join(rel);
            if let Ok(read_dir) = fs::read_dir(&base_dir) {
                for entry in read_dir.flatten() {
                    let name = entry.file_name().to_string_lossy().to_string();
                    let child_rel = if rel.as_os_str().is_empty() {
                        PathBuf::from(&name)
                    } else {
                        rel.join(&name)
                    };
                    if self.is_whiteouted(&child_rel) {
                        continue;
                    }
                    names.insert(name);
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

        // Prefer diff over base
        let (meta, from_diff) = match fs::symlink_metadata(self.overlay.diff_root().join(rel)) {
            Ok(m) => (m, true),
            Err(_) => {
                let (base_path, _, _) = self.overlay.find_layer_path(rel)?;
                fs::symlink_metadata(base_path).ok().map(|m| (m, false))?
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

        let attr = FileAttr {
            ino: self.get_or_insert_ino(rel),
            size: meta.len(),
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

    fn getattr(&mut self, _req: &Request<'_>, ino: u64, reply: ReplyAttr) {
        match self.rel_for(ino).and_then(|p| self.stat_path(&p)) {
            Some((attr, _)) => reply.attr(&TTL, &attr),
            None => reply.error(ENOENT),
        }
    }

    fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
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
        if self.is_whiteouted(&rel) {
            reply.error(ENOENT);
            return;
        }
        if fh != ino {
            reply.error(EIO);
            return;
        }

        match self.overlay.read(&rel) {
            Ok(Some(bytes)) => {
                let start = offset.max(0) as usize;
                if start >= bytes.len() {
                    reply.data(&[]);
                    return;
                }
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
        if self.is_whiteouted(&rel) {
            reply.error(ENOENT);
            return;
        }
        if fh != ino {
            reply.error(EIO);
            return;
        }

        let mut bytes = match self.overlay.read(&rel) {
            Ok(Some(buf)) => buf,
            Ok(None) => Vec::new(),
            Err(_) => {
                reply.error(EIO);
                return;
            }
        };

        let start = offset.max(0) as usize;
        if bytes.len() < start {
            bytes.resize(start, 0);
        }
        if bytes.len() < start + data.len() {
            bytes.resize(start + data.len(), 0);
        }
        bytes[start..start + data.len()].copy_from_slice(data);

        match self.overlay.write(&rel, &bytes) {
            Ok(()) => reply.written(data.len() as u32),
            Err(err) => reply.error(Self::err_from_anyhow(err)),
        }
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
        flags: i32,
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
                let ino = self.get_or_insert_ino(&rel);
                match self.stat_path(&rel) {
                    Some((attr, _)) => reply.created(&TTL, &attr, GENERATION, ino, flags as u32),
                    None => reply.error(ENOENT),
                }
            }
            Err(err) => reply.error(Self::err_code(err)),
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

        let diff_path = self.overlay.diff_root().join(&rel);
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

        if self.stat_path(&src_rel).is_none() {
            reply.error(ENOENT);
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

        if (size.is_some() || mode.is_some()) && self.stat_path(&rel).is_none() {
            reply.error(ENOENT);
            return;
        }

        if let Some(target_size) = size {
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
                }
                Err(err) => {
                    reply.error(Self::err_code(err));
                    return;
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

    fn fsync(
        &mut self,
        _req: &Request<'_>,
        ino: u64,
        _fh: u64,
        _datasync: bool,
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

        let path = {
            let diff = self.overlay.diff_root().join(&rel);
            if diff.exists() {
                diff
            } else {
                match self.overlay.find_layer_path(&rel) {
                    Some((p, _, _)) => p,
                    None => {
                        reply.error(ENOENT);
                        return;
                    }
                }
            }
        };

        match File::open(&path) {
            Ok(file) => {
                if let Err(err) = file.sync_all() {
                    reply.error(Self::err_code(err));
                    return;
                }
                reply.ok();
            }
            Err(err) => reply.error(Self::err_code(err)),
        }
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
