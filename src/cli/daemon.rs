//! Helpers for daemonization and launcher↔worker handshakes (Phase 8).

use std::{
    io,
    os::unix::io::RawFd,
};

pub const STATUS_STARTED: u8 = 0x01;
pub const STATUS_OK: u8 = 0x00;
pub const STATUS_ERR: u8 = 0xFF;

#[derive(Debug)]
pub struct Pipe {
    pub read_fd: RawFd,
    pub write_fd: RawFd,
}

impl Pipe {
    pub fn new() -> io::Result<Self> {
        let mut fds = [0i32; 2];
        let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
        if rc != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            read_fd: fds[0],
            write_fd: fds[1],
        })
    }

    pub fn close_read(&self) {
        let _ = unsafe { libc::close(self.read_fd) };
    }

    pub fn close_write(&self) {
        let _ = unsafe { libc::close(self.write_fd) };
    }
}

#[derive(Debug, Clone, Copy)]
pub struct HandshakeWriter {
    fd: RawFd,
}

impl HandshakeWriter {
    pub fn new(fd: RawFd) -> Self {
        Self { fd }
    }

    pub fn started(&self) -> io::Result<()> {
        self.write_code(STATUS_STARTED)
    }

    pub fn mount_ok(&self) -> io::Result<()> {
        self.write_code(STATUS_OK)
    }

    pub fn mount_error(&self, msg: &str) -> io::Result<()> {
        self.write_code(STATUS_ERR)?;
        if !msg.is_empty() {
            self.write_all(msg.as_bytes())?;
        }
        Ok(())
    }

    fn write_code(&self, code: u8) -> io::Result<()> {
        self.write_all(&[code])
    }

    fn write_all(&self, mut buf: &[u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let rc = unsafe { libc::write(self.fd, buf.as_ptr() as *const libc::c_void, buf.len()) };
            if rc < 0 {
                let err = io::Error::last_os_error();
                if err.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(err);
            }
            let written = rc as usize;
            buf = &buf[written..];
        }
        Ok(())
    }
}

pub fn daemonize() -> io::Result<()> {
    // Become session leader.
    let sid = unsafe { libc::setsid() };
    if sid < 0 {
        return Err(io::Error::last_os_error());
    }

    // Ignore SIGHUP so logout doesn't kill the worker.
    unsafe {
        libc::signal(libc::SIGHUP, libc::SIG_IGN);
    }

    // Redirect stdio to /dev/null.
    let devnull = unsafe { libc::open(b"/dev/null\0".as_ptr() as *const _, libc::O_RDWR) };
    if devnull < 0 {
        return Err(io::Error::last_os_error());
    }
    for fd in [libc::STDIN_FILENO, libc::STDOUT_FILENO, libc::STDERR_FILENO] {
        if unsafe { libc::dup2(devnull, fd) } < 0 {
            let err = io::Error::last_os_error();
            let _ = unsafe { libc::close(devnull) };
            return Err(err);
        }
    }
    let _ = unsafe { libc::close(devnull) };

    Ok(())
}
