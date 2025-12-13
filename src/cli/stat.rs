//! Implementation of `pbkfs stat` subcommand.

use std::path::PathBuf;

use clap::{Args, ValueEnum};
use serde_json;

use crate::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Default)]
pub enum StatFormat {
    #[default]
    Text,
    Json,
}

#[derive(Debug, Clone, Args, Default)]
pub struct StatArgs {
    /// Path to an existing pbkfs mount target
    #[arg(short = 'D', long = "mnt-path")]
    pub mnt_path: Option<PathBuf>,

    /// Output format for both the on-disk stat file and stdout.
    #[arg(long = "format", value_enum, default_value = "text")]
    pub format: StatFormat,

    /// Dump counters and reset them afterwards (SIGUSR2).
    #[arg(long = "counters-reset", default_value_t = false)]
    pub counters_reset: bool,
}

pub fn execute(args: StatArgs) -> Result<()> {
    crate::logging::init_logging(crate::logging::LoggingConfig {
        format: crate::logging::LogFormat::Human,
        sink: crate::logging::LogSink::Console,
        debug: false,
    })?;

    let mnt_path = args
        .mnt_path
        .ok_or_else(|| Error::Cli("mnt_path is required".into()))?;

    let pid_path = mnt_path.join(crate::cli::pid::DEFAULT_PID_FILE_REL);
    let stat_path = mnt_path.join(crate::cli::pid::DEFAULT_STAT_FILE_REL);

    let record = match crate::cli::pid::read_pid_record(&pid_path) {
        Ok(r) => r,
        Err(err) => {
            if let Some(io) = err.downcast_ref::<std::io::Error>() {
                if io.kind() == std::io::ErrorKind::NotFound {
                    return Err(Error::Cli("no active mount".into()).into());
                }
            }
            return Err(err);
        }
    };

    if !crate::cli::pid::pid_alive(record.pid) {
        return Err(Error::Cli("mount process not running (stale PID file)".into()).into());
    }

    let sig = if args.counters_reset {
        libc::SIGUSR2
    } else {
        libc::SIGUSR1
    };
    if unsafe { libc::kill(record.pid, sig) } != 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ESRCH) {
            return Err(Error::Cli("mount process not running (stale PID file)".into()).into());
        }
        return Err(Error::Cli(format!("failed to signal mount process: {err}")).into());
    }

    // Read stat file and wait briefly for the worker to update it after signalling.
    // The file may already exist (initialized to "{}"), so we must wait for a "fresh" payload.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    let mut last_bytes: Option<Vec<u8>> = None;
    let value: serde_json::Value = loop {
        match std::fs::read(&stat_path) {
            Ok(bytes) => {
                last_bytes = Some(bytes.clone());
                if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                    if v.get("overlay").is_some() && v.get("fuse").is_some() {
                        break v;
                    }
                }
            }
            Err(_e) if std::time::Instant::now() < deadline => {}
            Err(e) => return Err(Error::Io(e).into()),
        }

        if std::time::Instant::now() >= deadline {
            let raw = last_bytes
                .as_deref()
                .map(|b| String::from_utf8_lossy(b).to_string())
                .unwrap_or_else(|| "<missing>".into());
            return Err(Error::Cli(format!("failed to read fresh stats (timeout): {raw}")).into());
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    };

    match args.format {
        StatFormat::Json => {
            let out = serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string());
            println!("{out}");
            let _ = std::fs::write(&stat_path, out.as_bytes());
        }
        StatFormat::Text => {
            let out = format_text(&value);
            println!("{out}");
            let _ = std::fs::write(&stat_path, out.as_bytes());
        }
    }

    Ok(())
}

fn format_text(value: &serde_json::Value) -> String {
    // Simple deterministic formatter for common object-like stats.
    match value {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<_> = map.keys().cloned().collect();
            keys.sort();
            let mut out = String::new();
            for k in keys {
                let v = &map[&k];
                out.push_str(&format!("{k}={}\n", v));
            }
            out
        }
        other => other.to_string(),
    }
}
