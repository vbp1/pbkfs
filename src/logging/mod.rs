//! Logging initialization using `tracing` and `tracing-subscriber`.

use tracing_subscriber::{fmt, util::SubscriberInitExt, EnvFilter};

use crate::Result;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Human,
    Json,
}

impl Default for LogFormat {
    fn default() -> Self {
        LogFormat::Human
    }
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
