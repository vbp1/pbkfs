//! CLI module placeholder; subcommands live here.

use clap::{Parser, Subcommand};

use crate::Result;

pub mod cleanup;
pub mod mount;
pub mod unmount;

#[derive(Debug, Clone)]
pub enum Command {
    Mount(mount::MountArgs),
    Unmount(unmount::UnmountArgs),
    Cleanup(cleanup::CleanupArgs),
    None,
}

#[derive(Debug, Clone)]
pub struct CliArgs {
    pub command: Command,
}

impl Default for CliArgs {
    fn default() -> Self {
        Self {
            command: Command::None,
        }
    }
}

pub fn dispatch(args: CliArgs) -> Result<()> {
    match args.command {
        Command::Mount(m) => mount::execute(m),
        Command::Unmount(u) => unmount::execute(u),
        Command::Cleanup(c) => cleanup::execute(c),
        Command::None => Ok(()),
    }
}

#[derive(Parser, Debug)]
#[command(name = "pbkfs", version, about = "pg_probackup FUSE mount helper")]
struct Cli {
    #[command(subcommand)]
    command: Option<Subcommands>,
}

#[derive(Subcommand, Debug)]
enum Subcommands {
    /// Mount a pg_probackup backup into a target directory with copy-on-write diff storage.
    Mount(mount::MountArgs),
    /// Unmount a previously mounted pbkfs target.
    Unmount(unmount::UnmountArgs),
    /// Cleanup a diff directory for reuse.
    Cleanup(cleanup::CleanupArgs),
}

/// Parse CLI arguments into internal representation.
pub fn parse_args<I, S>(args: I) -> Result<CliArgs>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let argv: Vec<String> = args.into_iter().map(Into::into).collect();
    let cli = Cli::parse_from(argv);
    let command = match cli.command {
        Some(Subcommands::Mount(args)) => Command::Mount(args),
        Some(Subcommands::Unmount(args)) => Command::Unmount(args),
        Some(Subcommands::Cleanup(args)) => Command::Cleanup(args),
        None => Command::None,
    };

    Ok(CliArgs { command })
}
