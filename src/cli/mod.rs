//! CLI module placeholder; subcommands live here.

use clap::{CommandFactory, Parser, Subcommand};

use crate::Result;

pub mod cleanup;
pub mod daemon;
pub mod mount;
pub mod pid;
pub mod stat;
pub mod unmount;

#[derive(Debug, Clone)]
pub enum Command {
    Mount(mount::MountArgs),
    Unmount(unmount::UnmountArgs),
    Cleanup(cleanup::CleanupArgs),
    Stat(stat::StatArgs),
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
        Command::Stat(s) => stat::execute(s),
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
    /// Runs in background by default; use `--console` to stay in the foreground.
    Mount(mount::MountArgs),
    /// Unmount a previously mounted pbkfs target.
    Unmount(unmount::UnmountArgs),
    /// Cleanup a diff directory for reuse.
    Cleanup(cleanup::CleanupArgs),
    /// Dump pbkfs mount statistics (signals the mount worker).
    Stat(stat::StatArgs),
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
        Some(Subcommands::Stat(args)) => Command::Stat(args),
        None => Command::None,
    };

    Ok(CliArgs { command })
}

/// Build the underlying clap `Command` (useful for help/usage contract tests).
pub fn clap_command() -> clap::Command {
    Cli::command()
}
