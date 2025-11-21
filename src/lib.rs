pub mod backup;
pub mod binding;
pub mod cli;
pub mod fs;
pub mod logging;

/// Entry point for the library, called by the CLI thin wrapper.
pub fn run<I, S>(args: I) -> Result<(), Box<dyn std::error::Error>>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    // Placeholder until CLI argument parsing is implemented.
    let _args: Vec<String> = args.into_iter().map(Into::into).collect();
    Ok(())
}
