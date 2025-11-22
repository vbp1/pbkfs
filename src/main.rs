fn main() {
    let args = std::env::args();
    // Initialize logging as early as possible; fallback to stderr on failure.
    let _ = pbkfs::logging::init_logging(pbkfs::logging::LogFormat::Human);

    if let Err(err) = pbkfs::run(args) {
        eprintln!("pbkfs error: {err}");
        std::process::exit(1);
    }
}
