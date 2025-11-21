fn main() {
    let args = std::env::args();
    if let Err(err) = pbkfs::run(args) {
        eprintln!("pbkfs error: {err}");
        std::process::exit(1);
    }
}
