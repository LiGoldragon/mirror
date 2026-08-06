//! `meta-mirror` — the thin CLI client for the mirror daemon's owner-only
//! meta surface.
//!
//! Takes exactly one Dotos argument naming a meta Mirror input
//! order, sends it to the daemon over `MIRROR_META_SOCKET`, and prints
//! the reply as Dotos.

use mirror::client::MetaCommandLine;

fn main() -> std::process::ExitCode {
    match MetaCommandLine::from_environment().run(std::io::stdout().lock()) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("(MirrorMetaClientError [{error}])");
            std::process::ExitCode::from(2)
        }
    }
}
