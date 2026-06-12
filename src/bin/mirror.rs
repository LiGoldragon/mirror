//! `mirror` — the thin CLI client for the mirror daemon's working surface.
//!
//! Takes exactly one NOTA argument naming a `signal_mirror::Input`
//! request, sends it to the daemon over `MIRROR_SOCKET`, and prints the
//! reply as NOTA.

use mirror::client::CommandLine;

fn main() -> std::process::ExitCode {
    match CommandLine::from_environment().run(std::io::stdout().lock()) {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("(MirrorClientError [{error}])");
            std::process::ExitCode::from(2)
        }
    }
}
