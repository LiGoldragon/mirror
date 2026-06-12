//! `mirror-daemon` — the long-lived payload-blind version-control remote.
//!
//! Takes exactly one argument: a binary rkyv startup configuration file
//! (the single-argument rule; the daemon never parses NOTA). The emitted
//! `DaemonEntry` owns argv parsing, the two-tier Unix listener bind, and
//! the request spine; the mirror service actor binds the tailnet TCP
//! ingress on startup.

use mirror::{DaemonEntry, MirrorDaemon};

fn main() -> std::process::ExitCode {
    MirrorDaemon::run_to_exit_code()
}
