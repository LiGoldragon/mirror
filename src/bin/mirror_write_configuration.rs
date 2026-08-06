//! `mirror-write-configuration` — encode typed Dotos into daemon startup
//! rkyv.
//!
//! This is the deploy/bootstrap text edge. `mirror-daemon` itself takes
//! exactly one binary rkyv configuration file and never parses Dotos.

use dotos::DotosEncode;
use mirror::configuration_writer::CommandLine;
use triad_runtime::ComponentCommand;

fn main() -> std::process::ExitCode {
    match CommandLine::from_environment(ComponentCommand::from_environment()).run() {
        Ok(written) => {
            println!("{}", written.to_dotos());
            std::process::ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("(MirrorConfigurationWriterError [{error}])");
            std::process::ExitCode::from(2)
        }
    }
}
