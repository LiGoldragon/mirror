//! `mirror-write-configuration` — encode typed NOTA into daemon startup
//! rkyv.
//!
//! This is the deploy/bootstrap text edge. `mirror-daemon` itself takes
//! exactly one binary rkyv configuration file and never parses NOTA.

use mirror::configuration_writer::CommandLine;
use nota::NotaEncode;
use triad_runtime::ComponentCommand;

fn main() -> std::process::ExitCode {
    match CommandLine::from_environment(ComponentCommand::from_environment()).run() {
        Ok(written) => {
            println!("{}", written.to_nota());
            std::process::ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("(MirrorConfigurationWriterError [{error}])");
            std::process::ExitCode::from(2)
        }
    }
}
