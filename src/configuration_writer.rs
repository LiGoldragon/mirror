//! The deploy text edge that encodes typed NOTA into the daemon's binary
//! rkyv startup configuration.
//!
//! The request is the `meta-signal-mirror` contract's
//! `ConfigurationWrite` record (destination path + typed
//! `DaemonConfiguration`), read as the single NOTA argument
//! (inline or a `.nota` file). The daemon itself never parses NOTA.

use meta_signal_mirror::{ConfigurationWrite, WirePath};
use nota::{NotaDecodeError, NotaEncode, NotaSource};
use thiserror::Error;
use triad_runtime::{ArgumentError, ComponentArgument, ComponentCommand};

/// The writer command: one NOTA `ConfigurationWrite` argument.
pub struct CommandLine {
    command: ComponentCommand,
}

/// The typed receipt printed after the binary file is written.
#[derive(Debug, Clone, PartialEq, Eq, NotaEncode)]
pub enum Written {
    ConfigurationWritten(WirePath),
}

impl CommandLine {
    pub fn from_environment(command: ComponentCommand) -> Self {
        Self { command }
    }

    pub fn run(&self) -> Result<Written, WriterError> {
        let text = self.source_text()?;
        let request: ConfigurationWrite = NotaSource::new(&text).parse()?;
        let destination = request.wire_path.clone();
        request
            .daemon_configuration
            .write_binary_file(destination.as_path())?;
        Ok(Written::ConfigurationWritten(destination))
    }

    fn source_text(&self) -> Result<String, WriterError> {
        match self.command.nota_argument()? {
            ComponentArgument::InlineNota(argument) => Ok(argument.into_string()),
            ComponentArgument::NotaFile(file) => {
                let path = file.into_path();
                std::fs::read_to_string(&path)
                    .map_err(|source| WriterError::ReadNotaFile { path, source })
            }
            ComponentArgument::SignalFile(file) => Err(WriterError::UnsupportedSignalFile {
                path: file.into_path(),
            }),
        }
    }
}

#[derive(Debug, Error)]
pub enum WriterError {
    #[error("argument: {0}")]
    Argument(#[from] ArgumentError),

    #[error("NOTA decode: {0}")]
    Decode(#[from] NotaDecodeError),

    #[error("configuration archive: {0}")]
    Archive(#[from] meta_signal_mirror::ConfigurationArchiveError),

    #[error("failed to read NOTA file {path}: {source}")]
    ReadNotaFile {
        path: std::path::PathBuf,
        source: std::io::Error,
    },

    #[error("signal files are not a writer input: {path}")]
    UnsupportedSignalFile { path: std::path::PathBuf },
}
