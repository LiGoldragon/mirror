//! Dotos bootstrap edge for Mirror's binary startup configuration.

use dotos::{DotosDecodeError, DotosEncode, DotosSource};
use thiserror::Error;
use triad_runtime::{ArgumentError, ComponentArgument, ComponentCommand};

pub struct CommandLine {
    command: ComponentCommand,
}

#[derive(Debug, Clone, PartialEq, Eq, DotosEncode)]
pub enum Written {
    ConfigurationWritten(meta_signal_mirror::z2VYru),
}

impl CommandLine {
    pub fn from_environment(command: ComponentCommand) -> Self {
        Self { command }
    }

    pub fn run(&self) -> Result<Written, WriterError> {
        let text = self.source_text()?;
        let request = DotosSource::new(&text).parse::<meta_signal_mirror::z2VQo2>()?;
        let destination = request.field_0.clone();
        request.field_1.write_binary_file(destination.as_path())?;
        Ok(Written::ConfigurationWritten(destination))
    }

    fn source_text(&self) -> Result<String, WriterError> {
        match self.command.dotos_argument()? {
            ComponentArgument::InlineDotos(argument) => Ok(argument.into_string()),
            ComponentArgument::DotosFile(file) => {
                let path = file.into_path();
                std::fs::read_to_string(&path)
                    .map_err(|source| WriterError::ReadDotosFile { path, source })
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
    #[error("Dotos decode: {0}")]
    Decode(#[from] DotosDecodeError),
    #[error("configuration archive: {0}")]
    Archive(#[from] meta_signal_mirror::ConfigurationArchiveError),
    #[error("failed to read Dotos file {path}: {source}")]
    ReadDotosFile {
        path: std::path::PathBuf,
        source: std::io::Error,
    },
    #[error("signal files are not a writer input: {path}")]
    UnsupportedSignalFile { path: std::path::PathBuf },
}
