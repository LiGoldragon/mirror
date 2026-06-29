//! The mirror CLIs' daemon clients — thin text-to-Signal adapters.
//!
//! `mirror` reads one NOTA `signal_mirror::Input` off argv, encodes it
//! to a binary signal frame on the daemon's working Unix socket, and
//! renders the binary reply back as NOTA. `meta-mirror` does the same
//! over the owner-only meta socket with the `meta-signal-mirror`
//! contract. The daemon never sees NOTA — only the binary frames the
//! CLIs translate.

use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

use nota::NotaEncode;
use triad_runtime::{FrameBody, LengthPrefixedCodec};

use crate::error::{Error, Result};

/// A daemon socket resolved from one environment variable (no flag, no
/// socket argument — the spirit/message CLI convention).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonSocket {
    path: PathBuf,
}

impl DaemonSocket {
    pub fn from_environment(variable: &str) -> Option<Self> {
        std::env::var_os(variable).map(|path| Self { path: path.into() })
    }

    fn exchange(&self, request: Vec<u8>) -> Result<Vec<u8>> {
        let codec = LengthPrefixedCodec::default();
        let mut stream = UnixStream::connect(&self.path)?;
        codec.write_body(&mut stream, &FrameBody::new(request))?;
        stream.flush()?;
        Ok(codec.read_body(&mut stream)?.into_bytes())
    }

    /// Send one working-surface `signal_mirror::Input` as a binary signal frame
    /// and decode the typed `Output` reply. The non-text round-trip the NOTA CLI
    /// and the two-VM witness verifier both ride.
    pub fn request(&self, input: signal_mirror::Input) -> Result<signal_mirror::Output> {
        let reply = self.exchange(input.encode_signal_frame()?)?;
        let (_route, output) = signal_mirror::Output::decode_signal_frame(&reply)?;
        Ok(output)
    }
}

/// The working CLI command: one NOTA argument naming a
/// `signal_mirror::Input` request, sent over `MIRROR_SOCKET`.
pub struct CommandLine {
    argument: Option<String>,
}

impl CommandLine {
    pub fn from_environment() -> Self {
        Self {
            argument: std::env::args().nth(1),
        }
    }

    pub fn run(self, mut output: impl Write) -> Result<()> {
        let argument = self.argument.ok_or(Error::MissingArgument)?;
        let input: signal_mirror::Input = argument.parse().map_err(Error::NotaDecode)?;
        let socket = DaemonSocket::from_environment("MIRROR_SOCKET").ok_or_else(|| {
            Error::SocketVariableUnset {
                variable: "MIRROR_SOCKET".to_owned(),
            }
        })?;
        let decoded = socket.request(input)?;
        writeln!(output, "{}", decoded.to_nota()).map_err(Error::Io)?;
        Ok(())
    }
}

/// The meta CLI command: one NOTA argument naming a
/// `meta_signal_mirror::Input` order, sent over `MIRROR_META_SOCKET`.
pub struct MetaCommandLine {
    argument: Option<String>,
}

impl MetaCommandLine {
    pub fn from_environment() -> Self {
        Self {
            argument: std::env::args().nth(1),
        }
    }

    pub fn run(self, mut output: impl Write) -> Result<()> {
        let argument = self.argument.ok_or(Error::MissingArgument)?;
        let input: meta_signal_mirror::Input = argument.parse().map_err(Error::NotaDecode)?;
        let socket = DaemonSocket::from_environment("MIRROR_META_SOCKET").ok_or_else(|| {
            Error::SocketVariableUnset {
                variable: "MIRROR_META_SOCKET".to_owned(),
            }
        })?;
        let reply = socket.exchange(input.encode_signal_frame()?)?;
        let (_route, decoded) = meta_signal_mirror::Output::decode_signal_frame(&reply)?;
        writeln!(output, "{}", decoded.to_nota()).map_err(Error::Io)?;
        Ok(())
    }
}
