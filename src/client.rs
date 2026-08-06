//! Dotos-to-Signal adapters for Mirror's Unix interfaces.

use std::io::Write;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;

use dotos::{DotosEncode, DotosSource};
use signal_frame_interface::{
    ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply,
};
use triad_runtime::{FrameBody, LengthPrefixedCodec};

use crate::error::{Error, Result};

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

    fn exchange_identifier(&self) -> ExchangeIdentifier {
        ExchangeIdentifier::new(
            SessionEpoch::new(0),
            ExchangeLane::Connector,
            LaneSequence::first(),
        )
    }

    pub fn request(&self, input: signal_mirror::z2VVny) -> Result<signal_mirror::z2VTqL> {
        let reply = self.exchange(input.encode_request_frame(self.exchange_identifier())?)?;
        match signal_mirror::ContractMarker::decode_frame(&reply)?.into_body() {
            signal_mirror::FrameBody::Reply { reply, .. } => reply_value(reply),
            other => Err(Error::UnexpectedReplyFrame {
                actual: format!("{other:?}"),
            }),
        }
    }

    pub fn meta_request(
        &self,
        input: meta_signal_mirror::z2VWt5,
    ) -> Result<meta_signal_mirror::z2VUH6> {
        let reply = self.exchange(input.encode_request_frame(self.exchange_identifier())?)?;
        match meta_signal_mirror::ContractMarker::decode_frame(&reply)?.into_body() {
            meta_signal_mirror::FrameBody::Reply { reply, .. } => reply_value(reply),
            other => Err(Error::UnexpectedReplyFrame {
                actual: format!("{other:?}"),
            }),
        }
    }
}

fn reply_value<Output: std::fmt::Debug>(reply: Reply<Output>) -> Result<Output> {
    match reply {
        Reply::Accepted { per_operation, .. } => match per_operation.into_head() {
            SubReply::Ok(output) => Ok(output),
            other => Err(Error::UnexpectedSubReply {
                actual: format!("{other:?}"),
            }),
        },
        Reply::Rejected { reason } => Err(Error::ReplyRejected {
            reason: reason.to_string(),
        }),
    }
}

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
        let input = DotosSource::new(&argument).parse::<signal_mirror::z2VVny>()?;
        let socket = DaemonSocket::from_environment("MIRROR_SOCKET").ok_or_else(|| {
            Error::SocketVariableUnset {
                variable: "MIRROR_SOCKET".to_owned(),
            }
        })?;
        writeln!(output, "{}", socket.request(input)?.to_dotos()).map_err(Error::Io)
    }
}

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
        let input = DotosSource::new(&argument).parse::<meta_signal_mirror::z2VWt5>()?;
        let socket = DaemonSocket::from_environment("MIRROR_META_SOCKET").ok_or_else(|| {
            Error::SocketVariableUnset {
                variable: "MIRROR_META_SOCKET".to_owned(),
            }
        })?;
        writeln!(output, "{}", socket.meta_request(input)?.to_dotos()).map_err(Error::Io)
    }
}
