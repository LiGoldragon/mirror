//! The mirror daemon's typed crate error.

use thiserror::Error;
use triad_runtime::{AsyncListenerError, EngineRequestError, FrameError};

use crate::config::ConfigurationError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("missing NOTA request argument")]
    MissingArgument,

    #[error("NOTA request decode: {0}")]
    NotaDecode(#[from] nota_next::NotaDecodeError),

    #[error("socket environment variable {variable} is not set")]
    SocketVariableUnset { variable: String },

    #[error("configuration: {0}")]
    Configuration(#[from] ConfigurationError),

    #[error("triad frame: {0}")]
    Frame(#[from] FrameError),

    #[error("ordinary signal frame: {0}")]
    OrdinarySignalFrame(signal_mirror::SignalFrameError),

    #[error("meta signal frame: {0}")]
    MetaSignalFrame(meta_signal_mirror::SignalFrameError),

    #[error("engine actor: {0}")]
    EngineRequest(#[from] EngineRequestError),

    #[error("ledger storage: {0}")]
    Ledger(#[from] sema_engine::Error),

    #[error("payload encode for {surface}: {message}")]
    PayloadEncode {
        surface: &'static str,
        message: String,
    },

    #[error("component outbox has {outbox_rows} rows but replay returned {replay_entries} entries")]
    OutboxSuffixMismatch {
        outbox_rows: usize,
        replay_entries: usize,
    },

    #[error("no checkpoint is available to publish")]
    CheckpointUnavailable,

    #[error("mirror append rejected: {reason:?}")]
    MirrorAppendRejected {
        reason: signal_mirror::AppendRejectionReason,
        head: Option<signal_mirror::HeadMark>,
    },

    #[error("mirror checkpoint publish rejected: {reason:?}")]
    MirrorPublishRejected {
        reason: signal_mirror::PublishRejectionReason,
    },

    #[error("mirror faulted: {detail}")]
    MirrorFaulted { detail: String },

    #[error("unexpected mirror output while waiting for {expected}: {actual}")]
    UnexpectedMirrorOutput {
        expected: &'static str,
        actual: String,
    },

    #[error("tailnet listener: {0}")]
    TailnetListener(#[from] AsyncListenerError),

    #[error("meta request read timed out")]
    MetaRequestReadTimedOut,

    #[error("mirror service stopped before replying")]
    ServiceUnavailable,
}

impl From<signal_mirror::SignalFrameError> for Error {
    fn from(error: signal_mirror::SignalFrameError) -> Self {
        Self::OrdinarySignalFrame(error)
    }
}

impl From<meta_signal_mirror::SignalFrameError> for Error {
    fn from(error: meta_signal_mirror::SignalFrameError) -> Self {
        Self::MetaSignalFrame(error)
    }
}

pub type Result<T> = std::result::Result<T, Error>;
