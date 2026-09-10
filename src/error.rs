//! The mirror daemon's typed crate error.

use thiserror::Error;
use triad_runtime::{AsyncListenerError, EngineRequestError, FrameError};

use crate::config::ConfigurationError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("missing Dotos request argument")]
    MissingArgument,

    #[error("Dotos request decode: {0}")]
    #[cfg(feature = "dotos-text")]
    DotosDecode(#[from] dotos::DotosDecodeError),

    #[error("landed body is not a versioned commit log entry: {0}")]
    LandedBodyDecode(String),

    #[error("socket environment variable {variable} is not set")]
    SocketVariableUnset { variable: String },

    #[error("configuration: {0}")]
    Configuration(#[from] ConfigurationError),

    #[error("triad frame: {0}")]
    Frame(#[from] FrameError),

    #[error("signal archive: {0}")]
    Archive(String),

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

    #[error("Mirror carried a non-canonical digest: {digest}")]
    MirrorDigestInvalid { digest: String },

    #[error("unexpected mirror output while waiting for {expected}: {actual}")]
    UnexpectedMirrorOutput {
        expected: &'static str,
        actual: String,
    },

    #[error("unexpected Signal reply frame: {actual}")]
    UnexpectedReplyFrame { actual: String },

    #[error("unexpected Signal sub-reply: {actual}")]
    UnexpectedSubReply { actual: String },

    #[error("Signal reply rejected: {reason}")]
    ReplyRejected { reason: String },

    #[error("tailnet listener: {0}")]
    TailnetListener(#[from] AsyncListenerError),

    #[error("request read timed out")]
    RequestReadTimedOut,

    #[error("mirror service stopped before replying")]
    ServiceUnavailable,
}

pub type Result<T> = std::result::Result<T, Error>;
