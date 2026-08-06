//! `mirror` runtime — the payload-blind sema version-control remote.
//!
//! One append-ingest mirror daemon serves every component store (Spirit
//! 0yx5): it validates sequence continuity and expected head,
//! deduplicates idempotently by entry digest, persists into its own
//! versioned sema-engine store before acknowledging, and carries
//! registration and retention policy behind its owner-only meta signal.
//!
//! The working and owner Interfaces come directly from their Ethos
//! authorities. Mirror owns only its ordinary daemon mechanics and private
//! durable ledger; no runtime schema emitter or compatibility vocabulary
//! stands between those surfaces.

#[cfg(feature = "dotos-text")]
pub mod client;
pub mod component_daemon;
pub mod config;
#[cfg(feature = "dotos-text")]
pub mod configuration_writer;
pub mod daemon;
pub mod decision;
pub mod engine;
pub mod error;
pub mod ledger;
pub mod readback;
pub mod service;
pub mod shipper;
pub mod store;

pub use component_daemon::{ComponentDaemon, DaemonCommand, DaemonEntry, DaemonError};
pub use config::{Configuration, ConfigurationError};
pub use daemon::Daemon;
pub use engine::Engine;
pub use error::{Error, Result};
pub use readback::LandedBody;
pub use service::{
    Service, ServiceLink, TailnetIngress, TcpAddressQuery, TcpPeerWitness, TcpPeerWitnessQuery,
};
pub use shipper::{
    ComponentShipper, MirrorTailnetClient, PublishLatestCheckpoint, ShipOutcome, ShipUnshipped,
};
pub use store::Store;
