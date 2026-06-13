//! `mirror` runtime — the payload-blind sema version-control remote.
//!
//! One append-ingest mirror daemon serves every component store (Spirit
//! 0yx5): it validates sequence continuity and expected head,
//! deduplicates idempotently by entry digest, persists into its own
//! versioned sema-engine store before acknowledging, and carries
//! registration and retention policy behind its owner-only meta signal.
//!
//! The daemon is a schema-derived triad component on the emitted daemon
//! runtime: the working tier's `Input`/`Output` come from the dependency
//! contract `signal-mirror`, the meta tier from `meta-signal-mirror`,
//! and the two daemon-local plane schemas (`schema/nexus.schema`,
//! `schema/sema.schema`) generate the checked-in modules under
//! `src/schema/`. The tailnet TCP ingress (Spirit rj9y) is hand-wired
//! around the same service actor — see `src/service.rs`.

pub mod client;
pub mod config;
pub mod configuration_writer;
pub mod decision;
pub mod engine;
pub mod error;
pub mod schema_daemon;
pub mod service;
pub mod shipper;
pub mod store;

pub mod schema {
    #[rustfmt::skip]
    pub mod nexus;
    #[rustfmt::skip]
    pub mod sema;
    #[rustfmt::skip]
    pub mod daemon;
}

pub use config::{Configuration, ConfigurationError};
pub use engine::MirrorEngine;
pub use error::{Error, Result};
pub use schema::daemon::{ComponentDaemon, DaemonCommand, DaemonEntry, DaemonError};
pub use schema_daemon::MirrorDaemon;
pub use service::{
    MirrorService, ServiceLink, TailnetIngress, TcpAddressQuery, TcpPeerWitness,
    TcpPeerWitnessQuery,
};
pub use shipper::{
    ComponentShipper, MirrorTailnetClient, PublishLatestCheckpoint, ShipOutcome, ShipUnshipped,
};
pub use store::Store;
