//! The mirror daemon's hooks — the only Unix daemon code the mirror
//! hand-writes.
//!
//! The uniform daemon skeleton (argv parsing, the async working
//! decode -> ask -> encode spine, the two-tier Unix listener bind, and
//! the `ExitReport` entry) is EMITTED into `src/schema/daemon.rs` by
//! schema-rust's daemon emitter. The mirror fills the escape
//! hatches through `impl ComponentDaemon for Daemon`: its
//! `Configuration`, building the shared `ServiceLink` runtime (which
//! spawns the one `Service` actor — and with it the hand-wired
//! tailnet TCP ingress), the working-input forward, and the meta tier.

use std::time::Duration;

use tokio::io::AsyncWriteExt;
use triad_runtime::kameo::actor::Spawn;
use triad_runtime::{
    AcceptedConnection, ConnectionContext, FrameBody, LengthPrefixedCodec, MaximumFrameLength,
};

use crate::config::{Configuration, ConfigurationError};
use crate::engine::Engine;
use crate::error::Error;
use crate::schema::daemon::ComponentDaemon;
use crate::service::{Service, ServiceLink};

/// Maximum inbound meta-request-frame body the daemon accepts (1 MiB). A
/// meta order is a few hundred bytes; this bounds a hostile length prefix
/// far below the u32-prefix codec default.
const MAXIMUM_META_FRAME_BYTES: usize = 1024 * 1024;

/// How long the meta handler waits for a connected client to send its
/// request frame before dropping the stream.
const META_REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// The type-level selector for the mirror's emitted daemon. It carries no
/// runtime data — it is the marker the emitted `DaemonCommand<Daemon>`
/// dispatches on, selecting the mirror's `Configuration` / `Engine` /
/// `Error` through the `ComponentDaemon` associated types.
pub struct Daemon;

impl ComponentDaemon for Daemon {
    type Configuration = Configuration;
    type ConfigurationError = ConfigurationError;
    type Engine = ServiceLink;
    type Error = Error;

    const PROCESS_NAME: &'static str = "mirror-daemon";

    fn load_configuration(
        path: &std::path::Path,
    ) -> Result<Self::Configuration, Self::ConfigurationError> {
        Configuration::from_binary_path(path)
    }

    /// Open the durable store, spawn the one `Service` actor (whose
    /// `on_start` binds the tailnet TCP ingress), and hand the generated
    /// daemon a cloneable link into its mailbox.
    fn build_runtime(configuration: &Self::Configuration) -> Result<Self::Engine, Self::Error> {
        let engine = Engine::open(configuration)?;
        let service = Service::spawn(Service::new(engine, configuration.tcp_listen_address()));
        Ok(ServiceLink::new(service))
    }

    async fn handle_working_input(
        engine: &mut Self::Engine,
        input: signal_mirror::Input,
        connection: &ConnectionContext,
    ) -> Result<signal_mirror::Output, Self::Error> {
        engine.working(input, *connection).await
    }

    /// Serve one meta order end to end: decode a `meta-signal-mirror`
    /// `Input` off the length-prefixed frame, run it through the service
    /// mailbox, and write the meta `Output` back. Only this Unix
    /// owner-socket path constructs meta orders — the TCP ingress decodes
    /// the ordinary contract exclusively.
    async fn handle_meta_connection(
        engine: &mut Self::Engine,
        mut connection: AcceptedConnection,
    ) -> Result<(), Self::Error> {
        let codec = LengthPrefixedCodec::new(MaximumFrameLength::new(MAXIMUM_META_FRAME_BYTES));
        let body = tokio::time::timeout(
            META_REQUEST_READ_TIMEOUT,
            codec.read_body_async(connection.stream_mut()),
        )
        .await
        .map_err(|_| Error::MetaRequestReadTimedOut)??;
        let (_route, input) = meta_signal_mirror::Input::decode_signal_frame(&body.into_bytes())?;
        let reply = engine.meta(input).await?;
        codec
            .write_body_async(
                connection.stream_mut(),
                &FrameBody::new(reply.encode_signal_frame()?),
            )
            .await?;
        connection.stream_mut().flush().await?;
        Ok(())
    }
}
