//! Mirror's two-tier Unix daemon and shared tailnet service binding.

use std::time::Duration;

use tokio::io::AsyncWriteExt;
use triad_runtime::kameo::actor::Spawn;
use triad_runtime::{AcceptedConnection, FrameBody, LengthPrefixedCodec, MaximumFrameLength};

use crate::component_daemon::ComponentDaemon;
use crate::config::{Configuration, ConfigurationError};
use crate::engine::Engine;
use crate::error::Error;
use crate::service::{Service, ServiceLink};

const MAXIMUM_FRAME_BYTES: usize = 1024 * 1024;
const REQUEST_READ_TIMEOUT: Duration = Duration::from_secs(10);

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

    fn build_runtime(configuration: &Self::Configuration) -> Result<Self::Engine, Self::Error> {
        let engine = Engine::open(configuration)?;
        let service = Service::spawn(Service::new(engine, configuration.tcp_listen_address()));
        Ok(ServiceLink::new(service))
    }

    async fn handle_working_connection(
        engine: &Self::Engine,
        mut connection: AcceptedConnection,
    ) -> Result<(), Self::Error> {
        let codec = LengthPrefixedCodec::new(MaximumFrameLength::new(MAXIMUM_FRAME_BYTES));
        let body = tokio::time::timeout(
            REQUEST_READ_TIMEOUT,
            codec.read_body_async(connection.stream_mut()),
        )
        .await
        .map_err(|_| Error::RequestReadTimedOut)??;
        let (exchange, input) =
            signal_mirror::ContractMarker::decode_single_request(&body.into_bytes())?;
        let output = engine.working(input, *connection.context()).await?;
        codec
            .write_body_async(
                connection.stream_mut(),
                &FrameBody::new(output.encode_reply_frame(exchange)?),
            )
            .await?;
        connection.stream_mut().flush().await?;
        Ok(())
    }

    async fn handle_meta_connection(
        engine: &Self::Engine,
        mut connection: AcceptedConnection,
    ) -> Result<(), Self::Error> {
        let codec = LengthPrefixedCodec::new(MaximumFrameLength::new(MAXIMUM_FRAME_BYTES));
        let body = tokio::time::timeout(
            REQUEST_READ_TIMEOUT,
            codec.read_body_async(connection.stream_mut()),
        )
        .await
        .map_err(|_| Error::RequestReadTimedOut)??;
        let (exchange, input) =
            meta_signal_mirror::ContractMarker::decode_single_request(&body.into_bytes())?;
        let output = engine.meta(input).await?;
        codec
            .write_body_async(
                connection.stream_mut(),
                &FrameBody::new(output.encode_reply_frame(exchange)?),
            )
            .await?;
        connection.stream_mut().flush().await?;
        Ok(())
    }
}
