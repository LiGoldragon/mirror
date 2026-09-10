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
use meta_signal_mirror::{
    ByteViewable as MetaByteViewable, Restorable as MetaRestorable, Signal as MetaSignal,
    Signalizable as MetaSignalizable,
};
use signal_mirror::{
    ByteViewable as WorkingByteViewable, Restorable as WorkingRestorable, Signal as WorkingSignal,
    Signalizable as WorkingSignalizable,
};

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
        let input =
            rkyv::from_bytes::<signal_mirror::Query, rkyv::rancor::Error>(&body.into_bytes())
                .map_err(|error| Error::Archive(error.to_string()))?;
        let output = engine.working(input, *connection.context()).await?;
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&output)
            .map_err(|error| Error::Archive(error.to_string()))?;
        codec
            .write_body_async(
                connection.stream_mut(),
                &FrameBody::new(bytes.bytes().to_vec()),
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
        let input =
            rkyv::from_bytes::<meta_signal_mirror::Query, rkyv::rancor::Error>(&body.into_bytes())
                .map_err(|error| Error::Archive(error.to_string()))?;
        let output = engine.meta(input).await?;
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&output)
            .map_err(|error| Error::Archive(error.to_string()))?;
        codec
            .write_body_async(
                connection.stream_mut(),
                &FrameBody::new(bytes.bytes().to_vec()),
            )
            .await?;
        connection.stream_mut().flush().await?;
        Ok(())
    }
}
