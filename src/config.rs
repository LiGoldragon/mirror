//! The mirror daemon's runtime configuration wrapper.
//!
//! The typed configuration record `MirrorDaemonConfiguration` lives in the
//! `meta-signal-mirror` contract; the daemon decodes it from its single
//! binary rkyv startup argument and wraps it here so the daemon crate can
//! implement `triad_runtime::BindingSurface` (a foreign trait on a foreign
//! type otherwise) and pre-parse the tailnet listen address once.

use std::net::SocketAddr;
use std::path::Path;

use meta_signal_mirror::{ConfigurationArchiveError, MirrorDaemonConfiguration};
use thiserror::Error;
use triad_runtime::{BindingSurface, SocketMode};

use crate::error::Error;

/// The daemon-local configuration: the decoded contract record plus the
/// parsed tailnet socket address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Configuration {
    contract: MirrorDaemonConfiguration,
    tcp_listen_address: SocketAddr,
}

impl Configuration {
    /// Decode the daemon's single binary startup argument.
    pub fn from_binary_path(path: &Path) -> Result<Self, ConfigurationError> {
        MirrorDaemonConfiguration::from_binary_path(path)?.try_into()
    }

    pub fn contract(&self) -> &MirrorDaemonConfiguration {
        &self.contract
    }

    pub fn storage_path(&self) -> &Path {
        self.contract.storage_path.as_path()
    }

    /// The tailnet-bound TCP ingress address (Spirit rj9y). The runtime
    /// binds whatever address is configured; the tailnet trust boundary is
    /// the deployment's bind choice, never a payload claim.
    pub fn tcp_listen_address(&self) -> SocketAddr {
        self.tcp_listen_address
    }
}

impl TryFrom<MirrorDaemonConfiguration> for Configuration {
    type Error = ConfigurationError;

    fn try_from(contract: MirrorDaemonConfiguration) -> Result<Self, Self::Error> {
        let address = contract.tcp_listen_address.as_str();
        let tcp_listen_address =
            address
                .parse()
                .map_err(|_| ConfigurationError::ListenAddressInvalid {
                    address: address.to_owned(),
                })?;
        Ok(Self {
            contract,
            tcp_listen_address,
        })
    }
}

impl BindingSurface for Configuration {
    fn socket_path(&self) -> &Path {
        self.contract.working_socket_path.as_path()
    }

    fn socket_mode(&self) -> Option<SocketMode> {
        Some(SocketMode::new(
            self.contract.working_socket_mode.clone().into_u32(),
        ))
    }

    fn meta_socket_path(&self) -> Option<&Path> {
        Some(self.contract.meta_socket_path.as_path())
    }

    fn meta_socket_mode(&self) -> Option<SocketMode> {
        Some(SocketMode::new(
            self.contract.meta_socket_mode.clone().into_u32(),
        ))
    }

    fn database_path(&self) -> &Path {
        self.contract.storage_path.as_path()
    }
}

#[derive(Debug, Error)]
pub enum ConfigurationError {
    #[error("binary configuration archive: {0}")]
    Archive(#[from] ConfigurationArchiveError),

    #[error("tcp listen address is not a socket address: {address}")]
    ListenAddressInvalid { address: String },
}

impl From<ConfigurationError> for Error {
    fn from(error: ConfigurationError) -> Self {
        match error {
            ConfigurationError::Archive(archive) => {
                Error::Io(std::io::Error::other(archive.to_string()))
            }
            ConfigurationError::ListenAddressInvalid { address } => {
                Error::TailnetAddressInvalid { address }
            }
        }
    }
}
