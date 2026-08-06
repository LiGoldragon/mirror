//! The mirror daemon's runtime configuration wrapper.
//!
//! The typed configuration record `DaemonConfiguration` lives in the
//! `meta-signal-mirror` contract; the daemon decodes it from its single
//! binary rkyv startup argument and wraps it here so the daemon crate can
//! implement `triad_runtime::BindingSurface` (a foreign trait on a foreign
//! type otherwise) and pre-parse the tailnet listen address once.

use std::net::SocketAddr;
use std::path::Path;

use meta_signal_mirror::{ConfigurationArchiveError, z2VXab};
use thiserror::Error;
use triad_runtime::{BindingSurface, SocketMode};

/// The daemon-local configuration: the decoded contract record plus the
/// parsed tailnet socket address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Configuration {
    contract: z2VXab,
    tcp_listen_address: SocketAddr,
    working_socket_mode: SocketMode,
    meta_socket_mode: SocketMode,
}

impl Configuration {
    /// Decode the daemon's single binary startup argument.
    pub fn from_binary_path(path: &Path) -> Result<Self, ConfigurationError> {
        z2VXab::from_binary_path(path)?.try_into()
    }

    pub fn contract(&self) -> &z2VXab {
        &self.contract
    }

    pub fn storage_path(&self) -> &Path {
        self.contract.field_0.as_path()
    }

    /// The tailnet-bound TCP ingress address (Spirit rj9y). The runtime
    /// binds whatever address is configured; the tailnet trust boundary is
    /// the deployment's bind choice, never a payload claim.
    pub fn tcp_listen_address(&self) -> SocketAddr {
        self.tcp_listen_address
    }
}

impl TryFrom<z2VXab> for Configuration {
    type Error = ConfigurationError;

    fn try_from(contract: z2VXab) -> Result<Self, Self::Error> {
        let host = contract.field_3.field_0.as_str();
        let port = contract.field_3.field_1.clone().into_u16();
        let address = format!("{host}:{port}");
        let ip_address = host
            .parse()
            .map_err(|_| ConfigurationError::ListenAddressInvalid {
                address: address.clone(),
            })?;
        let tcp_listen_address = SocketAddr::new(ip_address, port);
        let working_socket_mode =
            SocketMode::new(contract.field_1.payload().field_1.clone().into_u32()?);
        let meta_socket_mode =
            SocketMode::new(contract.field_2.payload().field_1.clone().into_u32()?);
        Ok(Self {
            contract,
            tcp_listen_address,
            working_socket_mode,
            meta_socket_mode,
        })
    }
}

impl BindingSurface for Configuration {
    fn socket_path(&self) -> &Path {
        std::path::Path::new(self.contract.field_1.payload().field_0.as_str())
    }

    fn socket_mode(&self) -> Option<SocketMode> {
        Some(self.working_socket_mode)
    }

    fn meta_socket_path(&self) -> Option<&Path> {
        Some(std::path::Path::new(
            self.contract.field_2.payload().field_0.as_str(),
        ))
    }

    fn meta_socket_mode(&self) -> Option<SocketMode> {
        Some(self.meta_socket_mode)
    }

    fn database_path(&self) -> &Path {
        self.contract.field_0.as_path()
    }
}

#[derive(Debug, Error)]
pub enum ConfigurationError {
    #[error("binary configuration archive: {0}")]
    Archive(#[from] ConfigurationArchiveError),

    #[error("socket mode is outside the substrate range")]
    SocketMode(#[from] meta_signal_mirror::SocketModeRangeError),

    #[error("tcp listen address is not a socket address: {address}")]
    ListenAddressInvalid { address: String },
}
