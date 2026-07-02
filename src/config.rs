//! The mirror daemon's runtime configuration wrapper.
//!
//! The typed configuration record `DaemonConfiguration` lives in the
//! `meta-signal-mirror` contract; the daemon decodes it from its single
//! binary rkyv startup argument and wraps it here so the daemon crate can
//! implement `triad_runtime::BindingSurface` (a foreign trait on a foreign
//! type otherwise) and pre-parse the tailnet listen address once.
//!
//! The trust boundary for the TCP ingress (Spirit rj9y) is enforced at the
//! bind address: the daemon MUST bind to a specific tailnet interface, never to
//! the unspecified address (0.0.0.0 / ::), which would accept connections from
//! any reachable peer. Startup fails with [`ConfigurationError::ListenAddressUnspecified`]
//! if the configured address is unspecified, so a misconfigured deployment is loud
//! rather than silently open. A specific tailnet IP (e.g. `100.x.y.z:7474`)
//! keeps the kernel as the trust boundary enforcer.

use std::net::SocketAddr;
use std::path::Path;

use meta_signal_mirror::{ConfigurationArchiveError, DaemonConfiguration};
use thiserror::Error;
use triad_runtime::{BindingSurface, SocketMode};

/// The daemon-local configuration: the decoded contract record plus the
/// parsed tailnet socket address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Configuration {
    contract: DaemonConfiguration,
    tcp_listen_address: SocketAddr,
}

impl Configuration {
    /// Decode the daemon's single binary startup argument.
    pub fn from_binary_path(path: &Path) -> Result<Self, ConfigurationError> {
        DaemonConfiguration::from_binary_path(path)?.try_into()
    }

    pub fn contract(&self) -> &DaemonConfiguration {
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

impl TryFrom<DaemonConfiguration> for Configuration {
    type Error = ConfigurationError;

    fn try_from(contract: DaemonConfiguration) -> Result<Self, Self::Error> {
        let address = contract.tcp_listen_address.as_str();
        let tcp_listen_address: SocketAddr =
            address
                .parse()
                .map_err(|_| ConfigurationError::ListenAddressInvalid {
                    address: address.to_owned(),
                })?;
        // Enforce the tailnet trust boundary: an unspecified address (0.0.0.0 or
        // ::) binds all interfaces and defeats the kernel-level isolation the
        // tailnet bind provides. Reject it at startup so the misconfiguration is
        // loud rather than silently exposing the ingress to every reachable peer.
        if tcp_listen_address.ip().is_unspecified() {
            return Err(ConfigurationError::ListenAddressUnspecified {
                address: address.to_owned(),
            });
        }
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

    #[error(
        "tcp listen address {address} is unspecified (0.0.0.0 or ::); \
        configure the tailnet interface address to enforce the trust boundary"
    )]
    ListenAddressUnspecified { address: String },
}

#[cfg(test)]
mod tests {
    use meta_signal_mirror::{DaemonConfiguration, ListenAddress, SocketMode, WirePath};

    use super::*;

    fn make_contract(tcp_address: &str) -> DaemonConfiguration {
        DaemonConfiguration {
            storage_path: WirePath::new("/tmp/mirror.sema"),
            working_socket_path: WirePath::new("/tmp/mirror.sock"),
            working_socket_mode: SocketMode::new(0o660),
            meta_socket_path: WirePath::new("/tmp/mirror-meta.sock"),
            meta_socket_mode: SocketMode::new(0o600),
            tcp_listen_address: ListenAddress::new(tcp_address),
        }
    }

    #[test]
    fn rejects_ipv4_unspecified_address() {
        let contract = make_contract("0.0.0.0:7474");
        let result = Configuration::try_from(contract);
        assert!(
            matches!(result, Err(ConfigurationError::ListenAddressUnspecified { .. })),
            "expected ListenAddressUnspecified, got {result:?}"
        );
    }

    #[test]
    fn rejects_ipv6_unspecified_address() {
        let contract = make_contract("[::]:7474");
        let result = Configuration::try_from(contract);
        assert!(
            matches!(result, Err(ConfigurationError::ListenAddressUnspecified { .. })),
            "expected ListenAddressUnspecified, got {result:?}"
        );
    }

    #[test]
    fn accepts_specific_tailnet_address() {
        let contract = make_contract("127.0.0.1:7474");
        let result = Configuration::try_from(contract);
        assert!(result.is_ok(), "expected Ok for loopback, got {result:?}");
    }
}
