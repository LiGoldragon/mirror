//! The mirror service actor — the one component runtime both transports
//! share.
//!
//! `MirrorService` is the kameo actor that owns `MirrorEngine` (and so
//! the single-writer durable store). The generated Unix daemon's
//! `EngineActor` holds a `ServiceLink` and forwards working and meta
//! requests into this mailbox; the hand-wired tailnet TCP ingress
//! (`TailnetIngress`, spawned by this actor's own `on_start`) forwards
//! the same typed working requests into the same mailbox. Every request
//! from every transport serialises through one actor — the single
//! writer is structural.
//!
//! Trust shape (Spirit rj9y, this cut): TCP peers carry
//! `PeerIdentity::Tcp` and are tailnet-trusted WORKING traffic. The TCP
//! ingress decodes only the ordinary `signal-mirror` contract, so a TCP
//! peer structurally cannot reach the meta surface — meta stays on the
//! kernel-vouched Unix owner socket.

use std::net::SocketAddr;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use triad_runtime::kameo::Actor;
use triad_runtime::kameo::actor::{ActorRef, WeakActorRef};
use triad_runtime::kameo::error::{ActorStopReason, SendError};
use triad_runtime::kameo::message::{Context, Message};
use triad_runtime::{
    AcceptedConnection, AsyncConnectionRuntime, ConnectionContext, FrameBody, LengthPrefixedCodec,
    PeerIdentity, RequestErrorLog, TcpListenerDaemon,
};

use crate::engine::MirrorEngine;
use crate::error::{Error, Result};

/// The mirror's component runtime: the engine, the configured tailnet
/// ingress address, and the live listener state once started.
pub struct MirrorService {
    engine: MirrorEngine,
    tcp_listen_address: SocketAddr,
    tcp_bound_address: Option<SocketAddr>,
    tcp_listener_task: Option<tokio::task::JoinHandle<()>>,
    observed_tcp_peers: Vec<PeerIdentity>,
}

impl MirrorService {
    pub fn new(engine: MirrorEngine, tcp_listen_address: SocketAddr) -> Self {
        Self {
            engine,
            tcp_listen_address,
            tcp_bound_address: None,
            tcp_listener_task: None,
            observed_tcp_peers: Vec::new(),
        }
    }
}

impl Actor for MirrorService {
    type Args = Self;
    type Error = Error;

    /// Bind the hand-wired tailnet TCP ingress around this actor's own
    /// reference and serve it from a background task. The Unix tiers are
    /// bound by the generated daemon shell; the TCP tier is the first
    /// hand-wired consumer of `triad_runtime::TcpListenerDaemon`
    /// (schema-rust-next does not emit TCP daemons yet).
    async fn on_start(mut actor: Self::Args, actor_reference: ActorRef<Self>) -> Result<Self> {
        let ingress = TailnetIngress::new(actor_reference);
        let listener = TcpListenerDaemon::new(
            actor.tcp_listen_address,
            ingress,
            RequestErrorLog::new("mirror-daemon-tailnet"),
        )
        .bind()
        .await?;
        actor.tcp_bound_address = Some(listener.local_address()?);
        let error_log = RequestErrorLog::new("mirror-daemon-tailnet");
        actor.tcp_listener_task = Some(tokio::spawn(async move {
            if let Err(error) = listener.serve_connections().await {
                error_log.report(&error);
            }
        }));
        Ok(actor)
    }

    async fn on_stop(
        &mut self,
        _actor_reference: WeakActorRef<Self>,
        _reason: ActorStopReason,
    ) -> Result<()> {
        if let Some(task) = self.tcp_listener_task.take() {
            task.abort();
        }
        Ok(())
    }
}

/// One decoded working request, from either transport. The carried
/// connection context says what the transport vouches for; this cut
/// treats both kernel-vouched Unix peers and tailnet TCP peers as
/// working traffic (Spirit rj9y — no per-request auth; criome deferred).
pub struct WorkingSignal {
    input: signal_mirror::Input,
    context: ConnectionContext,
}

impl WorkingSignal {
    pub fn new(input: signal_mirror::Input, context: ConnectionContext) -> Self {
        Self { input, context }
    }
}

impl Message<WorkingSignal> for MirrorService {
    type Reply = Result<signal_mirror::Output>;

    async fn handle(
        &mut self,
        message: WorkingSignal,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if let PeerIdentity::Tcp(_) = message.context.peer() {
            self.observed_tcp_peers.push(*message.context.peer());
        }
        Ok(self.engine.handle(message.input).await)
    }
}

/// One decoded meta order. Only the generated Unix daemon's meta tier
/// constructs this message — the TCP ingress cannot, structurally.
pub struct MetaOrder {
    input: meta_signal_mirror::Input,
}

impl MetaOrder {
    pub fn new(input: meta_signal_mirror::Input) -> Self {
        Self { input }
    }
}

impl Message<MetaOrder> for MirrorService {
    type Reply = Result<meta_signal_mirror::Output>;

    async fn handle(
        &mut self,
        message: MetaOrder,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        Ok(self.engine.handle_meta(message.input))
    }
}

/// Observe the bound tailnet ingress address (port-zero deployments and
/// witnesses read the operating-system-assigned port here).
pub struct TcpAddressQuery;

impl Message<TcpAddressQuery> for MirrorService {
    type Reply = Option<SocketAddr>;

    async fn handle(
        &mut self,
        _message: TcpAddressQuery,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.tcp_bound_address
    }
}

/// Observe the typed peer identities the TCP ingress carried — the
/// witness that tailnet traffic arrives as `PeerIdentity::Tcp`.
pub struct ObservedTcpPeers;

impl Message<ObservedTcpPeers> for MirrorService {
    type Reply = Vec<PeerIdentity>;

    async fn handle(
        &mut self,
        _message: ObservedTcpPeers,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.observed_tcp_peers.clone()
    }
}

/// The cloneable handle both transports hold on the one mirror service.
/// The generated daemon's `ComponentDaemon::Engine` is this link; the
/// TCP ingress holds the same `ActorRef`.
#[derive(Clone)]
pub struct ServiceLink {
    service: ActorRef<MirrorService>,
}

impl ServiceLink {
    pub fn new(service: ActorRef<MirrorService>) -> Self {
        Self { service }
    }

    fn fallible<Request>(error: SendError<Request, Error>) -> Error {
        match error {
            SendError::HandlerError(error) => error,
            _ => Error::ServiceUnavailable,
        }
    }

    pub async fn working(
        &self,
        input: signal_mirror::Input,
        context: ConnectionContext,
    ) -> Result<signal_mirror::Output> {
        self.service
            .ask(WorkingSignal::new(input, context))
            .await
            .map_err(Self::fallible)
    }

    pub async fn meta(
        &self,
        input: meta_signal_mirror::Input,
    ) -> Result<meta_signal_mirror::Output> {
        self.service
            .ask(MetaOrder::new(input))
            .await
            .map_err(Self::fallible)
    }

    pub async fn tcp_bound_address(&self) -> Result<Option<SocketAddr>> {
        self.service
            .ask(TcpAddressQuery)
            .await
            .map_err(|_| Error::ServiceUnavailable)
    }

    pub async fn observed_tcp_peers(&self) -> Result<Vec<PeerIdentity>> {
        self.service
            .ask(ObservedTcpPeers)
            .await
            .map_err(|_| Error::ServiceUnavailable)
    }
}

/// The hand-wired tailnet TCP ingress: the same length-prefixed frame
/// codec and the same signal-frame working contract as the generated
/// Unix working tier, forwarding into the same service mailbox. One
/// request frame per connection, mirroring the generated working
/// transport's request shape.
pub struct TailnetIngress {
    service: ActorRef<MirrorService>,
    codec: LengthPrefixedCodec,
}

impl TailnetIngress {
    pub fn new(service: ActorRef<MirrorService>) -> Self {
        Self {
            service,
            codec: LengthPrefixedCodec::default(),
        }
    }
}

impl AsyncConnectionRuntime<TcpStream> for TailnetIngress {
    type Error = Error;

    async fn handle_connection(&self, mut connection: AcceptedConnection<TcpStream>) -> Result<()> {
        let body = self.codec.read_body_async(connection.stream_mut()).await?;
        let (_route, input) = signal_mirror::Input::decode_signal_frame(&body.into_bytes())?;
        let context = *connection.context();
        let output = ServiceLink::new(self.service.clone())
            .working(input, context)
            .await?;
        self.codec
            .write_body_async(
                connection.stream_mut(),
                &FrameBody::new(output.encode_signal_frame()?),
            )
            .await?;
        connection.stream_mut().flush().await?;
        Ok(())
    }
}
