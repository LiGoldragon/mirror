//! The mirror service actor — the one component runtime both transports
//! share.
//!
//! `Service` is the kameo actor that owns `Engine` (and so
//! the single-writer durable store). The Unix daemon runtime holds a
//! `ServiceLink` and forwards working and meta requests into this mailbox;
//! the tailnet TCP ingress
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

use meta_signal_mirror::{z2VUH6 as MetaOutput, z2VWt5 as MetaInput};
use signal_mirror::{z2VTqL as WorkingOutput, z2VVny as WorkingInput};
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

use crate::engine::Engine;
use crate::error::{Error, Result};

/// The mirror's component runtime: the engine, the configured tailnet
/// ingress address, and the live listener state once started.
pub struct Service {
    engine: Engine,
    tcp_listen_address: SocketAddr,
    tcp_bound_address: Option<SocketAddr>,
    tcp_listener_task: Option<tokio::task::JoinHandle<()>>,
    tcp_peer_witness: TcpPeerWitness,
}

impl Service {
    pub fn new(engine: Engine, tcp_listen_address: SocketAddr) -> Self {
        Self {
            engine,
            tcp_listen_address,
            tcp_bound_address: None,
            tcp_listener_task: None,
            tcp_peer_witness: TcpPeerWitness::default(),
        }
    }
}

/// The bounded tailnet-traffic witness: how many TCP-borne working
/// requests the service handled and the most recent typed peer. O(1)
/// state per service — the witness surface for tests without an
/// unbounded production peer list.
#[derive(Clone, Copy, Debug, Default)]
pub struct TcpPeerWitness {
    served_request_count: u64,
    last_peer: Option<PeerIdentity>,
}

/// Hand-written because the `kameo::Reply` derive emits absolute
/// `::kameo` paths and the mirror reaches kameo only through
/// `triad_runtime`'s re-export.
impl triad_runtime::kameo::Reply for TcpPeerWitness {
    type Ok = Self;
    type Error = triad_runtime::kameo::error::Infallible;
    type Value = Self;

    fn to_result(self) -> std::result::Result<Self::Ok, Self::Error> {
        Ok(self)
    }

    fn into_any_err(self) -> Option<Box<dyn triad_runtime::kameo::reply::ReplyError>> {
        None
    }

    fn into_value(self) -> Self::Value {
        self
    }
}

impl TcpPeerWitness {
    pub fn served_request_count(&self) -> u64 {
        self.served_request_count
    }

    pub fn last_peer(&self) -> Option<PeerIdentity> {
        self.last_peer
    }

    fn observe(&mut self, peer: PeerIdentity) {
        self.served_request_count += 1;
        self.last_peer = Some(peer);
    }
}

impl Actor for Service {
    type Args = Self;
    type Error = Error;

    /// Bind the tailnet TCP ingress around this actor's own reference and
    /// serve it from a background task. The ordinary daemon runtime binds the
    /// Unix tiers; both paths converge on this actor.
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
    input: WorkingInput,
    context: ConnectionContext,
}

impl WorkingSignal {
    pub fn new(input: WorkingInput, context: ConnectionContext) -> Self {
        Self { input, context }
    }
}

impl Message<WorkingSignal> for Service {
    type Reply = Result<WorkingOutput>;

    async fn handle(
        &mut self,
        message: WorkingSignal,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if let PeerIdentity::Tcp(_) = message.context.peer() {
            self.tcp_peer_witness.observe(*message.context.peer());
        }
        Ok(self.engine.handle(message.input).await)
    }
}

/// One decoded meta order. Only the Unix daemon's meta tier
/// constructs this message — the TCP ingress cannot, structurally.
pub struct MetaOrder {
    input: MetaInput,
}

impl MetaOrder {
    pub fn new(input: MetaInput) -> Self {
        Self { input }
    }
}

impl Message<MetaOrder> for Service {
    type Reply = Result<MetaOutput>;

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

impl Message<TcpAddressQuery> for Service {
    type Reply = Option<SocketAddr>;

    async fn handle(
        &mut self,
        _message: TcpAddressQuery,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.tcp_bound_address
    }
}

/// Observe the bounded TCP traffic witness — the proof that tailnet
/// traffic arrives as typed `PeerIdentity::Tcp`.
pub struct TcpPeerWitnessQuery;

impl Message<TcpPeerWitnessQuery> for Service {
    type Reply = TcpPeerWitness;

    async fn handle(
        &mut self,
        _message: TcpPeerWitnessQuery,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.tcp_peer_witness
    }
}

/// The cloneable handle both transports hold on the one mirror service.
/// The daemon's `ComponentDaemon::Engine` is this link; the
/// TCP ingress holds the same `ActorRef`.
#[derive(Clone)]
pub struct ServiceLink {
    service: ActorRef<Service>,
}

impl ServiceLink {
    pub fn new(service: ActorRef<Service>) -> Self {
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
        input: WorkingInput,
        context: ConnectionContext,
    ) -> Result<WorkingOutput> {
        self.service
            .ask(WorkingSignal::new(input, context))
            .await
            .map_err(Self::fallible)
    }

    pub async fn meta(&self, input: MetaInput) -> Result<MetaOutput> {
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

    pub async fn tcp_peer_witness(&self) -> Result<TcpPeerWitness> {
        self.service
            .ask(TcpPeerWitnessQuery)
            .await
            .map_err(|_| Error::ServiceUnavailable)
    }
}

/// The tailnet TCP ingress: the same length-prefixed frame codec and the same
/// signal-frame working contract as the Unix working tier, forwarding into
/// the same service mailbox. One request frame per connection, mirroring the
/// working
/// transport's request shape.
pub struct TailnetIngress {
    service: ActorRef<Service>,
    codec: LengthPrefixedCodec,
}

impl TailnetIngress {
    pub fn new(service: ActorRef<Service>) -> Self {
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
        let (exchange, input) =
            signal_mirror::ContractMarker::decode_single_request(&body.into_bytes())?;
        let context = *connection.context();
        let output = ServiceLink::new(self.service.clone())
            .working(input, context)
            .await?;
        self.codec
            .write_body_async(
                connection.stream_mut(),
                &FrameBody::new(output.encode_reply_frame(exchange)?),
            )
            .await?;
        connection.stream_mut().flush().await?;
        Ok(())
    }
}
