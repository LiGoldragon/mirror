//! Component-side mirror shipper: turns a component sema-engine
//! outbox suffix into payload-blind mirror frames, sends them to the
//! tailnet mirror ingress, and records the server-confirmed head back
//! into the component store.

use std::net::SocketAddr;
use std::sync::Arc;

use sema_engine::{
    Engine as ComponentEngine, MirrorHead, VersionedCommitLogEntry, VersionedStoreName,
};
use signal_mirror::{
    ArtifactBytes, ArtifactDigest, Bytes, CheckpointArtifact, CheckpointReceipt,
    CheckpointSequence, CommitSequence, EntryDigest, EntryEnvelope, EntrySuffix, FixedBytes,
    HeadMark, Input, Output, PayloadBytes, StoreName,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use triad_runtime::kameo::actor::{Actor, ActorRef};
use triad_runtime::kameo::message::{Context, Message};
use triad_runtime::{FrameBody, LengthPrefixedCodec};

use crate::error::{Error, Result};

/// Tailnet working-signal client for the mirror daemon. The mirror's
/// meta surface is intentionally absent: component shippers only append
/// working history and publish checkpoint artifacts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TailnetClient {
    address: SocketAddr,
}

impl TailnetClient {
    pub fn new(address: SocketAddr) -> Self {
        Self { address }
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub async fn exchange(&self, input: Input) -> Result<Output> {
        let codec = LengthPrefixedCodec::default();
        let mut stream = TcpStream::connect(self.address).await?;
        codec
            .write_body_async(&mut stream, &FrameBody::new(input.encode_signal_frame()?))
            .await?;
        stream.flush().await?;
        let reply = codec.read_body_async(&mut stream).await?;
        let (_route, output) = Output::decode_signal_frame(&reply.into_bytes())?;
        Ok(output)
    }
}

/// Result of one ship attempt over the component's durable outbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShipOutcome {
    AlreadyCommitted { head: Option<MirrorHead> },
    Shipped { head: MirrorHead },
}

/// The reusable production shipper. It can be used directly by a
/// component's existing engine-owning actor, or spawned as a Kameo
/// actor when the shipper owns the component engine itself.
pub struct ComponentShipper {
    engine: Arc<ComponentEngine>,
    client: TailnetClient,
    store_name: StoreName,
}

impl ComponentShipper {
    pub fn new(
        engine: Arc<ComponentEngine>,
        mirror_address: SocketAddr,
        store_name: VersionedStoreName,
    ) -> Self {
        Self::with_client(engine, TailnetClient::new(mirror_address), store_name)
    }

    pub fn with_client(
        engine: Arc<ComponentEngine>,
        client: TailnetClient,
        store_name: VersionedStoreName,
    ) -> Self {
        Self {
            engine,
            client,
            store_name: StoreName::new(store_name.as_str().to_owned()),
        }
    }

    pub fn engine(&self) -> &ComponentEngine {
        &self.engine
    }

    pub fn client(&self) -> TailnetClient {
        self.client
    }

    pub fn store_name(&self) -> &StoreName {
        &self.store_name
    }

    pub fn envelope_for_entry(&self, entry: &VersionedCommitLogEntry) -> Result<EntryEnvelope> {
        Ok(EntryEnvelope {
            sequence: CommitSequence::new(entry.commit_sequence().value()),
            previous_digest: entry
                .previous_entry_digest()
                .map(|digest| EntryDigest::new(FixedBytes::new(*digest.bytes()))),
            digest: EntryDigest::new(FixedBytes::new(*entry.entry_digest().bytes())),
            payload: PayloadBytes::new(Bytes::new(
                rkyv::to_bytes::<rkyv::rancor::Error>(entry)
                    .map_err(|source| Error::PayloadEncode {
                        surface: "versioned entry",
                        message: source.to_string(),
                    })?
                    .to_vec(),
            )),
        })
    }

    pub fn expected_head(&self) -> Result<Option<HeadMark>> {
        Ok(self.engine.mirror_head()?.map(|head| HeadMark {
            sequence: CommitSequence::new(head.commit_sequence().value()),
            digest: EntryDigest::new(FixedBytes::new(*head.entry_digest().bytes())),
        }))
    }

    pub async fn ship_unshipped(&self) -> Result<ShipOutcome> {
        let outbox = self.engine.unshipped_outbox()?;
        let Some(first) = outbox.first().map(|row| row.commit_sequence()) else {
            return Ok(ShipOutcome::AlreadyCommitted {
                head: self.engine.mirror_head()?,
            });
        };
        let entries: Vec<EntryEnvelope> = self
            .engine
            .versioned_replay_from_sequence(first)?
            .iter()
            .map(|entry| self.envelope_for_entry(entry))
            .collect::<Result<_>>()?;
        if entries.len() != outbox.len() {
            return Err(Error::OutboxSuffixMismatch {
                outbox_rows: outbox.len(),
                replay_entries: entries.len(),
            });
        }

        let output = self
            .client
            .exchange(Input::Append(EntrySuffix {
                store: self.store_name.clone(),
                expected_head: self.expected_head()?,
                entries,
            }))
            .await?;
        let receipt = match output {
            Output::Appended(receipt) => receipt,
            Output::AppendRejected(rejection) => {
                return Err(Error::MirrorAppendRejected {
                    reason: rejection.reason,
                    head: rejection.head,
                });
            }
            Output::MirrorFaulted(report) => {
                return Err(Error::MirrorFaulted {
                    detail: format!("{report:?}"),
                });
            }
            other => {
                return Err(Error::UnexpectedMirrorOutput {
                    expected: "Appended",
                    actual: format!("{other:?}"),
                });
            }
        };

        let head = Self::mirror_head_from_mark(&receipt.head);
        self.engine.acknowledge_mirror(head)?;
        Ok(ShipOutcome::Shipped { head })
    }

    pub async fn publish_latest_checkpoint(&self) -> Result<CheckpointReceipt> {
        let checkpoint = self
            .engine
            .latest_checkpoint()?
            .ok_or(Error::CheckpointUnavailable)?;
        let artifact = CheckpointArtifact {
            store: self.store_name.clone(),
            sequence: CheckpointSequence::new(checkpoint.metadata().sequence().value()),
            covered_end: CommitSequence::new(checkpoint.metadata().covered().last().value()),
            digest: ArtifactDigest::new(FixedBytes::new(
                *checkpoint.metadata().checkpoint_digest().bytes(),
            )),
            artifact: ArtifactBytes::new(Bytes::new(checkpoint.to_portable()?.into_bytes())),
        };
        let output = self
            .client
            .exchange(Input::PublishCheckpoint(artifact))
            .await?;
        match output {
            Output::CheckpointPublished(receipt) => Ok(receipt),
            Output::PublishRejected(rejection) => Err(Error::MirrorPublishRejected {
                reason: rejection.reason,
            }),
            Output::MirrorFaulted(report) => Err(Error::MirrorFaulted {
                detail: format!("{report:?}"),
            }),
            other => Err(Error::UnexpectedMirrorOutput {
                expected: "CheckpointPublished",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn mirror_head_from_mark(mark: &HeadMark) -> MirrorHead {
        MirrorHead::new(
            sema_engine::CommitSequence::new(*mark.sequence.payload()),
            sema_engine::EntryDigest::new(*mark.digest.payload().payload()),
        )
    }
}

impl Actor for ComponentShipper {
    type Args = Self;
    type Error = Error;

    async fn on_start(shipper: Self::Args, _actor_reference: ActorRef<Self>) -> Result<Self> {
        Ok(shipper)
    }
}

/// Ask a spawned [`ComponentShipper`] to ship its current outbox suffix.
pub struct ShipUnshipped;

impl Message<ShipUnshipped> for ComponentShipper {
    type Reply = Result<ShipOutcome>;

    async fn handle(
        &mut self,
        _message: ShipUnshipped,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.ship_unshipped().await
    }
}

/// Ask a spawned [`ComponentShipper`] to publish its latest checkpoint.
pub struct PublishLatestCheckpoint;

impl Message<PublishLatestCheckpoint> for ComponentShipper {
    type Reply = Result<CheckpointReceipt>;

    async fn handle(
        &mut self,
        _message: PublishLatestCheckpoint,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.publish_latest_checkpoint().await
    }
}
