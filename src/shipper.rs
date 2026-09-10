//! Component-side transport from a sema-engine outbox to Mirror.

use std::net::SocketAddr;
use std::sync::Arc;

use sema_engine::{
    Engine as ComponentEngine, MirrorHead, VersionedCommitLogEntry, VersionedStoreName,
};
use signal_mirror::{
    ByteViewable, Query, Response, Restorable, Signal, Signalizable, z2VLxP, z2VPuU, z2VSAK,
    z2VTXE, z2VTq5, z2VUKn, z2VUwg, z2VUxk, z2VcqM, z2Ve8p,
};
use signal_standard::z2VSyM;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use triad_runtime::kameo::actor::{Actor, ActorRef};
use triad_runtime::kameo::message::{Context, Message};
use triad_runtime::{FrameBody, LengthPrefixedCodec};

use crate::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MirrorTailnetClient {
    address: SocketAddr,
}

impl MirrorTailnetClient {
    pub fn new(address: SocketAddr) -> Self {
        Self { address }
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub async fn exchange(&self, input: Query) -> Result<Response> {
        let codec = LengthPrefixedCodec::default();
        let mut stream = TcpStream::connect(self.address).await?;
        codec
            .write_body_async(
                &mut stream,
                &FrameBody::new(
                    input
                        .signalize()
                        .map_err(|error| Error::Archive(error.to_string()))?
                        .bytes()
                        .to_vec(),
                ),
            )
            .await?;
        stream.flush().await?;
        let reply = codec.read_body_async(&mut stream).await?;
        Signal::<Response>::from(reply.into_bytes())
            .restore()
            .map_err(|error| Error::Archive(error.to_string()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShipOutcome {
    AlreadyCommitted { head: Option<MirrorHead> },
    Shipped { head: MirrorHead },
}

pub struct ComponentShipper {
    engine: Arc<ComponentEngine>,
    client: MirrorTailnetClient,
    store_name: z2Ve8p,
}

impl ComponentShipper {
    pub fn new(
        engine: ComponentEngine,
        mirror_address: SocketAddr,
        store_name: VersionedStoreName,
    ) -> Self {
        Self::with_client(engine, MirrorTailnetClient::new(mirror_address), store_name)
    }

    pub fn with_client(
        engine: ComponentEngine,
        client: MirrorTailnetClient,
        store_name: VersionedStoreName,
    ) -> Self {
        Self::with_shared_client(Arc::new(engine), client, store_name)
    }

    pub fn from_shared_engine(
        engine: Arc<ComponentEngine>,
        mirror_address: SocketAddr,
        store_name: VersionedStoreName,
    ) -> Self {
        Self::with_shared_client(engine, MirrorTailnetClient::new(mirror_address), store_name)
    }

    pub fn with_shared_client(
        engine: Arc<ComponentEngine>,
        client: MirrorTailnetClient,
        store_name: VersionedStoreName,
    ) -> Self {
        Self {
            engine,
            client,
            store_name: store_name.as_str().to_owned(),
        }
    }

    pub fn engine(&self) -> &ComponentEngine {
        &self.engine
    }

    pub fn shared_engine(&self) -> Arc<ComponentEngine> {
        Arc::clone(&self.engine)
    }

    pub fn client(&self) -> MirrorTailnetClient {
        self.client
    }

    pub fn store_name(&self) -> &String {
        &self.store_name
    }

    pub fn envelope_for_entry(
        &self,
        entry: &VersionedCommitLogEntry,
    ) -> Result<signal_mirror::EntryEnvelope> {
        let payload = rkyv::to_bytes::<rkyv::rancor::Error>(entry).map_err(|source| {
            Error::PayloadEncode {
                surface: "versioned entry",
                message: source.to_string(),
            }
        })?;
        Ok(signal_mirror::EntryEnvelope {
            commit_sequence: i64::try_from(entry.commit_sequence().value()).map_err(|error| {
                Error::PayloadEncode {
                    surface: "entry sequence",
                    message: error.to_string(),
                }
            })?,
            object_digest_option: entry
                .previous_entry_digest()
                .map(|digest| digest_text(digest.bytes())),
            object_digest: digest_text(entry.entry_digest().bytes()),
            payload_bytes: payload.iter().map(|value| i64::from(*value)).collect(),
        })
    }

    pub fn expected_head(&self) -> Result<Option<signal_mirror::HeadMark>> {
        self.engine
            .mirror_head()?
            .map(|head| {
                Ok(signal_mirror::HeadMark {
                    commit_sequence: i64::try_from(head.commit_sequence().value()).map_err(
                        |error| Error::PayloadEncode {
                            surface: "head sequence",
                            message: error.to_string(),
                        },
                    )?,
                    object_digest: digest_text(head.entry_digest().bytes()),
                })
            })
            .transpose()
    }

    pub async fn ship_unshipped(&self) -> Result<ShipOutcome> {
        let outbox = self.engine.unshipped_outbox()?;
        let Some(first) = outbox.first().map(|row| row.commit_sequence()) else {
            return Ok(ShipOutcome::AlreadyCommitted {
                head: self.engine.mirror_head()?,
            });
        };
        let entries = self
            .engine
            .versioned_replay_from_sequence(first)?
            .iter()
            .map(|entry| self.envelope_for_entry(entry))
            .collect::<Result<Vec<_>>>()?;
        if entries.len() != outbox.len() {
            return Err(Error::OutboxSuffixMismatch {
                outbox_rows: outbox.len(),
                replay_entries: entries.len(),
            });
        }

        let output = self
            .client
            .exchange(z2VVny::z2VVjQ(z2VTq5 {
                field_0: self.store_name.clone(),
                field_1: self.expected_head()?,
                field_2: entries,
            }))
            .await?;
        let receipt = match output {
            z2VTqL::z2VXSq(receipt) => receipt,
            z2VTqL::z2VX2Y(rejection) => {
                return Err(Error::MirrorAppendRejected {
                    reason: rejection.field_1,
                    head: rejection.field_2,
                });
            }
            z2VTqL::z2VPpj(report) => {
                return Err(Error::MirrorFaulted {
                    detail: report.payload().payload().clone(),
                });
            }
            other => {
                return Err(Error::UnexpectedMirrorOutput {
                    expected: "Appended",
                    actual: format!("{other:?}"),
                });
            }
        };

        let head = Self::mirror_head_from_mark(&receipt.field_1)?;
        self.engine.acknowledge_mirror(head)?;
        Ok(ShipOutcome::Shipped { head })
    }

    pub async fn publish_latest_checkpoint(&self) -> Result<z2VLxP> {
        let checkpoint = self
            .engine
            .latest_checkpoint()?
            .ok_or(Error::CheckpointUnavailable)?;
        let artifact = z2VTXE {
            field_0: self.store_name.clone(),
            field_1: z2VUKn::new(checkpoint.metadata().sequence().value()),
            field_2: z2VSAK::new(checkpoint.metadata().covered().last().value()),
            field_3: z2VSyM::new(digest_text(
                checkpoint.metadata().checkpoint_digest().bytes(),
            )),
            field_4: z2VUxk::from_octets(&checkpoint.to_portable()?.into_bytes()),
        };
        match self.client.exchange(z2VVny::z2VNu6(artifact)).await? {
            z2VTqL::z2VaSa(receipt) => Ok(receipt),
            z2VTqL::z2VWHb(rejection) => Err(Error::MirrorPublishRejected {
                reason: rejection.field_1,
            }),
            z2VTqL::z2VPpj(report) => Err(Error::MirrorFaulted {
                detail: report.payload().payload().clone(),
            }),
            other => Err(Error::UnexpectedMirrorOutput {
                expected: "CheckpointPublished",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn mirror_head_from_mark(mark: &z2VcqM) -> Result<MirrorHead> {
        Ok(MirrorHead::new(
            sema_engine::CommitSequence::new(*mark.field_0.payload()),
            sema_engine::EntryDigest::new(parse_digest(mark.field_1.as_str())?),
        ))
    }
}

fn digest_text(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn parse_digest(text: &str) -> Result<[u8; 32]> {
    let hexadecimal = text.strip_prefix("blake3:").unwrap_or(text);
    if hexadecimal.len() != 64 {
        return Err(Error::MirrorDigestInvalid {
            digest: text.to_owned(),
        });
    }
    let mut bytes = [0_u8; 32];
    for (offset, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hexadecimal[offset * 2..offset * 2 + 2], 16).map_err(|_| {
            Error::MirrorDigestInvalid {
                digest: text.to_owned(),
            }
        })?;
    }
    Ok(bytes)
}

impl Actor for ComponentShipper {
    type Args = Self;
    type Error = Error;

    async fn on_start(shipper: Self::Args, _actor_reference: ActorRef<Self>) -> Result<Self> {
        Ok(shipper)
    }
}

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

pub struct PublishLatestCheckpoint;

impl Message<PublishLatestCheckpoint> for ComponentShipper {
    type Reply = Result<z2VLxP>;

    async fn handle(
        &mut self,
        _message: PublishLatestCheckpoint,
        _context: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.publish_latest_checkpoint().await
    }
}
