//! THE END-TO-END ARC WITNESS (Spirit 29pb, first cut, daemon level).
//!
//! Two engines in one test: a fixture COMPONENT store (sema-engine
//! versioned engine with hand-registered families) takes writes, reads
//! its unshipped mirror outbox, and ships the suffix over REAL loopback
//! TCP frames to a RUNNING mirror service (the daemon's component
//! runtime with its hand-wired tailnet ingress). The mirror persists
//! into its OWN sema-engine store and acknowledges; the component marks
//! the shipped history `ServerCommitted`. Then a FRESH component store
//! restores from the mirror — fetch checkpoint + suffix, engine-owned
//! `ImportSession` — and the normal query surface reads identical
//! records.
//!
//! The component-side shipper lives INSIDE this fixture (workspace test
//! discipline: unblock the blocker in the test); a production shipper
//! actor is a named follow-up.

use std::net::SocketAddr;
use std::path::PathBuf;

use mirror::{MirrorEngine, MirrorService, ServiceLink, Store};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Checkpoint, CheckpointMetadata, CheckpointSegment, Durability,
    Engine as ComponentEngine, EngineOpen, EngineRecord, FamilyDirectory, FamilyName, MirrorHead,
    Mutation, QueryPlan, RecordKey, Retraction, RowMaterializer, SchemaHash, SchemaVersion,
    TableDescriptor, TableName, TableReference, VersionedCommitLogEntry, VersionedStoreName,
    VersioningPolicy,
};
use signal_mirror::{
    ArtifactBytes, ArtifactDigest, Bytes, CheckpointArtifact, CheckpointSequence, CommitSequence,
    EntryDigest, EntryEnvelope, EntrySuffix, FixedBytes, HeadMark, Input, Output, PayloadBytes,
    RestoreBundle, RestoreQuery, StoreName,
};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use triad_runtime::kameo::actor::Spawn;
use triad_runtime::{FrameBody, LengthPrefixedCodec, PeerIdentity};

const COMPONENT_STORE_NAME: &str = "arc-witness";

/// The component's domain record — the mirror never decodes it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
struct Thought {
    key: String,
    body: String,
}

impl Thought {
    fn new(key: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            body: body.into(),
        }
    }
}

impl EngineRecord for Thought {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.key.clone())
    }
}

/// The component's typed family knowledge for import materialization.
struct Families {
    thoughts: TableReference<Thought>,
}

impl Families {
    fn new() -> Self {
        Self {
            thoughts: TableReference::new(TableName::new("thoughts")),
        }
    }
}

impl FamilyDirectory for Families {
    fn materialize(&self, row: RowMaterializer<'_>) -> sema_engine::Result<()> {
        match row.family().family().as_str() {
            "thought" => row.apply(self.thoughts),
            other => Err(sema_engine::Error::FamilyUnknown {
                family: other.to_owned(),
            }),
        }
    }
}

/// The portable byte form of one checkpoint: what the component ships
/// and what the restorer decodes. The mirror stores these bytes blind.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct PortableCheckpoint {
    metadata: CheckpointMetadata,
    segments: Vec<CheckpointSegment>,
}

impl PortableCheckpoint {
    fn from_checkpoint(checkpoint: &Checkpoint) -> Self {
        Self {
            metadata: checkpoint.metadata().clone(),
            segments: checkpoint.segments().to_vec(),
        }
    }

    fn into_checkpoint(self) -> Checkpoint {
        Checkpoint::new(self.metadata, self.segments)
    }
}

/// One loopback frame exchange against the running mirror's TCP ingress
/// — the same length-prefixed codec and signal-frame contract the
/// daemon's Unix working tier speaks.
struct MirrorCall {
    address: SocketAddr,
}

impl MirrorCall {
    fn new(address: SocketAddr) -> Self {
        Self { address }
    }

    async fn exchange(&self, input: Input) -> Output {
        let codec = LengthPrefixedCodec::default();
        let mut stream = TcpStream::connect(self.address)
            .await
            .expect("connect to the mirror's tailnet ingress");
        codec
            .write_body_async(
                &mut stream,
                &FrameBody::new(input.encode_signal_frame().expect("encode request")),
            )
            .await
            .expect("write request frame");
        stream.flush().await.expect("flush request frame");
        let reply = codec
            .read_body_async(&mut stream)
            .await
            .expect("read reply frame");
        let (_route, output) =
            Output::decode_signal_frame(&reply.into_bytes()).expect("decode reply");
        output
    }
}

/// The component-side shipper: reads the unshipped outbox, projects the
/// versioned entries into payload-blind envelopes, ships them over TCP,
/// and acknowledges the server-confirmed head into the durable cursor.
struct Shipper {
    call: MirrorCall,
    store_name: StoreName,
}

impl Shipper {
    fn new(address: SocketAddr) -> Self {
        Self {
            call: MirrorCall::new(address),
            store_name: StoreName::new(COMPONENT_STORE_NAME.to_owned()),
        }
    }

    fn envelope(entry: &VersionedCommitLogEntry) -> EntryEnvelope {
        EntryEnvelope {
            sequence: CommitSequence::new(entry.commit_sequence().value()),
            previous_digest: entry
                .previous_entry_digest()
                .map(|digest| EntryDigest::new(FixedBytes::new(*digest.bytes()))),
            digest: EntryDigest::new(FixedBytes::new(*entry.entry_digest().bytes())),
            payload: PayloadBytes::new(Bytes::new(
                rkyv::to_bytes::<rkyv::rancor::Error>(entry)
                    .expect("encode versioned entry payload")
                    .to_vec(),
            )),
        }
    }

    fn expected_head(component: &ComponentEngine) -> Option<HeadMark> {
        component
            .mirror_head()
            .expect("read durable shipped cursor")
            .map(|head| HeadMark {
                sequence: CommitSequence::new(head.commit_sequence().value()),
                digest: EntryDigest::new(FixedBytes::new(*head.entry_digest().bytes())),
            })
    }

    /// Ship every unshipped outbox row and acknowledge the confirmed
    /// head. Returns the acknowledged head.
    async fn ship(&self, component: &ComponentEngine) -> MirrorHead {
        let outbox = component.unshipped_outbox().expect("read unshipped outbox");
        assert!(!outbox.is_empty(), "the outbox holds unshipped history");
        let first_unshipped = outbox[0].commit_sequence();
        let entries: Vec<EntryEnvelope> = component
            .versioned_replay_from_sequence(first_unshipped)
            .expect("load versioned entries for the outbox suffix")
            .iter()
            .map(Self::envelope)
            .collect();
        assert_eq!(entries.len(), outbox.len());

        let reply = self
            .call
            .exchange(Input::Append(EntrySuffix {
                store: self.store_name.clone(),
                expected_head: Self::expected_head(component),
                entries,
            }))
            .await;
        let receipt = match reply {
            Output::Appended(receipt) => receipt,
            other => panic!("expected Appended, got {other:?}"),
        };

        let confirmed = MirrorHead::new(
            sema_engine::CommitSequence::new(receipt.head.sequence.clone().into_u64()),
            sema_engine::EntryDigest::new(*receipt.head.digest.as_bytes()),
        );
        component
            .acknowledge_mirror(confirmed)
            .expect("acknowledge the server-confirmed head");
        confirmed
    }

    /// Publish the component's latest checkpoint as a payload-blind
    /// artifact.
    async fn publish_checkpoint(&self, component: &ComponentEngine) {
        let checkpoint = component
            .latest_checkpoint()
            .expect("load latest checkpoint")
            .expect("a checkpoint exists");
        let artifact = CheckpointArtifact {
            store: self.store_name.clone(),
            sequence: CheckpointSequence::new(checkpoint.metadata().sequence().value()),
            covered_end: CommitSequence::new(checkpoint.metadata().covered().last().value()),
            digest: ArtifactDigest::new(FixedBytes::new(
                *checkpoint.metadata().checkpoint_digest().bytes(),
            )),
            artifact: ArtifactBytes::new(Bytes::new(
                rkyv::to_bytes::<rkyv::rancor::Error>(&PortableCheckpoint::from_checkpoint(
                    &checkpoint,
                ))
                .expect("encode checkpoint artifact")
                .to_vec(),
            )),
        };
        let reply = self.call.exchange(Input::PublishCheckpoint(artifact)).await;
        assert!(
            matches!(reply, Output::CheckpointPublished(_)),
            "expected CheckpointPublished, got {reply:?}"
        );
    }
}

/// The component-side restorer: fetch checkpoint + suffix from the
/// mirror and import them into a fresh store.
struct Restorer {
    call: MirrorCall,
    store_name: StoreName,
}

impl Restorer {
    fn new(address: SocketAddr) -> Self {
        Self {
            call: MirrorCall::new(address),
            store_name: StoreName::new(COMPONENT_STORE_NAME.to_owned()),
        }
    }

    async fn fetch(&self) -> RestoreBundle {
        let reply = self
            .call
            .exchange(Input::Restore(RestoreQuery::new(self.store_name.clone())))
            .await;
        match reply {
            Output::Restored(bundle) => bundle,
            other => panic!("expected Restored, got {other:?}"),
        }
    }

    fn import(bundle: RestoreBundle, target: &mut ComponentEngine) {
        let checkpoint = rkyv::from_bytes::<PortableCheckpoint, rkyv::rancor::Error>(
            bundle.checkpoint.artifact.as_slice(),
        )
        .expect("decode checkpoint artifact")
        .into_checkpoint();
        let suffix: Vec<VersionedCommitLogEntry> = bundle
            .suffix
            .iter()
            .map(|envelope| {
                rkyv::from_bytes::<VersionedCommitLogEntry, rkyv::rancor::Error>(
                    envelope.payload.as_slice(),
                )
                .expect("decode versioned entry payload")
            })
            .collect();
        let mut session = target.begin_import().expect("import session mints");
        session
            .ingest_checkpoint(checkpoint)
            .expect("checkpoint ingests");
        session.ingest_suffix(suffix);
        session
            .commit(&Families::new())
            .expect("import commits into the fresh store");
    }
}

struct ComponentFixture {
    directory: tempfile::TempDir,
}

impl ComponentFixture {
    fn new() -> Self {
        Self {
            directory: tempfile::tempdir().expect("temp dir"),
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.directory.path().join(format!("{name}.sema"))
    }

    fn open_fresh(&self, file: &str) -> ComponentEngine {
        ComponentEngine::open(
            EngineOpen::new(self.path(file), SchemaVersion::new(1)).with_versioning(
                VersioningPolicy::new(VersionedStoreName::new(COMPONENT_STORE_NAME)),
            ),
        )
        .expect("component engine opens")
    }

    fn thought_descriptor(&self) -> TableDescriptor<Thought> {
        TableDescriptor::new(
            TableName::new("thoughts"),
            FamilyName::new("thought"),
            SchemaHash::for_label("thought-v1"),
        )
    }

    fn open_component(&self, file: &str) -> (ComponentEngine, TableReference<Thought>) {
        let mut engine = self.open_fresh(file);
        let thoughts = engine
            .register_table(self.thought_descriptor())
            .expect("thoughts register");
        (engine, thoughts)
    }

    /// Populate the source component: writes, a mid-history checkpoint,
    /// then post-checkpoint writes including a tombstone.
    fn populate(&self) -> (ComponentEngine, TableReference<Thought>) {
        let (engine, thoughts) = self.open_component("component-source");
        engine
            .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
            .expect("assert alpha");
        engine
            .assert(Assertion::new(thoughts, Thought::new("beta", "second")))
            .expect("assert beta");
        engine
            .mutate(Mutation::new(thoughts, Thought::new("alpha", "revised")))
            .expect("mutate alpha");
        engine.checkpoint().expect("checkpoint writes");
        engine
            .assert(Assertion::new(thoughts, Thought::new("gamma", "third")))
            .expect("assert gamma");
        engine
            .retract(Retraction::new(thoughts, RecordKey::new("beta")))
            .expect("retract beta");
        (engine, thoughts)
    }
}

async fn running_mirror(directory: &tempfile::TempDir) -> (ServiceLink, SocketAddr) {
    let store = Store::open(&directory.path().join("mirror.sema")).expect("mirror store opens");
    let service = MirrorService::spawn(MirrorService::new(
        MirrorEngine::new(store),
        "127.0.0.1:0".parse().expect("loopback address"),
    ));
    service.wait_for_startup().await;
    let link = ServiceLink::new(service);
    let address = link
        .tcp_bound_address()
        .await
        .expect("query bound address")
        .expect("the tailnet ingress is bound");
    (link, address)
}

#[tokio::test]
async fn component_history_ships_over_tcp_and_a_fresh_store_restores_identically() {
    let fixture = ComponentFixture::new();
    let (source, source_thoughts) = fixture.populate();

    // A running mirror daemon runtime: real engine, real store, real
    // loopback TCP listener.
    let mirror_directory = tempfile::tempdir().expect("mirror temp dir");
    let (link, address) = running_mirror(&mirror_directory).await;

    // The owner registers the component store on the meta surface.
    let registered = link
        .meta(meta_signal_mirror::Input::RegisterStore(
            meta_signal_mirror::StoreRegistration::new(meta_signal_mirror::StoreName::new(
                COMPONENT_STORE_NAME.to_owned(),
            )),
        ))
        .await
        .expect("meta register");
    assert!(matches!(
        registered,
        meta_signal_mirror::Output::StoreRegistered(_)
    ));

    // Before shipping: local history is queued, not server-committed.
    assert_eq!(
        source.store_durability().expect("durability reads"),
        Durability::QueuedForMirror
    );

    // SHIP: outbox suffix -> envelopes -> real TCP frames -> mirror
    // persists -> acknowledged head -> ServerCommitted.
    let shipper = Shipper::new(address);
    let confirmed = shipper.ship(&source).await;
    assert_eq!(
        source.store_durability().expect("durability reads"),
        Durability::ServerCommitted
    );
    assert_eq!(
        source
            .durability_of(confirmed.commit_sequence())
            .expect("per-entry durability reads"),
        Durability::ServerCommitted
    );
    assert!(
        source.unshipped_outbox().expect("outbox reads").is_empty(),
        "the shipped cursor covers the whole outbox"
    );

    // Publish the checkpoint artifact the restorer will fetch.
    shipper.publish_checkpoint(&source).await;

    // Re-shipping the same history is idempotent at the daemon level.
    let resend = MirrorCall::new(address)
        .exchange(Input::Append(EntrySuffix {
            store: StoreName::new(COMPONENT_STORE_NAME.to_owned()),
            expected_head: None,
            entries: source
                .versioned_replay_from_sequence(sema_engine::CommitSequence::new(1))
                .expect("reload full history")
                .iter()
                .map(Shipper::envelope)
                .collect(),
        }))
        .await;
    match resend {
        Output::Appended(receipt) => {
            assert_eq!(
                receipt.head.sequence.clone().into_u64(),
                confirmed.commit_sequence().value()
            );
        }
        other => panic!("expected idempotent Appended, got {other:?}"),
    }

    // The mirror carried the TCP peers as typed PeerIdentity::Tcp —
    // observed through the bounded witness (a count plus the most
    // recent peer; no unbounded production peer list).
    let witness = link.tcp_peer_witness().await.expect("peer witness");
    assert!(witness.served_request_count() >= 1);
    assert!(matches!(witness.last_peer(), Some(PeerIdentity::Tcp(_))));

    // RESTORE: a fresh component store imports checkpoint + suffix
    // fetched from the mirror.
    let restorer = Restorer::new(address);
    let bundle = restorer.fetch().await;
    assert_eq!(bundle.suffix.len(), 2, "gamma + the beta tombstone");
    let mut target = fixture.open_fresh("component-restored");
    Restorer::import(bundle, &mut target);
    let target_thoughts = target
        .register_table(fixture.thought_descriptor())
        .expect("thoughts re-register against the restored catalog");

    // The normal query surface reads identical records on both engines.
    let source_records = source
        .match_records(QueryPlan::all(source_thoughts))
        .expect("source query")
        .records()
        .to_vec();
    let target_records = target
        .match_records(QueryPlan::all(target_thoughts))
        .expect("target query")
        .records()
        .to_vec();
    assert_eq!(source_records, target_records);
    assert_eq!(
        target_records,
        vec![
            Thought::new("alpha", "revised"),
            Thought::new("gamma", "third"),
        ]
    );

    // And the restored store continues the same digest chain.
    assert_eq!(
        source.current_commit_sequence().expect("source cursor"),
        target.current_commit_sequence().expect("target cursor"),
    );
}
