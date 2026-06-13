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
//! The component-side shipper is the production reusable
//! `ComponentShipper`; this test exercises it over the daemon's real TCP
//! ingress.

use std::net::SocketAddr;
use std::path::PathBuf;

use mirror::{
    ComponentShipper, Engine, MirrorTailnetClient, PublishLatestCheckpoint, Service, ServiceLink,
    ShipOutcome, ShipUnshipped, Store,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Durability, Engine as ComponentEngine, EngineOpen, EngineRecord, FamilyDirectory,
    FamilyName, Mutation, PortableCheckpoint, QueryPlan, RecordKey, Retraction, RowMaterializer,
    SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference, VersionedCommitLogEntry,
    VersionedStoreName, VersioningPolicy,
};
use signal_mirror::{EntrySuffix, Input, Output, RestoreBundle, RestoreQuery, StoreName};
use triad_runtime::PeerIdentity;
use triad_runtime::kameo::actor::Spawn;

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

/// The component-side restorer: fetch checkpoint + suffix from the
/// mirror and import them into a fresh store.
struct Restorer {
    client: MirrorTailnetClient,
    store_name: StoreName,
}

impl Restorer {
    fn new(address: SocketAddr) -> Self {
        Self {
            client: MirrorTailnetClient::new(address),
            store_name: StoreName::new(COMPONENT_STORE_NAME.to_owned()),
        }
    }

    async fn fetch(&self) -> RestoreBundle {
        let reply = self
            .client
            .exchange(Input::Restore(RestoreQuery::new(self.store_name.clone())))
            .await
            .expect("restore call succeeds");
        match reply {
            Output::Restored(bundle) => bundle,
            other => panic!("expected Restored, got {other:?}"),
        }
    }

    fn import(bundle: RestoreBundle, target: &mut ComponentEngine) {
        let checkpoint =
            PortableCheckpoint::from_bytes(bundle.checkpoint.artifact.payload().payload().to_vec())
                .decode()
                .expect("decode checkpoint artifact");
        let suffix: Vec<VersionedCommitLogEntry> = bundle
            .suffix
            .iter()
            .map(|envelope| {
                rkyv::from_bytes::<VersionedCommitLogEntry, rkyv::rancor::Error>(
                    envelope.payload.payload().payload(),
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
    let service = Service::spawn(Service::new(
        Engine::new(store),
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
    let shipper = ComponentShipper::new(
        source,
        address,
        VersionedStoreName::new(COMPONENT_STORE_NAME),
    );
    let confirmed = match shipper
        .ship_unshipped()
        .await
        .expect("shipper ships unshipped suffix")
    {
        ShipOutcome::Shipped { head } => head,
        other => panic!("expected shipped history, got {other:?}"),
    };
    assert_eq!(
        shipper
            .engine()
            .store_durability()
            .expect("durability reads"),
        Durability::ServerCommitted
    );
    assert_eq!(
        shipper
            .engine()
            .durability_of(confirmed.commit_sequence())
            .expect("per-entry durability reads"),
        Durability::ServerCommitted
    );
    assert!(
        shipper
            .engine()
            .unshipped_outbox()
            .expect("outbox reads")
            .is_empty(),
        "the shipped cursor covers the whole outbox"
    );

    // Publish the checkpoint artifact the restorer will fetch.
    shipper
        .publish_latest_checkpoint()
        .await
        .expect("checkpoint publishes");

    // Re-shipping the same history is idempotent at the daemon level.
    let resend = MirrorTailnetClient::new(address)
        .exchange(Input::Append(EntrySuffix {
            store: StoreName::new(COMPONENT_STORE_NAME.to_owned()),
            expected_head: None,
            entries: shipper
                .engine()
                .versioned_replay_from_sequence(sema_engine::CommitSequence::new(1))
                .expect("reload full history")
                .iter()
                .map(|entry| {
                    shipper
                        .envelope_for_entry(entry)
                        .expect("encode versioned entry envelope")
                })
                .collect(),
        }))
        .await
        .expect("resend call succeeds");
    match resend {
        Output::Appended(receipt) => {
            assert_eq!(
                *receipt.head.sequence.payload(),
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
    let source_records = shipper
        .engine()
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
        shipper
            .engine()
            .current_commit_sequence()
            .expect("source cursor"),
        target.current_commit_sequence().expect("target cursor"),
    );
}

#[tokio::test]
async fn component_shipper_actor_ships_suffix_and_publishes_checkpoint() {
    let fixture = ComponentFixture::new();
    let (source, _source_thoughts) = fixture.populate();
    let mirror_directory = tempfile::tempdir().expect("mirror temp dir");
    let (link, address) = running_mirror(&mirror_directory).await;

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

    let shipper = ComponentShipper::spawn(ComponentShipper::new(
        source,
        address,
        VersionedStoreName::new(COMPONENT_STORE_NAME),
    ));
    let outcome = shipper
        .ask(ShipUnshipped)
        .await
        .expect("shipper actor ships suffix");
    assert!(matches!(outcome, ShipOutcome::Shipped { .. }));

    let receipt = shipper
        .ask(PublishLatestCheckpoint)
        .await
        .expect("shipper actor publishes checkpoint");
    assert_eq!(*receipt.sequence.payload(), 1);
    assert_eq!(*receipt.covered_end.payload(), 3);
}
