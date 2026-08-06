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
    VersionedRecoveryTopology, VersionedStoreName, VersioningPolicy,
};
use signal_mirror::{z2VTq5, z2VTqL, z2VVny, z2VbvA, z2Ve8p};
use triad_runtime::PeerIdentity;
use triad_runtime::kameo::actor::Spawn;

const COMPONENT_STORE_NAME: &str = "arc-witness";

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

struct Restorer {
    client: MirrorTailnetClient,
    store_name: z2Ve8p,
}

impl Restorer {
    fn new(address: SocketAddr) -> Self {
        Self {
            client: MirrorTailnetClient::new(address),
            store_name: z2Ve8p::new(COMPONENT_STORE_NAME.to_owned()),
        }
    }

    async fn fetch(&self) -> signal_mirror::z2VYSu {
        match self
            .client
            .exchange(z2VVny::z2VdHF(z2VbvA::new(self.store_name.clone())))
            .await
            .expect("restore call")
        {
            z2VTqL::z2VVve(bundle) => bundle,
            other => panic!("unexpected output: {other:?}"),
        }
    }

    fn import(bundle: signal_mirror::z2VYSu, target: &mut ComponentEngine) {
        let checkpoint = PortableCheckpoint::from_bytes(
            bundle.field_1.field_4.octets().expect("checkpoint octets"),
        )
        .decode()
        .expect("decode checkpoint");
        let suffix = bundle
            .field_2
            .iter()
            .map(|envelope| {
                rkyv::from_bytes::<VersionedCommitLogEntry, rkyv::rancor::Error>(
                    &envelope.field_3.octets().expect("entry octets"),
                )
                .expect("decode entry")
            })
            .collect();
        let mut session = target.begin_import().expect("import session");
        session.ingest_checkpoint(checkpoint).expect("checkpoint");
        session.ingest_suffix(suffix);
        session.commit(&Families::new()).expect("import commit");
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
                VersioningPolicy::new(VersionedStoreName::new(COMPONENT_STORE_NAME))
                    .with_recovery_topology(VersionedRecoveryTopology::Mirror),
            ),
        )
        .expect("component engine")
    }

    fn descriptor(&self) -> TableDescriptor<Thought> {
        TableDescriptor::new(
            TableName::new("thoughts"),
            FamilyName::new("thought"),
            SchemaHash::for_label("thought-v1"),
        )
    }

    fn open_component(&self, file: &str) -> (ComponentEngine, TableReference<Thought>) {
        let mut engine = self.open_fresh(file);
        let thoughts = engine.register_table(self.descriptor()).expect("thoughts");
        (engine, thoughts)
    }

    fn populate(&self) -> (ComponentEngine, TableReference<Thought>) {
        let (engine, thoughts) = self.open_component("source");
        engine
            .assert(Assertion::new(thoughts, Thought::new("alpha", "first")))
            .unwrap();
        engine
            .assert(Assertion::new(thoughts, Thought::new("beta", "second")))
            .unwrap();
        engine
            .mutate(Mutation::new(thoughts, Thought::new("alpha", "revised")))
            .unwrap();
        engine.checkpoint().unwrap();
        engine
            .assert(Assertion::new(thoughts, Thought::new("gamma", "third")))
            .unwrap();
        engine
            .retract(Retraction::new(thoughts, RecordKey::new("beta")))
            .unwrap();
        (engine, thoughts)
    }
}

async fn running_mirror(directory: &tempfile::TempDir) -> (ServiceLink, SocketAddr) {
    let store = Store::open(&directory.path().join("mirror.sema")).expect("mirror store");
    let service = Service::spawn(Service::new(
        Engine::new(store),
        "127.0.0.1:0".parse().unwrap(),
    ));
    service.wait_for_startup().await;
    let link = ServiceLink::new(service);
    let address = link.tcp_bound_address().await.unwrap().unwrap();
    (link, address)
}

async fn register(link: &ServiceLink) {
    let output = link
        .meta(meta_signal_mirror::z2VWt5::z2VWC2(
            meta_signal_mirror::z2VWBn {
                field_0: z2Ve8p::new(COMPONENT_STORE_NAME.to_owned()),
                field_1: meta_signal_mirror::z2VMYP::z2Vf8Y,
            },
        ))
        .await
        .expect("register");
    assert!(matches!(output, meta_signal_mirror::z2VUH6::z2VSig(_)));
}

#[tokio::test]
async fn component_history_ships_over_tcp_and_restores_identically() {
    let fixture = ComponentFixture::new();
    let (source, source_thoughts) = fixture.populate();
    let mirror_directory = tempfile::tempdir().expect("mirror temp");
    let (link, address) = running_mirror(&mirror_directory).await;
    register(&link).await;
    assert_eq!(
        source.store_durability().unwrap(),
        Durability::QueuedForMirror
    );

    let shipper = ComponentShipper::new(
        source,
        address,
        VersionedStoreName::new(COMPONENT_STORE_NAME),
    );
    let confirmed = match shipper.ship_unshipped().await.expect("ship") {
        ShipOutcome::Shipped { head } => head,
        other => panic!("unexpected outcome: {other:?}"),
    };
    assert_eq!(
        shipper.engine().store_durability().unwrap(),
        Durability::ServerCommitted
    );
    shipper
        .publish_latest_checkpoint()
        .await
        .expect("checkpoint");

    let entries = shipper
        .engine()
        .versioned_replay_from_sequence(sema_engine::CommitSequence::new(1))
        .unwrap()
        .iter()
        .map(|entry| shipper.envelope_for_entry(entry).unwrap())
        .collect();
    match MirrorTailnetClient::new(address)
        .exchange(z2VVny::z2VVjQ(z2VTq5 {
            field_0: z2Ve8p::new(COMPONENT_STORE_NAME.to_owned()),
            field_1: None,
            field_2: entries,
        }))
        .await
        .expect("resend")
    {
        z2VTqL::z2VXSq(receipt) => {
            assert_eq!(
                *receipt.field_1.field_0.payload(),
                confirmed.commit_sequence().value()
            )
        }
        other => panic!("unexpected resend: {other:?}"),
    }

    let witness = link.tcp_peer_witness().await.expect("peer witness");
    assert!(witness.served_request_count() >= 1);
    assert!(matches!(witness.last_peer(), Some(PeerIdentity::Tcp(_))));

    let bundle = Restorer::new(address).fetch().await;
    assert_eq!(bundle.field_2.len(), 2);
    let mut target = fixture.open_fresh("restored");
    Restorer::import(bundle, &mut target);
    let target_thoughts = target.register_table(fixture.descriptor()).unwrap();
    let source_records = shipper
        .engine()
        .match_records(QueryPlan::all(source_thoughts))
        .unwrap()
        .records()
        .to_vec();
    let target_records = target
        .match_records(QueryPlan::all(target_thoughts))
        .unwrap()
        .records()
        .to_vec();
    assert_eq!(source_records, target_records);
    assert_eq!(
        target_records,
        vec![
            Thought::new("alpha", "revised"),
            Thought::new("gamma", "third")
        ]
    );
}

#[tokio::test]
async fn shipper_actor_ships_and_publishes() {
    let fixture = ComponentFixture::new();
    let (source, _) = fixture.populate();
    let mirror_directory = tempfile::tempdir().expect("mirror temp");
    let (link, address) = running_mirror(&mirror_directory).await;
    register(&link).await;
    let shipper = ComponentShipper::spawn(ComponentShipper::new(
        source,
        address,
        VersionedStoreName::new(COMPONENT_STORE_NAME),
    ));
    assert!(matches!(
        shipper.ask(ShipUnshipped).await.expect("ship"),
        ShipOutcome::Shipped { .. }
    ));
    let receipt = shipper.ask(PublishLatestCheckpoint).await.expect("publish");
    assert_eq!(*receipt.field_1.payload(), 1);
    assert_eq!(*receipt.field_2.payload(), 3);
}
