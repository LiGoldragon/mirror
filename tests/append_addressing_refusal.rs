mod support;

use mirror::{Engine, LandedBody, Store};
use signal_mirror::{z2VPuU, z2VSAK, z2VTqL, z2VUwg, z2VcyE};
use signal_standard::z2VSyM;

use support::{append, observed_head, register, store};

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
struct WitnessRecord {
    key: String,
    body: String,
}

impl sema_engine::EngineRecord for WitnessRecord {
    fn record_key(&self) -> sema_engine::RecordKey {
        sema_engine::RecordKey::new(self.key.clone())
    }
}

fn digest_text(bytes: &[u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn real_genesis_envelope(
    store_name: &str,
    key: &str,
    body: &str,
) -> (z2VPuU, sema_engine::EntryDigest) {
    let directory = tempfile::tempdir().expect("source store directory");
    let mut engine = sema_engine::Engine::open(
        sema_engine::EngineOpen::new(
            directory.path().join("source.sema"),
            sema_engine::SchemaVersion::new(1),
        )
        .with_versioning(sema_engine::VersioningPolicy::new(
            sema_engine::VersionedStoreName::new(store_name),
        )),
    )
    .expect("source component engine opens");
    let records: sema_engine::TableReference<WitnessRecord> = engine
        .register_table(sema_engine::TableDescriptor::new(
            sema_engine::TableName::new("records"),
            sema_engine::FamilyName::new("record"),
            sema_engine::SchemaHash::for_label("witness-record-v1"),
        ))
        .expect("record family registers");
    engine
        .assert(sema_engine::Assertion::new(
            records,
            WitnessRecord {
                key: key.to_owned(),
                body: body.to_owned(),
            },
        ))
        .expect("assert witness record");
    let genesis = engine
        .versioned_commit_log()
        .expect("commit log")
        .last()
        .expect("genesis")
        .clone();
    let head = genesis.entry_digest();
    let payload = rkyv::to_bytes::<rkyv::rancor::Error>(&genesis).expect("serialize");
    (
        z2VPuU {
            field_0: z2VSAK::new(1),
            field_1: None,
            field_2: z2VSyM::new(digest_text(head.bytes())),
            field_3: z2VUwg::from_octets(&payload),
        },
        head,
    )
}

#[tokio::test]
async fn addressing_policy_refuses_tampering_without_weakening_opaque_stores() {
    let directory = tempfile::tempdir().expect("mirror store directory");
    let mut engine = Engine::new(
        Store::open(&directory.path().join("mirror.sema")).expect("mirror store opens"),
    );
    register(&mut engine, "spirit", meta_signal_mirror::z2VMYP::z2VbgN);
    register(&mut engine, "control", meta_signal_mirror::z2VMYP::z2Vf8Y);

    let (faithful, faithful_head) = real_genesis_envelope("spirit", "record-1", "criome witness");
    let (other, other_head) = real_genesis_envelope("spirit", "record-2", "other body");
    assert_ne!(faithful_head, other_head);
    let tampered = z2VPuU {
        field_0: z2VSAK::new(1),
        field_1: None,
        field_2: faithful.field_2.clone(),
        field_3: other.field_3.clone(),
    };
    let tampered_octets = tampered.field_3.octets().expect("octets");
    assert_eq!(
        LandedBody::new(&tampered_octets)
            .content_address()
            .expect("genuine versioned entry"),
        other_head
    );

    match engine
        .handle(append("spirit", None, vec![tampered.clone()]))
        .await
    {
        z2VTqL::z2VX2Y(rejection) => assert_eq!(rejection.field_1, z2VcyE::z2VPd1),
        other => panic!("unexpected output: {other:?}"),
    }
    assert!(
        engine
            .store()
            .landed_entries(&store("spirit"))
            .unwrap()
            .is_empty()
    );
    assert_eq!(observed_head(&mut engine, "spirit").await, None);

    assert!(matches!(
        engine.handle(append("spirit", None, vec![faithful])).await,
        z2VTqL::z2VXSq(_)
    ));
    let landed = engine.store().landed_entries(&store("spirit")).unwrap();
    let landed_octets = landed[0].field_3.octets().expect("octets");
    assert_eq!(
        LandedBody::new(&landed_octets).content_address().unwrap(),
        faithful_head
    );

    assert!(matches!(
        engine.handle(append("control", None, vec![tampered])).await,
        z2VTqL::z2VXSq(_)
    ));
    assert_eq!(
        engine
            .store()
            .landed_entries(&store("control"))
            .unwrap()
            .len(),
        1
    );
}
