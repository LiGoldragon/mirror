mod support;

use mirror::{Engine, LandedBody, Store};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_mirror::{z2VPuU, z2VSAK, z2VTqL, z2VUwg, z2VVny};
use signal_standard::z2VSyM;

use support::{append, artifact, register, store};

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
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

fn real_genesis_envelope(store_name: &str) -> (z2VPuU, sema_engine::EntryDigest) {
    let directory = tempfile::tempdir().expect("source directory");
    let mut engine = sema_engine::Engine::open(
        sema_engine::EngineOpen::new(
            directory.path().join("source.sema"),
            sema_engine::SchemaVersion::new(1),
        )
        .with_versioning(sema_engine::VersioningPolicy::new(
            sema_engine::VersionedStoreName::new(store_name),
        )),
    )
    .expect("source opens");
    let table = engine
        .register_table(sema_engine::TableDescriptor::<WitnessRecord>::new(
            sema_engine::TableName::new("records"),
            sema_engine::FamilyName::new("record"),
            sema_engine::SchemaHash::for_label("witness-record-v1"),
        ))
        .expect("table");
    engine
        .assert(sema_engine::Assertion::new(
            table,
            WitnessRecord {
                key: "witness-1".to_owned(),
                body: "criome witness".to_owned(),
            },
        ))
        .expect("assert");
    let genesis = engine
        .versioned_commit_log()
        .unwrap()
        .last()
        .unwrap()
        .clone();
    let head = genesis.entry_digest();
    let digest = head
        .bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let body = rkyv::to_bytes::<rkyv::rancor::Error>(&genesis).unwrap();
    (
        z2VPuU {
            field_0: z2VSAK::new(1),
            field_1: None,
            field_2: z2VSyM::new(digest),
            field_3: z2VUwg::from_octets(&body),
        },
        head,
    )
}

#[tokio::test]
async fn restore_returns_the_exact_landed_body_and_its_content_address() {
    let (envelope, real_head) = real_genesis_envelope("spirit");
    let shipped_body = envelope.field_3.octets().expect("octets");
    let directory = tempfile::tempdir().expect("mirror directory");
    let mut engine = Engine::new(Store::open(&directory.path().join("mirror.sema")).unwrap());
    register(&mut engine, "spirit", meta_signal_mirror::z2VMYP::z2Vf8Y);
    assert!(matches!(
        engine.handle(append("spirit", None, vec![envelope])).await,
        z2VTqL::z2VXSq(_)
    ));
    let mut zero = artifact("spirit", 1, 0);
    zero.field_4 = signal_mirror::z2VUxk::from_octets(&[]);
    assert!(matches!(
        engine.handle(z2VVny::z2VNu6(zero)).await,
        z2VTqL::z2VaSa(_)
    ));
    let bundle = match engine
        .handle(z2VVny::z2VdHF(signal_mirror::z2VbvA::new(store("spirit"))))
        .await
    {
        z2VTqL::z2VVve(bundle) => bundle,
        other => panic!("unexpected output: {other:?}"),
    };
    assert_eq!(bundle.field_2.len(), 1);
    let landed = &bundle.field_2[0];
    let landed_body = landed.field_3.octets().expect("octets");
    assert_eq!(landed_body, shipped_body);
    let rederived = LandedBody::new(&landed_body).content_address().unwrap();
    assert_eq!(rederived, real_head);
    assert_eq!(
        landed.field_2.as_str(),
        real_head
            .bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
}
