mod support;

use mirror::ledger::{ContentAddressing, NovelSuffix, RecordFamily};
use mirror::{Engine, Store};
use signal_mirror::{z2VPgu, z2VTqL, z2VVny, z2VcyE, z2VdLR};
use tempfile::TempDir;

use support::{append, artifact, envelope, head, observed_head, register, store};

struct Harness {
    _directory: TempDir,
    engine: Engine,
}

impl Harness {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("temporary directory");
        let store = Store::open(&directory.path().join("mirror.redb")).expect("open store");
        Self {
            _directory: directory,
            engine: Engine::new(store),
        }
    }

    fn registered(name: &str) -> Self {
        let mut harness = Self::new();
        register(
            &mut harness.engine,
            name,
            meta_signal_mirror::z2VMYP::z2Vf8Y,
        );
        harness
    }
}

#[tokio::test]
async fn unknown_store_is_refused() {
    let mut harness = Harness::new();
    match harness
        .engine
        .handle(append("spirit", None, vec![envelope(1, None, 0x11)]))
        .await
    {
        z2VTqL::z2VX2Y(rejection) => assert_eq!(rejection.field_1, z2VcyE::z2VcFn),
        other => panic!("unexpected output: {other:?}"),
    }
}

#[tokio::test]
async fn append_is_durable_before_ack_and_duplicate_is_idempotent() {
    let mut harness = Harness::registered("spirit");
    let request = append(
        "spirit",
        None,
        vec![envelope(1, None, 0x11), envelope(2, Some(0x11), 0x22)],
    );
    assert!(matches!(
        harness.engine.handle(request.clone()).await,
        z2VTqL::z2VXSq(_)
    ));
    let before = harness
        .engine
        .store()
        .engine()
        .versioned_commit_log()
        .expect("commit log")
        .len();
    assert!(matches!(
        harness.engine.handle(request).await,
        z2VTqL::z2VXSq(_)
    ));
    let after = harness
        .engine
        .store()
        .engine()
        .versioned_commit_log()
        .expect("commit log")
        .len();
    assert_eq!(before, after);
    assert_eq!(
        observed_head(&mut harness.engine, "spirit").await,
        Some(head(2, 0x22))
    );
}

#[tokio::test]
async fn partially_duplicate_suffix_persists_only_the_remainder() {
    let mut harness = Harness::registered("spirit");
    assert!(matches!(
        harness
            .engine
            .handle(append("spirit", None, vec![envelope(1, None, 0x11)]))
            .await,
        z2VTqL::z2VXSq(_)
    ));
    assert!(matches!(
        harness
            .engine
            .handle(append(
                "spirit",
                Some(head(1, 0x11)),
                vec![envelope(2, Some(0x11), 0x22), envelope(3, Some(0x22), 0x33)],
            ))
            .await,
        z2VTqL::z2VXSq(_)
    ));
    assert_eq!(
        observed_head(&mut harness.engine, "spirit").await,
        Some(head(3, 0x33))
    );
}

#[tokio::test]
async fn crash_window_resend_re_advances_head() {
    let harness = Harness::registered("spirit");
    let entries = vec![envelope(1, None, 0x11), envelope(2, Some(0x11), 0x22)];
    let suffix = NovelSuffix::new(store("spirit"), head(2, 0x22), entries.clone());
    harness
        .engine
        .store()
        .engine()
        .versioned_commit_log()
        .expect("store stays inspectable");
    // Re-open mutable access through a standalone store to exercise the
    // deliberate crash-window seam without weakening Engine's ownership.
    let directory = tempfile::tempdir().expect("temp");
    let mut store_value = Store::open(&directory.path().join("window.redb")).expect("open");
    store_value
        .register_store(&store("spirit"), ContentAddressing::Opaque)
        .expect("register");
    store_value.commit_entry_rows(&suffix).expect("entry rows");
    let mut engine = Engine::new(store_value);
    assert!(matches!(
        engine.handle(append("spirit", None, entries)).await,
        z2VTqL::z2VXSq(_)
    ));
    assert_eq!(
        observed_head(&mut engine, "spirit").await,
        Some(head(2, 0x22))
    );
}

#[tokio::test]
async fn gap_fork_divergence_and_empty_suffix_are_typed_refusals() {
    let mut harness = Harness::registered("spirit");
    for (request, expected) in [
        (append("spirit", None, Vec::new()), z2VcyE::z2VRBT),
        (
            append("spirit", None, vec![envelope(2, None, 0x22)]),
            z2VcyE::z2VQma,
        ),
        (
            append(
                "spirit",
                None,
                vec![envelope(1, None, 0x11), envelope(2, Some(0x33), 0x22)],
            ),
            z2VcyE::z2VZT1,
        ),
    ] {
        match harness.engine.handle(request).await {
            z2VTqL::z2VX2Y(rejection) => assert_eq!(rejection.field_1, expected),
            other => panic!("unexpected output: {other:?}"),
        }
    }
    assert!(matches!(
        harness
            .engine
            .handle(append("spirit", None, vec![envelope(1, None, 0x11)]))
            .await,
        z2VTqL::z2VXSq(_)
    ));
    match harness
        .engine
        .handle(append("spirit", None, vec![envelope(1, None, 0x99)]))
        .await
    {
        z2VTqL::z2VX2Y(rejection) => assert_eq!(rejection.field_1, z2VcyE::z2VPd1),
        other => panic!("unexpected output: {other:?}"),
    }
}

#[tokio::test]
async fn retirement_then_registration_resumes_surviving_history() {
    let mut harness = Harness::registered("spirit");
    assert!(matches!(
        harness
            .engine
            .handle(append("spirit", None, vec![envelope(1, None, 0x11)]))
            .await,
        z2VTqL::z2VXSq(_)
    ));
    assert!(matches!(
        harness
            .engine
            .handle_meta(meta_signal_mirror::z2VWt5::z2VXXR(
                meta_signal_mirror::z2VWZw::new(store("spirit")),
            )),
        meta_signal_mirror::z2VUH6::z2VdCD(_)
    ));
    register(
        &mut harness.engine,
        "spirit",
        meta_signal_mirror::z2VMYP::z2Vf8Y,
    );
    assert_eq!(
        observed_head(&mut harness.engine, "spirit").await,
        Some(head(1, 0x11))
    );
}

#[tokio::test]
async fn object_notice_accepts_only_a_known_head() {
    let mut harness = Harness::registered("spirit");
    assert!(matches!(
        harness
            .engine
            .handle(append("spirit", None, vec![envelope(1, None, 0x11)]))
            .await,
        z2VTqL::z2VXSq(_)
    ));
    let known = signal_mirror::z2VZWt {
        field_0: store("spirit"),
        field_1: head(1, 0x11),
        field_2: None,
    };
    assert!(matches!(
        harness.engine.handle(z2VVny::z2VaYk(known)).await,
        z2VTqL::z2VR8x(_)
    ));
    let unknown = signal_mirror::z2VZWt {
        field_0: store("spirit"),
        field_1: head(2, 0x22),
        field_2: None,
    };
    match harness.engine.handle(z2VVny::z2VaYk(unknown)).await {
        z2VTqL::z2VSxB(rejection) => assert_eq!(rejection.field_1, z2VdLR::z2VLZN),
        other => panic!("unexpected output: {other:?}"),
    }
}

#[test]
fn separator_bearing_store_names_are_refused() {
    let mut harness = Harness::new();
    match harness
        .engine
        .handle_meta(meta_signal_mirror::z2VWt5::z2VWC2(
            meta_signal_mirror::z2VWBn {
                field_0: store("spirit/evil"),
                field_1: meta_signal_mirror::z2VMYP::z2Vf8Y,
            },
        )) {
        meta_signal_mirror::z2VUH6::z2VXCa(rejection) => {
            assert_eq!(rejection.field_0, meta_signal_mirror::z2VYSE::z2VPC3)
        }
        other => panic!("unexpected output: {other:?}"),
    }
}

#[tokio::test]
async fn checkpoint_and_restore_preserve_the_suffix() {
    let mut harness = Harness::registered("spirit");
    assert!(matches!(
        harness
            .engine
            .handle(append(
                "spirit",
                None,
                vec![envelope(1, None, 0x11), envelope(2, Some(0x11), 0x22)],
            ))
            .await,
        z2VTqL::z2VXSq(_)
    ));
    assert!(matches!(
        harness
            .engine
            .handle(z2VVny::z2VNu6(artifact("spirit", 1, 1)))
            .await,
        z2VTqL::z2VaSa(_)
    ));
    match harness
        .engine
        .handle(z2VVny::z2VdHF(signal_mirror::z2VbvA::new(store("spirit"))))
        .await
    {
        z2VTqL::z2VVve(bundle) => {
            assert_eq!(bundle.field_2.len(), 1);
            assert_eq!(*bundle.field_2[0].field_0.payload(), 2);
        }
        other => panic!("unexpected output: {other:?}"),
    }
}

#[tokio::test]
async fn restore_without_checkpoint_is_typed() {
    let mut harness = Harness::registered("spirit");
    match harness
        .engine
        .handle(z2VVny::z2VdHF(signal_mirror::z2VbvA::new(store("spirit"))))
        .await
    {
        z2VTqL::z2VLCz(rejection) => assert_eq!(rejection.field_1, z2VPgu::z2VXM2),
        other => panic!("unexpected output: {other:?}"),
    }
}

#[test]
fn mirror_dogfoods_a_versioned_store() {
    let harness = Harness::registered("spirit");
    assert_eq!(RecordFamily::STORE_NAME, "mirror:sema");
    assert!(
        !harness
            .engine
            .store()
            .engine()
            .versioned_commit_log()
            .expect("commit log")
            .is_empty()
    );
}
