//! Daemon-logic witnesses driven through the engine's full Nexus path
//! (`Engine::handle`): append with matching expected head accepted
//! and the head advances; a duplicate suffix acknowledges idempotently
//! (same head, no duplicate rows); gaps and forks are rejected typed;
//! the crash window between the entry transaction and the head advance
//! self-heals on re-send; retire-then-re-register resumes the surviving
//! chain; restore returns checkpoint plus suffix; and the mirror's own
//! ledger is itself a versioned sema-engine store (the dogfooding
//! proof).

use mirror::schema::sema::{NovelSuffix, RecordFamily};
use mirror::{Engine, Store};
use signal_mirror::{
    AppendRejectionReason, ArtifactBytes, ArtifactDigest, Bytes, CheckpointArtifact,
    CheckpointSequence, CommitSequence, EntryDigest, EntryEnvelope, EntrySuffix, FixedBytes,
    HeadMark, HeadQuery, Input, ObjectNotice, ObjectNoticeRejectionReason, Output, PayloadBytes,
    RestoreQuery, RestoreRejectionReason, StoreName,
};

struct Fixture {
    _directory: tempfile::TempDir,
    engine: Engine,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("temp dir");
        let store = Store::open(&directory.path().join("mirror.sema")).expect("mirror store opens");
        Self {
            _directory: directory,
            engine: Engine::new(store),
        }
    }

    fn with_registered(store_name: &str) -> Self {
        let mut fixture = Self::new();
        let registered = fixture
            .engine
            .handle_meta(meta_signal_mirror::Input::RegisterStore(
                meta_signal_mirror::StoreRegistration::new(meta_signal_mirror::StoreName::new(
                    store_name.to_owned(),
                )),
            ));
        assert!(matches!(
            registered,
            meta_signal_mirror::Output::StoreRegistered(_)
        ));
        fixture
    }

    async fn handle(&mut self, input: Input) -> Output {
        self.engine.handle(input).await
    }
}

fn store(name: &str) -> StoreName {
    StoreName::new(name.to_owned())
}

fn digest(seed: u8) -> EntryDigest {
    EntryDigest::new(FixedBytes::new([seed; 32]))
}

fn head(sequence: u64, seed: u8) -> HeadMark {
    HeadMark {
        sequence: CommitSequence::new(sequence),
        digest: digest(seed),
    }
}

fn envelope(sequence: u64, previous: Option<u8>, seed: u8) -> EntryEnvelope {
    EntryEnvelope {
        sequence: CommitSequence::new(sequence),
        previous_digest: previous.map(digest),
        digest: digest(seed),
        payload: PayloadBytes::new(Bytes::new(vec![0xaa, seed])),
    }
}

fn append(store_name: &str, expected: Option<HeadMark>, entries: Vec<EntryEnvelope>) -> Input {
    Input::Append(EntrySuffix {
        store: store(store_name),
        expected_head: expected,
        entries,
    })
}

fn object_notice(store_name: &str, announced: HeadMark) -> Input {
    Input::NotifyObject(ObjectNotice {
        store: store(store_name),
        head: announced,
        source: None,
    })
}

fn artifact(store_name: &str, sequence: u64, covered_end: u64) -> CheckpointArtifact {
    CheckpointArtifact {
        store: store(store_name),
        sequence: CheckpointSequence::new(sequence),
        covered_end: CommitSequence::new(covered_end),
        digest: ArtifactDigest::new(FixedBytes::new([0xcc; 32])),
        artifact: ArtifactBytes::new(Bytes::new(vec![9, 9, 9])),
    }
}

#[tokio::test]
async fn append_to_unregistered_store_is_rejected_typed() {
    let mut fixture = Fixture::new();
    let reply = fixture
        .handle(append("ghost", None, vec![envelope(1, None, 0x11)]))
        .await;
    match reply {
        Output::AppendRejected(rejection) => {
            assert_eq!(rejection.reason, AppendRejectionReason::UnknownStore);
        }
        other => panic!("expected AppendRejected, got {other:?}"),
    }
}

#[tokio::test]
async fn append_with_matching_expected_head_is_accepted_and_head_advances() {
    let mut fixture = Fixture::with_registered("spirit");
    let first = fixture
        .handle(append(
            "spirit",
            None,
            vec![envelope(1, None, 0x11), envelope(2, Some(0x11), 0x22)],
        ))
        .await;
    match first {
        Output::Appended(receipt) => assert_eq!(receipt.head, head(2, 0x22)),
        other => panic!("expected Appended, got {other:?}"),
    }

    let second = fixture
        .handle(append(
            "spirit",
            Some(head(2, 0x22)),
            vec![envelope(3, Some(0x22), 0x33)],
        ))
        .await;
    match second {
        Output::Appended(receipt) => assert_eq!(receipt.head, head(3, 0x33)),
        other => panic!("expected Appended, got {other:?}"),
    }

    let heads = fixture
        .handle(Input::ObserveHeads(HeadQuery::new(Some(store("spirit")))))
        .await;
    match heads {
        Output::HeadsObserved(listing) => {
            assert_eq!(listing.payload().len(), 1);
            assert_eq!(listing.payload()[0].head, Some(head(3, 0x33)));
        }
        other => panic!("expected HeadsObserved, got {other:?}"),
    }
}

#[tokio::test]
async fn duplicate_suffix_acknowledges_idempotently_without_duplicate_rows() {
    let mut fixture = Fixture::with_registered("spirit");
    let suffix = vec![envelope(1, None, 0x11), envelope(2, Some(0x11), 0x22)];
    let first = fixture.handle(append("spirit", None, suffix.clone())).await;
    assert!(matches!(first, Output::Appended(_)));
    let log_after_first = fixture
        .engine
        .store()
        .engine()
        .versioned_commit_log()
        .expect("versioned log reads");

    // The identical re-send: same head back, and the mirror's own
    // versioned log gains no new entry — nothing was rewritten.
    let second = fixture.handle(append("spirit", None, suffix)).await;
    match second {
        Output::Appended(receipt) => assert_eq!(receipt.head, head(2, 0x22)),
        other => panic!("expected idempotent Appended, got {other:?}"),
    }
    let log_after_second = fixture
        .engine
        .store()
        .engine()
        .versioned_commit_log()
        .expect("versioned log reads");
    assert_eq!(log_after_first.len(), log_after_second.len());
}

#[tokio::test]
async fn partially_duplicate_resend_appends_only_the_novel_remainder() {
    let mut fixture = Fixture::with_registered("spirit");
    let first = fixture
        .handle(append("spirit", None, vec![envelope(1, None, 0x11)]))
        .await;
    assert!(matches!(first, Output::Appended(_)));

    // Re-send entry 1 together with novel entry 2 under the original
    // expected head — the duplicate dedups, the remainder appends.
    let resend = fixture
        .handle(append(
            "spirit",
            None,
            vec![envelope(1, None, 0x11), envelope(2, Some(0x11), 0x22)],
        ))
        .await;
    match resend {
        Output::Appended(receipt) => assert_eq!(receipt.head, head(2, 0x22)),
        other => panic!("expected Appended, got {other:?}"),
    }
}

#[tokio::test]
async fn sequence_gap_is_rejected_typed() {
    let mut fixture = Fixture::with_registered("spirit");
    let first = fixture
        .handle(append("spirit", None, vec![envelope(1, None, 0x11)]))
        .await;
    assert!(matches!(first, Output::Appended(_)));

    // Entry 3 with expected head at 2: the mirror never saw entry 2.
    let gapped = fixture
        .handle(append(
            "spirit",
            Some(head(2, 0x22)),
            vec![envelope(3, Some(0x22), 0x33)],
        ))
        .await;
    match gapped {
        Output::AppendRejected(rejection) => {
            assert_eq!(rejection.reason, AppendRejectionReason::SequenceGap);
            assert_eq!(rejection.head, Some(head(1, 0x11)));
        }
        other => panic!("expected AppendRejected, got {other:?}"),
    }
}

#[tokio::test]
async fn forked_history_is_rejected_typed() {
    let mut fixture = Fixture::with_registered("spirit");
    let first = fixture
        .handle(append("spirit", None, vec![envelope(1, None, 0x11)]))
        .await;
    assert!(matches!(first, Output::Appended(_)));

    // A second writer claims a DIFFERENT entry 1 digest and continues
    // from it: the expected head digest disagrees with stored history.
    let forked = fixture
        .handle(append(
            "spirit",
            Some(head(1, 0x99)),
            vec![envelope(2, Some(0x99), 0x88)],
        ))
        .await;
    match forked {
        Output::AppendRejected(rejection) => {
            assert_eq!(rejection.reason, AppendRejectionReason::HeadForked);
        }
        other => panic!("expected AppendRejected, got {other:?}"),
    }

    // A divergent re-send of an already-stored sequence is a digest
    // mismatch, never a rewrite.
    let divergent = fixture
        .handle(append("spirit", None, vec![envelope(1, None, 0x99)]))
        .await;
    match divergent {
        Output::AppendRejected(rejection) => {
            assert_eq!(rejection.reason, AppendRejectionReason::DigestMismatch);
        }
        other => panic!("expected AppendRejected, got {other:?}"),
    }
}

#[tokio::test]
async fn empty_suffix_is_rejected_typed() {
    let mut fixture = Fixture::with_registered("spirit");
    let reply = fixture.handle(append("spirit", None, Vec::new())).await;
    match reply {
        Output::AppendRejected(rejection) => {
            assert_eq!(rejection.reason, AppendRejectionReason::EmptySuffix);
        }
        other => panic!("expected AppendRejected, got {other:?}"),
    }
}

#[tokio::test]
async fn crash_window_resend_re_advances_the_head_and_the_store_stays_live() {
    let directory = tempfile::tempdir().expect("temp dir");
    let mut crashed = Store::open(&directory.path().join("mirror.sema")).expect("store opens");
    crashed.register_store(&store("spirit")).expect("registers");
    // The crash window: `Store::persist_suffix` is two transactions —
    // the entry rows commit, the head advance does not. Drive the first
    // transaction through the same public seam persist_suffix uses,
    // leaving exactly the state a crash between the two leaves behind.
    crashed
        .commit_entry_rows(&NovelSuffix {
            store: store("spirit"),
            head: head(2, 0x22),
            entries: vec![envelope(1, None, 0x11), envelope(2, Some(0x11), 0x22)],
        })
        .expect("entry rows commit");
    let mut engine = Engine::new(crashed);

    // A DIVERGENT re-send into the crash window is a digest mismatch,
    // never a rewrite.
    let divergent = engine
        .handle(append("spirit", None, vec![envelope(1, None, 0x99)]))
        .await;
    match divergent {
        Output::AppendRejected(rejection) => {
            assert_eq!(rejection.reason, AppendRejectionReason::DigestMismatch);
        }
        other => panic!("expected AppendRejected, got {other:?}"),
    }

    // The shipper's idempotent re-send: the orphan rows dedup against
    // the loaded known rows and the head re-advances head-only. A
    // re-asserted row would have faulted the persist (sema-engine
    // rejects duplicate assert keys), so Appended IS the proof no row
    // was rewritten.
    let healed = engine
        .handle(append(
            "spirit",
            None,
            vec![envelope(1, None, 0x11), envelope(2, Some(0x11), 0x22)],
        ))
        .await;
    match healed {
        Output::Appended(receipt) => assert_eq!(receipt.head, head(2, 0x22)),
        other => panic!("expected the re-send to heal the crash window, got {other:?}"),
    }

    // The store stays live: the next append continues the chain.
    let appended = engine
        .handle(append(
            "spirit",
            Some(head(2, 0x22)),
            vec![envelope(3, Some(0x22), 0x33)],
        ))
        .await;
    match appended {
        Output::Appended(receipt) => assert_eq!(receipt.head, head(3, 0x33)),
        other => panic!("expected Appended after healing, got {other:?}"),
    }

    let heads = engine
        .handle(Input::ObserveHeads(HeadQuery::new(Some(store("spirit")))))
        .await;
    match heads {
        Output::HeadsObserved(listing) => {
            assert_eq!(listing.payload()[0].head, Some(head(3, 0x33)));
        }
        other => panic!("expected HeadsObserved, got {other:?}"),
    }
}

#[tokio::test]
async fn retire_then_reregister_resumes_the_surviving_chain() {
    let mut fixture = Fixture::with_registered("spirit");
    let first = fixture
        .handle(append(
            "spirit",
            None,
            vec![envelope(1, None, 0x11), envelope(2, Some(0x11), 0x22)],
        ))
        .await;
    assert!(matches!(first, Output::Appended(_)));

    let retired = fixture
        .engine
        .handle_meta(meta_signal_mirror::Input::RetireStore(
            meta_signal_mirror::StoreRetirement::new(meta_signal_mirror::StoreName::new(
                "spirit".to_owned(),
            )),
        ));
    assert!(matches!(
        retired,
        meta_signal_mirror::Output::StoreRetired(_)
    ));

    // Re-registration RESUMES: retirement keeps the received entries
    // (retention enforcement is deferred), so the head restores from
    // the highest surviving entry row instead of restarting at genesis
    // and faulting on re-asserted rows.
    let reregistered = fixture
        .engine
        .handle_meta(meta_signal_mirror::Input::RegisterStore(
            meta_signal_mirror::StoreRegistration::new(meta_signal_mirror::StoreName::new(
                "spirit".to_owned(),
            )),
        ));
    assert!(matches!(
        reregistered,
        meta_signal_mirror::Output::StoreRegistered(_)
    ));

    let heads = fixture
        .handle(Input::ObserveHeads(HeadQuery::new(Some(store("spirit")))))
        .await;
    match heads {
        Output::HeadsObserved(listing) => {
            assert_eq!(listing.payload()[0].head, Some(head(2, 0x22)));
        }
        other => panic!("expected HeadsObserved, got {other:?}"),
    }

    // The shipper's full re-send acknowledges idempotently...
    let resend = fixture
        .handle(append(
            "spirit",
            None,
            vec![envelope(1, None, 0x11), envelope(2, Some(0x11), 0x22)],
        ))
        .await;
    match resend {
        Output::Appended(receipt) => assert_eq!(receipt.head, head(2, 0x22)),
        other => panic!("expected idempotent Appended, got {other:?}"),
    }

    // ...and the chain continues from the resumed head.
    let appended = fixture
        .handle(append(
            "spirit",
            Some(head(2, 0x22)),
            vec![envelope(3, Some(0x22), 0x33)],
        ))
        .await;
    match appended {
        Output::Appended(receipt) => assert_eq!(receipt.head, head(3, 0x33)),
        other => panic!("expected Appended after resume, got {other:?}"),
    }
}

#[tokio::test]
async fn object_notice_for_unregistered_store_is_rejected_typed() {
    let mut fixture = Fixture::new();
    let reply = fixture.handle(object_notice("ghost", head(1, 0x11))).await;
    match reply {
        Output::ObjectNoticeRejected(rejection) => {
            assert_eq!(rejection.reason, ObjectNoticeRejectionReason::UnknownStore);
            assert_eq!(rejection.head, None);
        }
        other => panic!("expected ObjectNoticeRejected, got {other:?}"),
    }
}

#[tokio::test]
async fn object_notice_for_known_head_is_accepted() {
    let mut fixture = Fixture::with_registered("spirit");
    let appended = fixture
        .handle(append(
            "spirit",
            None,
            vec![envelope(1, None, 0x11), envelope(2, Some(0x11), 0x22)],
        ))
        .await;
    assert!(matches!(appended, Output::Appended(_)));

    let reply = fixture.handle(object_notice("spirit", head(2, 0x22))).await;
    match reply {
        Output::ObjectNoticeAccepted(receipt) => {
            assert_eq!(receipt.store, store("spirit"));
            assert_eq!(receipt.head, head(2, 0x22));
        }
        other => panic!("expected ObjectNoticeAccepted, got {other:?}"),
    }
}

#[tokio::test]
async fn object_notice_for_missing_head_reports_current_head() {
    let mut fixture = Fixture::with_registered("spirit");
    let appended = fixture
        .handle(append("spirit", None, vec![envelope(1, None, 0x11)]))
        .await;
    assert!(matches!(appended, Output::Appended(_)));

    let reply = fixture.handle(object_notice("spirit", head(2, 0x22))).await;
    match reply {
        Output::ObjectNoticeRejected(rejection) => {
            assert_eq!(rejection.reason, ObjectNoticeRejectionReason::HeadBehind);
            assert_eq!(rejection.head, Some(head(1, 0x11)));
        }
        other => panic!("expected ObjectNoticeRejected, got {other:?}"),
    }
}

#[tokio::test]
async fn separator_bearing_store_name_is_refused_at_registration() {
    let mut fixture = Fixture::new();
    let refused = fixture
        .engine
        .handle_meta(meta_signal_mirror::Input::RegisterStore(
            meta_signal_mirror::StoreRegistration::new(meta_signal_mirror::StoreName::new(
                "spirit/evil".to_owned(),
            )),
        ));
    match refused {
        meta_signal_mirror::Output::OrderRejected(rejection) => {
            assert_eq!(
                rejection.reason,
                meta_signal_mirror::OrderRejectionReason::StoreNameInvalid
            );
        }
        other => panic!("expected OrderRejected, got {other:?}"),
    }
}

#[test]
fn the_mirror_own_store_name_is_pinned() {
    // The mirror's own versioned ledger registers under the emitted
    // schema identity. Deploy tooling and restore paths name it; this
    // pin catches silent drift in the emission.
    assert_eq!(RecordFamily::STORE_NAME, "mirror:sema");
}

#[tokio::test]
async fn restore_returns_checkpoint_plus_suffix_past_its_coverage() {
    let mut fixture = Fixture::with_registered("spirit");
    let appended = fixture
        .handle(append(
            "spirit",
            None,
            vec![
                envelope(1, None, 0x11),
                envelope(2, Some(0x11), 0x22),
                envelope(3, Some(0x22), 0x33),
            ],
        ))
        .await;
    assert!(matches!(appended, Output::Appended(_)));

    let published = fixture
        .handle(Input::PublishCheckpoint(artifact("spirit", 1, 2)))
        .await;
    assert!(matches!(published, Output::CheckpointPublished(_)));

    let restored = fixture
        .handle(Input::Restore(RestoreQuery::new(store("spirit"))))
        .await;
    match restored {
        Output::Restored(bundle) => {
            assert_eq!(bundle.checkpoint, artifact("spirit", 1, 2));
            assert_eq!(bundle.suffix, vec![envelope(3, Some(0x22), 0x33)]);
        }
        other => panic!("expected Restored, got {other:?}"),
    }
}

#[tokio::test]
async fn restore_without_checkpoint_is_rejected_typed() {
    let mut fixture = Fixture::with_registered("spirit");
    let reply = fixture
        .handle(Input::Restore(RestoreQuery::new(store("spirit"))))
        .await;
    match reply {
        Output::RestoreRejected(rejection) => {
            assert_eq!(rejection.reason, RestoreRejectionReason::NoCheckpoint);
        }
        other => panic!("expected RestoreRejected, got {other:?}"),
    }
}

#[tokio::test]
async fn the_mirror_ledger_is_itself_a_versioned_store() {
    let mut fixture = Fixture::with_registered("spirit");
    let appended = fixture
        .handle(append("spirit", None, vec![envelope(1, None, 0x11)]))
        .await;
    assert!(matches!(appended, Output::Appended(_)));

    // Dogfooding (Spirit 0yx5): the registration and the append landed
    // in the mirror's OWN versioned commit log and mirror outbox.
    let mirror_engine = fixture.engine.store().engine();
    let log = mirror_engine
        .versioned_commit_log()
        .expect("the mirror's own versioned log reads");
    assert!(log.len() >= 3, "registration + entries + head advance");
    let outbox = mirror_engine
        .unshipped_outbox()
        .expect("the mirror's own outbox reads");
    assert_eq!(outbox.len(), log.len());
}
