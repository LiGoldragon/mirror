//! THE APPEND-TIME ADDRESSING WITNESS: a `SemaVersionedLog` store recomputes
//! every body's content address at append and REFUSES a digest-mismatched body
//! BEFORE it lands — while an `Opaque` control store stays payload-blind and
//! lands the same tampered body unchanged.
//!
//! The canonical attack this proves the mirror now catches: a body whose carried
//! `EntryEnvelope.digest` chains correctly (genesis sequence 1, no previous
//! digest) but whose `payload` is the rkyv body of a DIFFERENT genuine entry, so
//! it does not hash to the carried digest. The three linkage guards
//! (`suffix_inconsistency`, `expected_head_violation`, `known_divergence`) all
//! pass; only the new `body_addressing_violation` guard — recomputing through
//! sema-engine's own content-addressing via `LandedBody::addresses_to` — catches
//! it. The refusal routes to `Output::AppendRejected` and never reaches
//! `persist_suffix`, so nothing is committed or fsynced: `landed_entries` stays
//! empty and the head stays `None`.
//!
//! The same body lands faithfully when its carried digest matches, and re-hashing
//! the landed body reproduces the head — so the guard refuses tampering without
//! refusing genuine history. The `Opaque` control lands the tampered body,
//! witnessing that `Opaque` is the unchanged, payload-blind default (Spirit
//! 0yx5): the policy bends the mirror's posture only for the store that opts in.

use mirror::{Engine, LandedBody, Store};
use signal_mirror::{
    Bytes, CommitSequence, EntryDigest, EntryEnvelope, EntrySuffix, FixedBytes, HeadQuery, Input,
    Output, PayloadBytes, StoreName,
};

/// A source component's domain record, content-addressed by sema-engine exactly
/// as Spirit's records are — the same fixture shape the readback witness uses.
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

/// Build a REAL versioned sema-engine genesis entry and return the wire envelope
/// the mirror lands for it (the rkyv `VersionedCommitLogEntry` as the body and
/// the entry's own content address as the carried digest) plus the head digest
/// the store reports. Distinct `key`/`body` produce a distinct content address.
fn real_genesis_envelope(
    store: &str,
    key: &str,
    body: &str,
) -> (EntryEnvelope, sema_engine::EntryDigest) {
    let directory = tempfile::tempdir().expect("source store directory");
    let mut engine = sema_engine::Engine::open(
        sema_engine::EngineOpen::new(
            directory.path().join("source.sema"),
            sema_engine::SchemaVersion::new(1),
        )
        .with_versioning(sema_engine::VersioningPolicy::new(
            sema_engine::VersionedStoreName::new(store),
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
        .expect("assert the witness record");

    let log = engine
        .versioned_commit_log()
        .expect("read the versioned commit log");
    let genesis = log
        .last()
        .expect("the asserted record produced a genesis entry");
    let real_head = genesis.entry_digest();
    let octets = rkyv::to_bytes::<rkyv::rancor::Error>(genesis)
        .expect("serialize the genesis entry")
        .to_vec();
    let envelope = EntryEnvelope::new(
        CommitSequence::new(1),
        None,
        EntryDigest::new(FixedBytes::new(*real_head.bytes())),
        PayloadBytes::new(Bytes::new(octets)),
    );
    (envelope, real_head)
}

fn store(name: &str) -> StoreName {
    StoreName::new(name.to_owned())
}

fn register(engine: &mut Engine, name: &str, addressing: meta_signal_mirror::ContentAddressing) {
    let registered = engine.handle_meta(meta_signal_mirror::Input::RegisterStore(
        meta_signal_mirror::StoreRegistration {
            store: meta_signal_mirror::StoreName::new(name.to_owned()),
            addressing,
        },
    ));
    assert!(
        matches!(registered, meta_signal_mirror::Output::StoreRegistered(_)),
        "store {name} registers, got {registered:?}"
    );
}

fn genesis_append(store_name: &str, envelope: EntryEnvelope) -> Input {
    Input::Append(EntrySuffix::from_entries(
        store(store_name),
        None,
        vec![envelope],
    ))
}

async fn observed_head(engine: &mut Engine, store_name: &str) -> Option<signal_mirror::HeadMark> {
    match engine
        .handle(Input::ObserveHeads(HeadQuery::new(Some(store(store_name)))))
        .await
    {
        Output::HeadsObserved(listing) => {
            assert_eq!(listing.heads().len(), 1, "exactly the one registered store");
            listing.heads()[0].head().cloned()
        }
        other => panic!("expected HeadsObserved, got {other:?}"),
    }
}

#[tokio::test]
async fn refuses_mismatched_body_and_lands_matching_body() {
    let directory = tempfile::tempdir().expect("mirror store directory");
    let store_path = directory.path().join("mirror.sema");
    let mut engine = Engine::new(Store::open(&store_path).expect("mirror store opens"));

    // One store opts into content-address enforcement; one stays Opaque.
    register(
        &mut engine,
        "spirit",
        meta_signal_mirror::ContentAddressing::SemaVersionedLog,
    );
    register(
        &mut engine,
        "control",
        meta_signal_mirror::ContentAddressing::Opaque,
    );

    // The faithful genesis body and a DIFFERENT genuine genesis body.
    let (faithful, real_head) =
        real_genesis_envelope("spirit", "witness-record-1", "criome auth witness record");
    let (other, other_head) =
        real_genesis_envelope("spirit", "witness-record-2", "a different genuine record");
    assert_ne!(
        real_head.bytes(),
        other_head.bytes(),
        "the two genuine bodies have different content addresses"
    );

    // The attack: carried digest from the faithful entry (so all three linkage
    // guards pass — genesis sequence 1, no previous), payload from the OTHER
    // genuine entry (so it decodes but re-derives a different digest).
    let tampered = EntryEnvelope::new(
        CommitSequence::new(1),
        None,
        EntryDigest::new(FixedBytes::new(*real_head.bytes())),
        PayloadBytes::new(Bytes::new(other.payload.as_slice().to_vec())),
    );
    // The tampered body does decode — it is a genuine versioned-log entry — but
    // to the OTHER head, not the carried one. That is the silent corruption the
    // append-time guard must catch.
    assert_eq!(
        LandedBody::new(tampered.payload.as_slice())
            .content_address()
            .expect("the tampered body is itself a genuine versioned-log entry"),
        other_head,
        "the tampered body re-derives to the other head, not its carried digest"
    );

    // (a) SemaVersionedLog refuses the tampered body — DigestMismatch.
    let refused = engine
        .handle(genesis_append("spirit", tampered.clone()))
        .await;
    match refused {
        Output::AppendRejected(rejection) => assert_eq!(
            rejection.reason,
            signal_mirror::AppendRejectionReason::DigestMismatch,
            "the tampered body is refused as a digest mismatch"
        ),
        other => panic!("expected AppendRejected, got {other:?}"),
    }
    // NOT PERSISTED: the refusal never reached persist_suffix.
    assert!(
        engine
            .store()
            .landed_entries(&store("spirit"))
            .expect("landed entries read")
            .is_empty(),
        "the refused body left no row"
    );
    assert_eq!(
        observed_head(&mut engine, "spirit").await,
        None,
        "the refused body did not advance the head"
    );

    // (b) The faithful body lands and re-hashes to the head.
    let appended = engine.handle(genesis_append("spirit", faithful)).await;
    assert!(
        matches!(appended, Output::Appended(_)),
        "the faithful genesis lands, got {appended:?}"
    );
    let landed = engine
        .store()
        .landed_entries(&store("spirit"))
        .expect("landed entries read");
    assert_eq!(landed.len(), 1, "exactly the faithful genesis landed");
    assert_eq!(
        LandedBody::new(landed[0].payload.as_slice())
            .content_address()
            .expect("the landed body is a genuine versioned-log entry"),
        real_head,
        "the landed body re-hashes to the carried head",
    );

    // (c) The Opaque control lands the SAME tampered body unchanged.
    let control = engine.handle(genesis_append("control", tampered)).await;
    assert!(
        matches!(control, Output::Appended(_)),
        "an Opaque store is payload-blind and lands the tampered body, got {control:?}"
    );
    assert_eq!(
        engine
            .store()
            .landed_entries(&store("control"))
            .expect("control landed entries read")
            .len(),
        1,
        "the Opaque control landed the tampered body unchanged",
    );
}
