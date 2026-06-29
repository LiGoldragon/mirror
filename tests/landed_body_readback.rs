//! THE WIRE READBACK: a REAL content-addressed record body lands in the mirror,
//! and the witness reads that exact body back OUT of the mirror over its own
//! `signal-mirror` working contract — then re-derives the body's content address
//! and reproduces the landed head.
//!
//! The fast router witness (`router/tests/criome_forward_lands_in_mirror.rs`)
//! re-hashes the landed body through the IN-PROCESS `Store::landed_entries`. The
//! two-VM witness has no in-process handle to the remote mirror — it has only the
//! deployment wire. The mirror's working contract carries no read-one-body op
//! (`ObserveHeads` returns only the head digest), and adding one would split the
//! 0.1.1 `signal-mirror` pin across every component that links it (router and
//! spirit both do), so this proves the readback over the EXISTING wire ops:
//! `PublishCheckpoint` a zero-coverage checkpoint, then `Restore` — whose bundle
//! suffix carries the FULL landed body for every entry past the checkpoint's
//! `covered_end`. With `covered_end = 0`, that suffix is the whole chain,
//! genesis included.
//!
//! The witnessed claim: the body the mirror hands back over `Restore` is the
//! exact body that landed, and re-deriving its digest through sema-engine's own
//! `VersionedCommitLogEntry::new` reproduces the head — so the two-VM witness can
//! re-hash a remote mirror's landed body with no in-process access and no new
//! wire op.

use mirror::{Engine, LandedBody, Store};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_mirror::{
    ArtifactBytes, ArtifactDigest, Bytes, CheckpointArtifact, CheckpointSequence, CommitSequence,
    EntryDigest, EntryEnvelope, EntrySuffix, FixedBytes, Input, Output, PayloadBytes, RestoreQuery,
    StoreName,
};

/// A source component's domain record, content-addressed by sema-engine exactly
/// as Spirit's records are — the same fixture shape the router witness uses, so
/// the body this test lands is the same kind the production shipper ships.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
struct WitnessRecord {
    key: String,
    body: String,
}

impl WitnessRecord {
    fn new(key: impl Into<String>, body: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            body: body.into(),
        }
    }
}

impl sema_engine::EngineRecord for WitnessRecord {
    fn record_key(&self) -> sema_engine::RecordKey {
        sema_engine::RecordKey::new(self.key.clone())
    }
}

/// Build a REAL versioned sema-engine store's genesis entry and return the wire
/// envelope the mirror lands for it — the rkyv `VersionedCommitLogEntry` as the
/// body and the entry's own content address as the carried digest — plus the
/// head digest the store reports (the value Spirit's `ObserveHead` returns).
fn real_genesis_envelope(store: &str) -> (EntryEnvelope, sema_engine::EntryDigest) {
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
            WitnessRecord::new("witness-record-1", "criome auth witness record"),
        ))
        .expect("assert the witness record");

    let log = engine
        .versioned_commit_log()
        .expect("read the versioned commit log");
    let genesis = log
        .last()
        .expect("the asserted record produced a genesis entry");
    let real_head = genesis.entry_digest();
    // The body is byte-for-byte what `ComponentShipper::envelope_for_entry`
    // ships: the same `rkyv::to_bytes::<rancor::Error>` call.
    let body = rkyv::to_bytes::<rkyv::rancor::Error>(genesis)
        .expect("serialize the genesis entry")
        .to_vec();
    let envelope = EntryEnvelope::new(
        CommitSequence::new(1),
        None,
        EntryDigest::new(FixedBytes::new(*real_head.bytes())),
        PayloadBytes::new(Bytes::new(body)),
    );
    (envelope, real_head)
}

struct Mirror {
    _directory: tempfile::TempDir,
    engine: Engine,
}

impl Mirror {
    /// A mirror with one registered store, empty.
    fn with_registered(store_name: &str) -> Self {
        let directory = tempfile::tempdir().expect("mirror store directory");
        let store = Store::open(&directory.path().join("mirror.sema")).expect("mirror store opens");
        let mut engine = Engine::new(store);
        let registered = engine.handle_meta(meta_signal_mirror::Input::RegisterStore(
            meta_signal_mirror::StoreRegistration {
                store: meta_signal_mirror::StoreName::new(store_name.to_owned()),
                addressing: meta_signal_mirror::ContentAddressing::Opaque,
            },
        ));
        assert!(matches!(
            registered,
            meta_signal_mirror::Output::StoreRegistered(_)
        ));
        Self {
            _directory: directory,
            engine,
        }
    }

    async fn handle(&mut self, input: Input) -> Output {
        self.engine.handle(input).await
    }
}

/// A zero-coverage checkpoint: payload-blind, `covered_end = 0`, so the
/// subsequent `Restore` suffix is the whole chain (every entry past sequence 0).
/// This is the witness's readback handshake — the mirror accepts any artifact for
/// a registered store with no prior checkpoint.
fn zero_coverage_checkpoint(store_name: &str) -> Input {
    Input::PublishCheckpoint(CheckpointArtifact {
        store: StoreName::new(store_name.to_owned()),
        sequence: CheckpointSequence::new(1),
        covered_end: CommitSequence::new(0),
        digest: ArtifactDigest::new(FixedBytes::new([0xcc; 32])),
        artifact: ArtifactBytes::new(Bytes::new(Vec::new())),
    })
}

#[tokio::test]
async fn restore_hands_back_the_landed_genesis_body_which_rehashes_to_the_head() {
    let store_name = "spirit";
    let (envelope, real_head) = real_genesis_envelope(store_name);
    let shipped_body = envelope.payload.as_slice().to_vec();
    assert_ne!(
        shipped_body,
        b"criome-verified durable append".to_vec(),
        "the landed body is the real versioned-log entry, not the old placeholder"
    );

    let mut mirror = Mirror::with_registered(store_name);

    // Land the real genesis body.
    let appended = mirror
        .handle(Input::Append(EntrySuffix::from_entries(
            StoreName::new(store_name.to_owned()),
            None,
            vec![envelope],
        )))
        .await;
    assert!(
        matches!(appended, Output::Appended(_)),
        "the genesis append lands, got {appended:?}"
    );

    // The readback handshake: a zero-coverage checkpoint, then Restore.
    let published = mirror.handle(zero_coverage_checkpoint(store_name)).await;
    assert!(
        matches!(published, Output::CheckpointPublished(_)),
        "a zero-coverage checkpoint publishes, got {published:?}"
    );

    let restored = mirror
        .handle(Input::Restore(RestoreQuery::new(StoreName::new(
            store_name.to_owned(),
        ))))
        .await;
    let bundle = match restored {
        Output::Restored(bundle) => bundle,
        other => panic!("expected Restored, got {other:?}"),
    };

    // THE READBACK: the mirror hands back the genesis body it landed, intact.
    let suffix = bundle.suffix();
    assert_eq!(
        suffix.len(),
        1,
        "the zero-coverage restore suffix is the whole chain"
    );
    let landed = &suffix[0];
    assert_eq!(
        landed.sequence,
        CommitSequence::new(1),
        "the restored entry is the genesis"
    );
    assert_eq!(
        landed.payload.as_slice(),
        shipped_body.as_slice(),
        "Restore handed back the EXACT landed body — the real entry, intact"
    );

    // THE PROOF: re-derive the content address from the body read back over the
    // wire, reproducing the head — sema-engine's own content-addressing, through
    // the SAME `LandedBody::content_address` the two-VM witness verifier bin uses.
    let rederived = LandedBody::new(landed.payload.as_slice())
        .content_address()
        .expect("the restored body is a genuine rkyv VersionedCommitLogEntry");
    assert_eq!(
        rederived, real_head,
        "re-deriving the digest from the RESTORED body reproduces the record's real head"
    );
    assert_eq!(
        landed.digest.as_bytes(),
        rederived.bytes(),
        "the landed head digest is the genuine content address of the body Restore handed back"
    );

    eprintln!(
        "RESTORE_READBACK head = {} body octets = {}",
        rederived,
        shipped_body.len()
    );
}
