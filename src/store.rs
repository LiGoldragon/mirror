//! The mirror's durable SEMA plane over `sema-engine`.
//!
//! The mirror's own ledger is itself a VERSIONED sema-engine store
//! (Spirit 0yx5): `Store::open` enables the generated
//! `RecordFamily::versioning_policy()`, so every registration, received
//! entry, checkpoint artifact, and retention setting lands in the
//! mirror's own versioned commit log and mirror outbox. The acknowledge
//! boundary: every method here returns only after the underlying redb
//! write transaction committed (redb is durable at commit), so the
//! Nexus reply that follows a returned receipt is an
//! ack-after-durable-write.

use std::path::Path;

use sema_engine::{
    Assertion, CommitRequest, Engine, EngineOpen, EngineRecord, KeyRange, Mutation, QueryPlan,
    RecordKey, Retraction, SchemaVersion, TableReference,
};
use signal_mirror::{
    AppendReceipt, ArtifactBytes, ArtifactDigest, CheckpointArtifact, CheckpointReceipt,
    CommitSequence, EntryDigest, EntryEnvelope, EntrySuffix, HeadListing, HeadMark, HeadQuery,
    ObjectNotice, PayloadBytes, RestoreBundle, RestoreQuery, RestoreRejection,
    RestoreRejectionReason, StoreHead, StoreName,
};

use crate::error::Result;
use crate::schema::sema::{
    Bytes, CheckedAppend, CheckedCheckpoint, CheckedObjectNotice, ContentAddressing, DigestBytes,
    Entries, HeadStamp, KnownEntries, KnownEntry, LatestCheckpoint, LedgerHead, NovelSuffix,
    PreviousDigest, ReceivedEntry, RecordFamily, RegisteredLedger, RetentionRule, RetentionSetting,
    Scope, StoreLedger, StorePolicy, StoredCheckpoint, StoredHead, StoredHeadStamp,
};

/// Key separator between a store name and an ordering suffix. Component
/// store names come from the `VersionedStoreName` vocabulary and do not
/// carry `/`.
const KEY_SEPARATOR: char = '/';

/// Zero-padded width for sequence key suffixes, so lexicographic key
/// order equals numeric sequence order.
const SEQUENCE_KEY_WIDTH: usize = 20;

impl EngineRecord for StoredHead {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.store.clone())
    }
}

impl EngineRecord for ReceivedEntry {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!(
            "{}{}{:0width$}",
            self.store,
            KEY_SEPARATOR,
            self.sequence,
            width = SEQUENCE_KEY_WIDTH
        ))
    }
}

impl EngineRecord for StoredCheckpoint {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!(
            "{}{}{:0width$}",
            self.store,
            KEY_SEPARATOR,
            self.sequence,
            width = SEQUENCE_KEY_WIDTH
        ))
    }
}

impl EngineRecord for RetentionSetting {
    fn record_key(&self) -> RecordKey {
        match self.scope.payload() {
            Some(store) => RecordKey::new(format!("store{KEY_SEPARATOR}{store}")),
            None => RecordKey::new("all-stores".to_owned()),
        }
    }
}

impl EngineRecord for StorePolicy {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.store.clone())
    }
}

impl ContentAddressing {
    /// Map the meta wire policy onto the mirror's SELF-CONTAINED local
    /// policy at the store boundary — the sibling of
    /// `RetentionSetting::from_order`. The two enums are deliberately
    /// distinct types (wire vs durable shape); this is their one contact
    /// point.
    pub fn from_meta(addressing: &meta_signal_mirror::ContentAddressing) -> Self {
        match addressing {
            meta_signal_mirror::ContentAddressing::Opaque => Self::Opaque,
            meta_signal_mirror::ContentAddressing::SemaVersionedLog => Self::SemaVersionedLog,
        }
    }
}

impl NovelSuffix {
    pub fn new(store: StoreName, head: HeadMark, entries: Vec<EntryEnvelope>) -> Self {
        Self {
            store,
            head,
            entries: Entries::new(entries),
        }
    }

    pub fn entries(&self) -> &[EntryEnvelope] {
        self.entries.payload()
    }
}

impl RegisteredLedger {
    pub fn new(
        head: Option<HeadMark>,
        known: Vec<KnownEntry>,
        latest_checkpoint: Option<CheckpointReceipt>,
        addressing: ContentAddressing,
    ) -> Self {
        Self {
            ledger_head: LedgerHead::new(head),
            known_entries: KnownEntries::new(known),
            latest_checkpoint: LatestCheckpoint::new(latest_checkpoint),
            addressing,
        }
    }

    pub fn head(&self) -> Option<&HeadMark> {
        self.ledger_head.payload().as_ref()
    }

    pub fn known(&self) -> &[KnownEntry] {
        self.known_entries.payload()
    }

    pub fn latest_checkpoint(&self) -> Option<&CheckpointReceipt> {
        self.latest_checkpoint.payload().as_ref()
    }

    /// The store's content-addressing policy. A store registered before
    /// the policy family existed has no row and reads as `Opaque`, so
    /// the append guard is a no-op for it — absence is the normal case.
    pub fn addressing(&self) -> ContentAddressing {
        self.addressing
    }
}

impl StoredHead {
    pub fn new(store: String, head: Option<HeadStamp>) -> Self {
        Self {
            store,
            stored_head_stamp: StoredHeadStamp::new(head),
        }
    }

    pub fn head(&self) -> Option<&HeadStamp> {
        self.stored_head_stamp.payload().as_ref()
    }
}

impl DigestBytes {
    pub fn from_entry_digest(digest: &EntryDigest) -> Self {
        Self::new(crate::schema::sema::FixedBytes::new(*digest.as_bytes()))
    }

    pub fn from_artifact_digest(digest: &ArtifactDigest) -> Self {
        Self::new(crate::schema::sema::FixedBytes::new(*digest.as_bytes()))
    }

    pub fn to_entry_digest(&self) -> EntryDigest {
        EntryDigest::new(signal_mirror::FixedBytes::new(*self.payload().payload()))
    }

    pub fn to_artifact_digest(&self) -> ArtifactDigest {
        ArtifactDigest::new(signal_mirror::FixedBytes::new(*self.payload().payload()))
    }
}

impl HeadStamp {
    pub fn from_mark(mark: &HeadMark) -> Self {
        Self {
            sequence: mark.sequence.clone().into_u64(),
            digest: DigestBytes::from_entry_digest(&mark.digest),
        }
    }

    pub fn to_mark(&self) -> HeadMark {
        HeadMark {
            sequence: CommitSequence::new(self.sequence),
            digest: self.digest.to_entry_digest(),
        }
    }
}

impl ReceivedEntry {
    pub fn from_envelope(store: &StoreName, envelope: &EntryEnvelope) -> Self {
        Self {
            store: store.as_str().to_owned(),
            sequence: envelope.sequence.clone().into_u64(),
            previous_digest: PreviousDigest::new(
                envelope
                    .previous_digest()
                    .map(DigestBytes::from_entry_digest),
            ),
            digest: DigestBytes::from_entry_digest(&envelope.digest),
            payload: Bytes::new(envelope.payload.as_slice().to_vec()),
        }
    }

    pub fn to_envelope(&self) -> EntryEnvelope {
        EntryEnvelope::new(
            CommitSequence::new(self.sequence),
            self.previous_digest().map(DigestBytes::to_entry_digest),
            self.digest.to_entry_digest(),
            PayloadBytes::new(signal_mirror::Bytes::new(self.payload.payload().to_vec())),
        )
    }

    pub fn previous_digest(&self) -> Option<&DigestBytes> {
        self.previous_digest.payload().as_ref()
    }

    pub fn to_known_entry(&self) -> KnownEntry {
        KnownEntry {
            sequence: CommitSequence::new(self.sequence),
            digest: self.digest.to_entry_digest(),
        }
    }

    pub fn to_head_stamp(&self) -> HeadStamp {
        HeadStamp {
            sequence: self.sequence,
            digest: self.digest.clone(),
        }
    }
}

impl StoredCheckpoint {
    pub fn from_artifact(artifact: &CheckpointArtifact) -> Self {
        Self {
            store: artifact.store.as_str().to_owned(),
            sequence: artifact.sequence.clone().into_u64(),
            covered_end: artifact.covered_end.clone().into_u64(),
            digest: DigestBytes::from_artifact_digest(&artifact.digest),
            artifact: Bytes::new(artifact.artifact.as_slice().to_vec()),
        }
    }

    pub fn to_artifact(&self) -> CheckpointArtifact {
        CheckpointArtifact {
            store: StoreName::new(self.store.clone()),
            sequence: signal_mirror::CheckpointSequence::new(self.sequence),
            covered_end: CommitSequence::new(self.covered_end),
            digest: self.digest.to_artifact_digest(),
            artifact: ArtifactBytes::new(signal_mirror::Bytes::new(
                self.artifact.payload().to_vec(),
            )),
        }
    }

    pub fn to_receipt(&self) -> CheckpointReceipt {
        CheckpointReceipt {
            store: StoreName::new(self.store.clone()),
            sequence: signal_mirror::CheckpointSequence::new(self.sequence),
            covered_end: CommitSequence::new(self.covered_end),
        }
    }
}

impl RetentionSetting {
    pub fn from_order(order: &meta_signal_mirror::RetentionOrder) -> Self {
        Self {
            scope: Scope::new(match &order.scope {
                meta_signal_mirror::RetentionScope::Store(store) => Some(store.as_str().to_owned()),
                meta_signal_mirror::RetentionScope::AllStores => None,
            }),
            rule: match &order.rule {
                meta_signal_mirror::RetentionRule::KeepEverything => RetentionRule::KeepEverything,
                meta_signal_mirror::RetentionRule::KeepLatestCheckpoints(count) => {
                    RetentionRule::KeepLatestCheckpoints(crate::schema::sema::KeepCount::new(
                        *count.payload(),
                    ))
                }
            },
        }
    }
}

/// The mirror's durable store: the versioned sema-engine plus the four
/// registered family tables. The single writer of the daemon — every
/// mutation flows through the engine actor that owns this value.
pub struct Store {
    engine: Engine,
    heads: TableReference<StoredHead>,
    entries: TableReference<ReceivedEntry>,
    checkpoints: TableReference<StoredCheckpoint>,
    retention: TableReference<RetentionSetting>,
    policies: TableReference<StorePolicy>,
}

impl Store {
    /// Open (or create) the mirror's own versioned store and register the
    /// generated record families.
    pub fn open(path: &Path) -> Result<Self> {
        let mut engine = Engine::open(
            EngineOpen::new(path, SchemaVersion::new(1))
                .with_versioning(RecordFamily::versioning_policy()),
        )?;
        let heads = engine.register_table(RecordFamily::head_family())?;
        let entries = engine.register_table(RecordFamily::entry_family())?;
        let checkpoints = engine.register_table(RecordFamily::checkpoint_family())?;
        let retention = engine.register_table(RecordFamily::retention_family())?;
        let policies = engine.register_table(RecordFamily::policy_family())?;
        Ok(Self {
            engine,
            heads,
            entries,
            checkpoints,
            retention,
            policies,
        })
    }

    fn head_row(&self, store: &StoreName) -> Result<Option<StoredHead>> {
        let snapshot = self.engine.match_records(QueryPlan::key(
            self.heads,
            RecordKey::new(store.as_str().to_owned()),
        ))?;
        Ok(snapshot.records().first().cloned())
    }

    /// The store's content-addressing policy row. A store registered
    /// before the policy family existed has none; the caller defaults
    /// such a store to `Opaque`, so absence is the payload-blind normal
    /// case rather than a branch to remember.
    fn policy_row(&self, store: &StoreName) -> Result<Option<StorePolicy>> {
        let snapshot = self.engine.match_records(QueryPlan::key(
            self.policies,
            RecordKey::new(store.as_str().to_owned()),
        ))?;
        Ok(snapshot.records().first().cloned())
    }

    fn entry_rows(&self, range: KeyRange) -> Result<Vec<ReceivedEntry>> {
        let snapshot = self
            .engine
            .match_records(QueryPlan::key_range(self.entries, range))?;
        Ok(snapshot.records().to_vec())
    }

    fn sequence_key(store: &StoreName, sequence: u64) -> RecordKey {
        RecordKey::new(format!(
            "{}{}{:0width$}",
            store.as_str(),
            KEY_SEPARATOR,
            sequence,
            width = SEQUENCE_KEY_WIDTH
        ))
    }

    fn latest_checkpoint_row(&self, store: &StoreName) -> Result<Option<StoredCheckpoint>> {
        let range = KeyRange::between(
            Self::sequence_key(store, 0),
            Self::sequence_key(store, u64::MAX),
        );
        let snapshot = self
            .engine
            .match_records(QueryPlan::key_range(self.checkpoints, range))?;
        Ok(snapshot.records().last().cloned())
    }

    /// Load the ledger state a pending append or checkpoint decision
    /// needs: registration, current head, the known entries overlapping
    /// the request's sequence range, and the latest checkpoint receipt.
    pub fn load_ledger(
        &self,
        store: &StoreName,
        sequence_range: Option<(u64, u64)>,
    ) -> Result<StoreLedger> {
        let Some(head_row) = self.head_row(store)? else {
            return Ok(StoreLedger::Unregistered);
        };
        let known = match sequence_range {
            Some((first, last)) => self
                .entry_rows(KeyRange::between(
                    Self::sequence_key(store, first),
                    Self::sequence_key(store, last),
                ))?
                .iter()
                .map(ReceivedEntry::to_known_entry)
                .collect(),
            None => Vec::new(),
        };
        let addressing = self
            .policy_row(store)?
            .map(|policy| policy.addressing)
            .unwrap_or(ContentAddressing::Opaque);
        Ok(StoreLedger::Registered(RegisteredLedger::new(
            head_row.head().map(HeadStamp::to_mark),
            known,
            self.latest_checkpoint_row(store)?
                .as_ref()
                .map(StoredCheckpoint::to_receipt),
            addressing,
        )))
    }

    /// Look up ledger state for a pending append. The known range covers
    /// the suffix plus the entry just before it, so the decision can
    /// verify the expected head against stored digests.
    pub fn check_append(&self, request: EntrySuffix) -> Result<CheckedAppend> {
        let range = request.entries().first().map(|first| {
            let first_sequence = first.sequence.clone().into_u64();
            let last_sequence = request
                .entries()
                .last()
                .map(|entry| entry.sequence.clone().into_u64())
                .unwrap_or(first_sequence);
            (first_sequence.saturating_sub(1), last_sequence)
        });
        let ledger = self.load_ledger(&request.store, range)?;
        Ok(CheckedAppend { request, ledger })
    }

    /// Look up ledger state for a pending checkpoint publication.
    pub fn check_checkpoint(&self, artifact: CheckpointArtifact) -> Result<CheckedCheckpoint> {
        let ledger = self.load_ledger(&artifact.store, None)?;
        Ok(CheckedCheckpoint { artifact, ledger })
    }

    /// Look up ledger state for a routed-object notice. The known range
    /// is the announced head's exact sequence: accepting the notice means
    /// this mirror already holds that content-addressed head, not merely
    /// a higher sequence number with an unknown ancestry.
    pub fn check_object_notice(&self, notice: ObjectNotice) -> Result<CheckedObjectNotice> {
        let sequence = notice.head.sequence.clone().into_u64();
        let ledger = self.load_ledger(&notice.store, Some((sequence, sequence)))?;
        Ok(CheckedObjectNotice { notice, ledger })
    }

    /// Persist a validated novel suffix: every entry row in one commit,
    /// then the advanced head row. Both transactions are durable at
    /// return; the ack is sent only after both committed. A crash
    /// between the two leaves entry rows ahead of the head row; the
    /// shipper's idempotent re-send dedups against the loaded known
    /// rows and arrives here with an EMPTY entry remainder, so the
    /// entry transaction is skipped (sema-engine rejects empty commits)
    /// and only the head re-advances (see ARCHITECTURE.md).
    pub fn persist_suffix(&mut self, suffix: &NovelSuffix) -> Result<AppendReceipt> {
        if !suffix.entries().is_empty() {
            self.commit_entry_rows(suffix)?;
        }
        self.advance_head(suffix)
    }

    /// The first of the two suffix transactions: every novel entry row
    /// in one commit. Public as the crash-window seam — the witness
    /// proving the window self-heals commits the rows without the head
    /// advance, exactly the state a crash between the transactions
    /// leaves behind.
    pub fn commit_entry_rows(&mut self, suffix: &NovelSuffix) -> Result<()> {
        let mut commit = CommitRequest::new(self.entries);
        for envelope in suffix.entries() {
            commit = commit.assert(ReceivedEntry::from_envelope(&suffix.store, envelope));
        }
        self.engine.commit(commit)?;
        Ok(())
    }

    /// The second of the two suffix transactions: mutate the head row
    /// to the suffix's decided head.
    pub fn advance_head(&mut self, suffix: &NovelSuffix) -> Result<AppendReceipt> {
        self.engine.mutate(Mutation::new(
            self.heads,
            StoredHead::new(
                suffix.store.as_str().to_owned(),
                Some(HeadStamp::from_mark(&suffix.head)),
            ),
        ))?;
        Ok(AppendReceipt {
            store: suffix.store.clone(),
            head: suffix.head.clone(),
        })
    }

    /// Persist a checkpoint artifact row.
    pub fn persist_checkpoint(
        &mut self,
        artifact: &CheckpointArtifact,
    ) -> Result<CheckpointReceipt> {
        let row = StoredCheckpoint::from_artifact(artifact);
        let receipt = row.to_receipt();
        self.engine.assert(Assertion::new(self.checkpoints, row))?;
        Ok(receipt)
    }

    /// Whether a store name fits the key scheme: entry and checkpoint
    /// keys are `<store>/<sequence>`, so a name carrying the separator
    /// would collide with the ordering suffix. Registration refuses
    /// such names with a typed rejection.
    pub fn name_is_keyable(store: &StoreName) -> bool {
        !store.as_str().contains(KEY_SEPARATOR)
    }

    /// Register a store name. A virgin name starts with no head. A name
    /// with surviving history rows (retired earlier — retirement keeps
    /// the received entries until retention enforcement lands) RESUMES:
    /// the head restores from the highest surviving entry row, so a
    /// shipper continues the chain instead of faulting on re-asserted
    /// rows. Registering an already-registered store is reported by the
    /// caller through the registry check, not here.
    pub fn register_store(
        &mut self,
        store: &StoreName,
        addressing: ContentAddressing,
    ) -> Result<()> {
        let surviving = self.entry_rows(KeyRange::between(
            Self::sequence_key(store, 0),
            Self::sequence_key(store, u64::MAX),
        ))?;
        self.engine.assert(Assertion::new(
            self.heads,
            StoredHead::new(
                store.as_str().to_owned(),
                surviving.last().map(ReceivedEntry::to_head_stamp),
            ),
        ))?;
        // Upsert the policy: a virgin store asserts a fresh row; a
        // re-registration after retire (which keeps the surviving policy
        // row) mutates it to the newly chosen addressing. The head above
        // is always a fresh assert because retire retracts it.
        self.upsert_policy(StorePolicy {
            store: store.as_str().to_owned(),
            addressing,
        })?;
        Ok(())
    }

    /// Assert a policy row when none exists, otherwise mutate the
    /// surviving one — the same idempotent shape `persist_retention`
    /// uses, so a re-registration overwrites the prior policy.
    fn upsert_policy(&mut self, row: StorePolicy) -> Result<()> {
        let key = row.record_key();
        let existing = self
            .engine
            .match_records(QueryPlan::key(self.policies, key))?;
        if existing.records().is_empty() {
            self.engine.assert(Assertion::new(self.policies, row))?;
        } else {
            self.engine.mutate(Mutation::new(self.policies, row))?;
        }
        Ok(())
    }

    /// Retire a store name: retract the head row. Received entries and
    /// checkpoint artifacts remain until retention enforcement lands
    /// (deferred by decision); re-registration resumes from them.
    pub fn retire_store(&mut self, store: &StoreName) -> Result<()> {
        self.engine.retract(Retraction::new(
            self.heads,
            RecordKey::new(store.as_str().to_owned()),
        ))?;
        Ok(())
    }

    /// Persist a retention setting (typed placeholder — stored, not
    /// enforced; deferred by decision).
    pub fn persist_retention(&mut self, order: &meta_signal_mirror::RetentionOrder) -> Result<()> {
        let row = RetentionSetting::from_order(order);
        let key = row.record_key();
        let existing = self
            .engine
            .match_records(QueryPlan::key(self.retention, key))?;
        if existing.records().is_empty() {
            self.engine.assert(Assertion::new(self.retention, row))?;
        } else {
            self.engine.mutate(Mutation::new(self.retention, row))?;
        }
        Ok(())
    }

    /// The restore bundle for one store: the latest checkpoint artifact
    /// plus every received entry past its covered end.
    pub fn load_restore(
        &self,
        query: &RestoreQuery,
    ) -> Result<std::result::Result<RestoreBundle, RestoreRejection>> {
        let store = query.payload();
        if self.head_row(store)?.is_none() {
            return Ok(Err(RestoreRejection {
                store: store.clone(),
                reason: RestoreRejectionReason::UnknownStore,
            }));
        }
        let Some(checkpoint_row) = self.latest_checkpoint_row(store)? else {
            return Ok(Err(RestoreRejection {
                store: store.clone(),
                reason: RestoreRejectionReason::NoCheckpoint,
            }));
        };
        let suffix: Vec<EntryEnvelope> = self
            .entry_rows(KeyRange::between(
                Self::sequence_key(store, checkpoint_row.covered_end + 1),
                Self::sequence_key(store, u64::MAX),
            ))?
            .iter()
            .map(ReceivedEntry::to_envelope)
            .collect();
        Ok(Ok(RestoreBundle::from_suffix(
            store.clone(),
            checkpoint_row.to_artifact(),
            suffix,
        )))
    }

    /// Every entry this mirror durably holds for one store, oldest first,
    /// reconstructed as the wire envelope it landed as — sequence, previous
    /// digest, carried digest, and the FULL landed payload. The read sibling
    /// of `persist_suffix`: where `load_heads` surfaces only the head digest,
    /// this surfaces the exact body bytes the mirror committed, so a caller can
    /// re-derive an entry's content address from what actually landed rather
    /// than trusting the head it was told. Unlike `load_restore`, it needs no
    /// checkpoint and returns the whole chain.
    pub fn landed_entries(&self, store: &StoreName) -> Result<Vec<EntryEnvelope>> {
        Ok(self
            .entry_rows(KeyRange::between(
                Self::sequence_key(store, 0),
                Self::sequence_key(store, u64::MAX),
            ))?
            .iter()
            .map(ReceivedEntry::to_envelope)
            .collect())
    }

    /// Observe store heads: one store or every registered store.
    pub fn load_heads(&self, query: &HeadQuery) -> Result<HeadListing> {
        let rows = match query.payload() {
            Some(store) => self.head_row(store)?.into_iter().collect(),
            None => self
                .engine
                .match_records(QueryPlan::all(self.heads))?
                .records()
                .to_vec(),
        };
        Ok(HeadListing::from_heads(
            rows.iter()
                .map(|row| {
                    StoreHead::new(
                        StoreName::new(row.store.clone()),
                        row.head().map(HeadStamp::to_mark),
                    )
                })
                .collect(),
        ))
    }

    /// Observe the registered store names.
    pub fn load_registry(&self) -> Result<meta_signal_mirror::RegistryListing> {
        let rows = self.engine.match_records(QueryPlan::all(self.heads))?;
        Ok(meta_signal_mirror::RegistryListing::new(
            rows.records()
                .iter()
                .map(|row| {
                    meta_signal_mirror::RegisteredStore::new(meta_signal_mirror::StoreName::new(
                        row.store.clone(),
                    ))
                })
                .collect(),
        ))
    }

    /// The mirror's own versioned engine — exposed for inspection in
    /// witnesses (the dogfooding proof reads the mirror's own versioned
    /// commit log).
    pub fn engine(&self) -> &Engine {
        &self.engine
    }
}
