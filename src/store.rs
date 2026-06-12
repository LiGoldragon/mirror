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
    PayloadBytes, RestoreBundle, RestoreQuery, RestoreRejection, RestoreRejectionReason, StoreHead,
    StoreName,
};

use crate::error::Result;
use crate::schema::sema::{
    Bytes, CheckedAppend, CheckedCheckpoint, DigestBytes, HeadStamp, KnownEntry, NovelSuffix,
    ReceivedEntry, RecordFamily, RegisteredLedger, RetentionRule, RetentionSetting, StoreLedger,
    StoredCheckpoint, StoredHead,
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
        match &self.scope {
            Some(store) => RecordKey::new(format!("store{KEY_SEPARATOR}{store}")),
            None => RecordKey::new("all-stores".to_owned()),
        }
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
            previous_digest: envelope
                .previous_digest
                .as_ref()
                .map(DigestBytes::from_entry_digest),
            digest: DigestBytes::from_entry_digest(&envelope.digest),
            payload: Bytes::new(envelope.payload.as_slice().to_vec()),
        }
    }

    pub fn to_envelope(&self) -> EntryEnvelope {
        EntryEnvelope {
            sequence: CommitSequence::new(self.sequence),
            previous_digest: self
                .previous_digest
                .as_ref()
                .map(DigestBytes::to_entry_digest),
            digest: self.digest.to_entry_digest(),
            payload: PayloadBytes::new(signal_mirror::Bytes::new(self.payload.payload().to_vec())),
        }
    }

    pub fn to_known_entry(&self) -> KnownEntry {
        KnownEntry {
            sequence: CommitSequence::new(self.sequence),
            digest: self.digest.to_entry_digest(),
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
            scope: match &order.scope {
                meta_signal_mirror::RetentionScope::Store(store) => Some(store.as_str().to_owned()),
                meta_signal_mirror::RetentionScope::AllStores => None,
            },
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
        Ok(Self {
            engine,
            heads,
            entries,
            checkpoints,
            retention,
        })
    }

    fn head_row(&self, store: &StoreName) -> Result<Option<StoredHead>> {
        let snapshot = self.engine.match_records(QueryPlan::key(
            self.heads,
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
        Ok(StoreLedger::Registered(RegisteredLedger {
            head: head_row.head.as_ref().map(HeadStamp::to_mark),
            known,
            latest_checkpoint: self
                .latest_checkpoint_row(store)?
                .as_ref()
                .map(StoredCheckpoint::to_receipt),
        }))
    }

    /// Look up ledger state for a pending append. The known range covers
    /// the suffix plus the entry just before it, so the decision can
    /// verify the expected head against stored digests.
    pub fn check_append(&self, request: EntrySuffix) -> Result<CheckedAppend> {
        let range = request.entries.first().map(|first| {
            let first_sequence = first.sequence.clone().into_u64();
            let last_sequence = request
                .entries
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

    /// Persist a validated novel suffix: every entry row in one commit,
    /// then the advanced head row. Both transactions are durable at
    /// return; the ack is sent only after both committed. A crash
    /// between the two leaves entry rows ahead of the head row — the
    /// shipper's idempotent re-send dedups the rows and re-advances the
    /// head (see ARCHITECTURE.md).
    pub fn persist_suffix(&mut self, suffix: &NovelSuffix) -> Result<AppendReceipt> {
        let mut commit = CommitRequest::new(self.entries);
        for envelope in &suffix.entries {
            commit = commit.assert(ReceivedEntry::from_envelope(&suffix.store, envelope));
        }
        self.engine.commit(commit)?;
        self.engine.mutate(Mutation::new(
            self.heads,
            StoredHead {
                store: suffix.store.as_str().to_owned(),
                head: Some(HeadStamp::from_mark(&suffix.head)),
            },
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

    /// Register a store name: the head row exists with no head yet.
    /// Registering an already-registered store is reported by the caller
    /// through the ledger check, not here.
    pub fn register_store(&mut self, store: &StoreName) -> Result<()> {
        self.engine.assert(Assertion::new(
            self.heads,
            StoredHead {
                store: store.as_str().to_owned(),
                head: None,
            },
        ))?;
        Ok(())
    }

    /// Retire a store name: retract the head row. Received entries and
    /// checkpoint artifacts remain until retention enforcement lands
    /// (deferred by decision).
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
        let suffix = self
            .entry_rows(KeyRange::between(
                Self::sequence_key(store, checkpoint_row.covered_end + 1),
                Self::sequence_key(store, u64::MAX),
            ))?
            .iter()
            .map(ReceivedEntry::to_envelope)
            .collect();
        Ok(Ok(RestoreBundle {
            store: store.clone(),
            checkpoint: checkpoint_row.to_artifact(),
            suffix,
        }))
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
        Ok(HeadListing::new(
            rows.iter()
                .map(|row| StoreHead {
                    store: StoreName::new(row.store.clone()),
                    head: row.head.as_ref().map(HeadStamp::to_mark),
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

    /// Whether a store name is registered (has a head row).
    pub fn is_registered(&self, store: &StoreName) -> Result<bool> {
        Ok(self.head_row(store)?.is_some())
    }

    /// The mirror's own versioned engine — exposed for inspection in
    /// witnesses (the dogfooding proof reads the mirror's own versioned
    /// commit log).
    pub fn engine(&self) -> &Engine {
        &self.engine
    }
}
