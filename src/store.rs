//! Mirror's durable payload-blind ledger.
//!
//! Every mutating method returns only after sema-engine commits. The engine
//! therefore cannot emit an acknowledgement before durable state exists.

use std::path::Path;

use sema_engine::{
    Assertion, CommitRequest, Engine, EngineOpen, EngineRecord, KeyRange, Mutation, QueryPlan,
    RecordKey, Retraction, SchemaVersion, TableReference,
};
use signal_mirror::{
    AppendReceipt, ArtifactBytes, CheckpointArtifact, CheckpointReceipt, CheckpointSequence,
    CommitSequence, EntryEnvelope, EntrySuffix, HeadListing, HeadMark, HeadQuery, ObjectNotice,
    PayloadBytes, RestoreBundle, RestoreQuery, RestoreRejection, RestoreRejectionReason, StoreHead,
    StoreName,
};
use signal_standard::ObjectDigest;

use crate::error::Result;
use crate::ledger::{
    CheckedAppend, CheckedCheckpoint, CheckedObjectNotice, ContentAddressing, HeadStamp,
    KnownEntry, NovelSuffix, ReceivedEntry, RecordFamily, RegisteredLedger, RetentionRule,
    RetentionSetting, StoreLedger, StorePolicy, StoredCheckpoint, StoredHead,
};

const KEY_SEPARATOR: char = '/';
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
            self.checkpoint_sequence,
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

impl EngineRecord for StorePolicy {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.store.clone())
    }
}

impl HeadStamp {
    fn from_mark(mark: &HeadMark) -> Self {
        Self {
            sequence: u64::try_from(mark.commit_sequence).expect("nonnegative mark sequence"),
            digest: mark.object_digest.as_str().to_owned(),
        }
    }

    fn to_mark(&self) -> HeadMark {
        HeadMark {
            commit_sequence: i64::try_from(self.sequence)
                .expect("stored sequence fits wire integer"),
            object_digest: self.digest.clone(),
        }
    }
}

impl ReceivedEntry {
    fn from_envelope(store: &StoreName, envelope: &EntryEnvelope) -> Result<Self> {
        Ok(Self {
            store: store.clone(),
            sequence: u64::try_from(envelope.commit_sequence).expect("nonnegative entry sequence"),
            previous_digest: envelope
                .object_digest_option
                .as_ref()
                .map(|digest| digest.as_str().to_owned()),
            digest: envelope.object_digest.as_str().to_owned(),
            payload: envelope
                .payload_bytes
                .iter()
                .map(|value| u8::try_from(*value))
                .collect::<std::result::Result<Vec<_>, _>>()?,
        })
    }

    fn to_envelope(&self) -> EntryEnvelope {
        EntryEnvelope {
            commit_sequence: i64::try_from(self.sequence)
                .expect("stored sequence fits wire integer"),
            object_digest_option: self.previous_digest.clone(),
            object_digest: self.digest.clone(),
            payload_bytes: self.payload.iter().map(|value| i64::from(*value)).collect(),
        }
    }

    fn to_known_entry(&self) -> KnownEntry {
        KnownEntry {
            sequence: self.sequence,
            digest: self.digest.clone(),
        }
    }

    fn to_head_stamp(&self) -> HeadStamp {
        HeadStamp {
            sequence: self.sequence,
            digest: self.digest.clone(),
        }
    }
}

impl StoredCheckpoint {
    fn from_artifact(artifact: &CheckpointArtifact) -> Result<Self> {
        Ok(Self {
            store: artifact.store_name.clone(),
            checkpoint_sequence: u64::try_from(artifact.checkpoint_sequence)
                .expect("nonnegative checkpoint sequence"),
            covered_end_sequence: u64::try_from(artifact.commit_sequence)
                .expect("nonnegative commit sequence"),
            digest: artifact.object_digest.as_str().to_owned(),
            artifact: artifact
                .artifact_bytes
                .iter()
                .map(|value| u8::try_from(*value))
                .collect::<std::result::Result<Vec<_>, _>>()?,
        })
    }

    fn to_artifact(&self) -> CheckpointArtifact {
        CheckpointArtifact {
            store_name: self.store.clone(),
            checkpoint_sequence: i64::try_from(self.checkpoint_sequence)
                .expect("stored checkpoint sequence fits wire integer"),
            commit_sequence: i64::try_from(self.covered_end_sequence)
                .expect("stored commit sequence fits wire integer"),
            object_digest: self.digest.clone(),
            artifact_bytes: self
                .artifact
                .iter()
                .map(|value| i64::from(*value))
                .collect(),
        }
    }

    fn to_receipt(&self) -> CheckpointReceipt {
        CheckpointReceipt {
            store_name: self.store.clone(),
            checkpoint_sequence: i64::try_from(self.checkpoint_sequence)
                .expect("stored checkpoint sequence fits wire integer"),
            commit_sequence: i64::try_from(self.covered_end_sequence)
                .expect("stored commit sequence fits wire integer"),
        }
    }
}

impl RetentionSetting {
    fn from_order(order: &meta_signal_mirror::RetentionOrder) -> Self {
        Self {
            scope: match &order.retention_scope {
                meta_signal_mirror::RetentionScope::Store(store) => Some(store.clone()),
                meta_signal_mirror::RetentionScope::AllStores => None,
            },
            rule: match &order.retention_rule {
                meta_signal_mirror::RetentionRule::KeepEverything => RetentionRule::KeepEverything,
                meta_signal_mirror::RetentionRule::KeepLatestCheckpoints(count) => {
                    RetentionRule::KeepLatestCheckpoints(
                        u64::try_from(*count).expect("validated retention count"),
                    )
                }
            },
        }
    }
}

pub struct Store {
    engine: Engine,
    heads: TableReference<StoredHead>,
    entries: TableReference<ReceivedEntry>,
    checkpoints: TableReference<StoredCheckpoint>,
    retention: TableReference<RetentionSetting>,
    policies: TableReference<StorePolicy>,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self> {
        let mut engine = Engine::open(
            EngineOpen::new(path, SchemaVersion::new(2))
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
        let snapshot = self
            .engine
            .match_records(QueryPlan::key(self.heads, RecordKey::new(store.clone())))?;
        Ok(snapshot.records().first().cloned())
    }

    fn policy_row(&self, store: &StoreName) -> Result<Option<StorePolicy>> {
        let snapshot = self
            .engine
            .match_records(QueryPlan::key(self.policies, RecordKey::new(store.clone())))?;
        Ok(snapshot.records().first().cloned())
    }

    fn entry_rows(&self, range: KeyRange) -> Result<Vec<ReceivedEntry>> {
        Ok(self
            .engine
            .match_records(QueryPlan::key_range(self.entries, range))?
            .records()
            .to_vec())
    }

    fn sequence_key(store: &StoreName, sequence: u64) -> RecordKey {
        RecordKey::new(format!(
            "{}{}{:0width$}",
            store,
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
        Ok(self
            .engine
            .match_records(QueryPlan::key_range(self.checkpoints, range))?
            .records()
            .last()
            .cloned())
    }

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
            head_row.head.as_ref().map(HeadStamp::to_mark),
            known,
            self.latest_checkpoint_row(store)?
                .as_ref()
                .map(StoredCheckpoint::to_artifact),
            addressing,
        )))
    }

    pub fn check_append(&self, request: EntrySuffix) -> Result<CheckedAppend> {
        let range = request.entry_envelope_vector.first().map(|first| {
            let first_sequence =
                u64::try_from(first.commit_sequence).expect("nonnegative first sequence");
            let last_sequence = request
                .entry_envelope_vector
                .last()
                .map_or(first_sequence, |entry| {
                    u64::try_from(entry.commit_sequence).expect("nonnegative entry sequence")
                });
            (first_sequence.saturating_sub(1), last_sequence)
        });
        let ledger = self.load_ledger(&request.store_name, range)?;
        Ok(CheckedAppend {
            entry_suffix: request,
            store_ledger: ledger,
        })
    }

    pub fn check_checkpoint(&self, artifact: CheckpointArtifact) -> Result<CheckedCheckpoint> {
        let ledger = self.load_ledger(&artifact.store_name, None)?;
        Ok(CheckedCheckpoint {
            checkpoint_artifact: artifact,
            store_ledger: ledger,
        })
    }

    pub fn check_object_notice(&self, notice: ObjectNotice) -> Result<CheckedObjectNotice> {
        let sequence =
            u64::try_from(notice.head_mark.commit_sequence).expect("nonnegative notice sequence");
        let ledger = self.load_ledger(&notice.store_name, Some((sequence, sequence)))?;
        Ok(CheckedObjectNotice {
            object_notice: notice,
            store_ledger: ledger,
        })
    }

    pub fn persist_suffix(&mut self, suffix: &NovelSuffix) -> Result<AppendReceipt> {
        if !suffix.entries().is_empty() {
            self.commit_entry_rows(suffix)?;
        }
        self.advance_head(suffix)
    }

    pub fn commit_entry_rows(&mut self, suffix: &NovelSuffix) -> Result<()> {
        let mut commit = CommitRequest::new(self.entries);
        for envelope in suffix.entries() {
            commit = commit.assert(ReceivedEntry::from_envelope(&suffix.store_name, envelope)?);
        }
        self.engine.commit(commit)?;
        Ok(())
    }

    pub fn advance_head(&mut self, suffix: &NovelSuffix) -> Result<AppendReceipt> {
        self.engine.mutate(Mutation::new(
            self.heads,
            StoredHead {
                store: suffix.store_name.clone(),
                head: Some(HeadStamp::from_mark(&suffix.head_mark)),
            },
        ))?;
        Ok(AppendReceipt {
            store_name: suffix.store_name.clone(),
            head_mark: suffix.head_mark.clone(),
        })
    }

    pub fn persist_checkpoint(
        &mut self,
        artifact: &CheckpointArtifact,
    ) -> Result<CheckpointReceipt> {
        let row = StoredCheckpoint::from_artifact(artifact)?;
        let receipt = row.to_receipt();
        self.engine.assert(Assertion::new(self.checkpoints, row))?;
        Ok(receipt)
    }

    pub fn name_is_keyable(store: &StoreName) -> bool {
        !store.contains(KEY_SEPARATOR)
    }

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
            StoredHead {
                store: store.clone(),
                head: surviving.last().map(ReceivedEntry::to_head_stamp),
            },
        ))?;
        self.upsert_policy(StorePolicy {
            store: store.clone(),
            addressing,
        })
    }

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

    pub fn retire_store(&mut self, store: &StoreName) -> Result<()> {
        self.engine
            .retract(Retraction::new(self.heads, RecordKey::new(store.clone())))?;
        Ok(())
    }

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

    pub fn load_restore(
        &self,
        query: &RestoreQuery,
    ) -> Result<std::result::Result<RestoreBundle, RestoreRejection>> {
        let store = query;
        if self.head_row(store)?.is_none() {
            return Ok(Err(RestoreRejection {
                store_name: store.clone(),
                restore_rejection_reason: RestoreRejectionReason::NoCheckpoint,
            }));
        }
        let Some(checkpoint) = self.latest_checkpoint_row(store)? else {
            return Ok(Err(RestoreRejection {
                store_name: store.clone(),
                restore_rejection_reason: RestoreRejectionReason::UnknownStore,
            }));
        };
        let suffix = self
            .entry_rows(KeyRange::between(
                Self::sequence_key(store, checkpoint.covered_end_sequence + 1),
                Self::sequence_key(store, u64::MAX),
            ))?
            .iter()
            .map(ReceivedEntry::to_envelope)
            .collect();
        Ok(Ok(RestoreBundle {
            store_name: store.clone(),
            checkpoint_artifact: checkpoint.to_artifact(),
            entry_envelope_vector: suffix,
        }))
    }

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

    pub fn load_heads(&self, query: &HeadQuery) -> Result<HeadListing> {
        let rows = match query {
            Some(store) => self.head_row(store)?.into_iter().collect(),
            None => self
                .engine
                .match_records(QueryPlan::all(self.heads))?
                .records()
                .to_vec(),
        };
        Ok(HeadListing {
            store_head_vector: rows
                .iter()
                .map(|row| StoreHead {
                    store_name: row.store.clone(),
                    head_mark_option: row.head.as_ref().map(HeadStamp::to_mark),
                })
                .collect(),
        })
    }

    pub fn load_registry(&self) -> Result<meta_signal_mirror::RegistryListing> {
        let rows = self.engine.match_records(QueryPlan::all(self.heads))?;
        Ok(meta_signal_mirror::RegistryListing {
            registered_store_vector: rows.records().iter().map(|row| row.store.clone()).collect(),
        })
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }
}
