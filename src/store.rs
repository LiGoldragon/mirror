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
    z2VLxP, z2VPgu, z2VPuU, z2VSAK, z2VTXE, z2VTq5, z2VUKn, z2VUwg, z2VUxk, z2VY7x, z2VYSu, z2VZWt,
    z2VaxY, z2VbBN, z2Vbm6, z2VbvA, z2VcqM, z2Vdqa, z2Ve8p,
};
use signal_standard::z2VSyM;

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
    fn from_mark(mark: &z2VcqM) -> Self {
        Self {
            sequence: *mark.field_0.payload(),
            digest: mark.field_1.as_str().to_owned(),
        }
    }

    fn to_mark(&self) -> z2VcqM {
        z2VcqM {
            field_0: z2VSAK::new(self.sequence),
            field_1: z2VSyM::new(self.digest.clone()),
        }
    }
}

impl ReceivedEntry {
    fn from_envelope(store: &z2Ve8p, envelope: &z2VPuU) -> Result<Self> {
        Ok(Self {
            store: store.payload().clone(),
            sequence: *envelope.field_0.payload(),
            previous_digest: envelope
                .field_1
                .as_ref()
                .map(|digest| digest.as_str().to_owned()),
            digest: envelope.field_2.as_str().to_owned(),
            payload: envelope.field_3.octets()?,
        })
    }

    fn to_envelope(&self) -> z2VPuU {
        z2VPuU {
            field_0: z2VSAK::new(self.sequence),
            field_1: self.previous_digest.clone().map(z2VSyM::new),
            field_2: z2VSyM::new(self.digest.clone()),
            field_3: z2VUwg::from_octets(&self.payload),
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
    fn from_artifact(artifact: &z2VTXE) -> Result<Self> {
        Ok(Self {
            store: artifact.field_0.payload().clone(),
            checkpoint_sequence: *artifact.field_1.payload(),
            covered_end_sequence: *artifact.field_2.payload(),
            digest: artifact.field_3.as_str().to_owned(),
            artifact: artifact.field_4.octets()?,
        })
    }

    fn to_artifact(&self) -> z2VTXE {
        z2VTXE {
            field_0: z2Ve8p::new(self.store.clone()),
            field_1: z2VUKn::new(self.checkpoint_sequence),
            field_2: z2VSAK::new(self.covered_end_sequence),
            field_3: z2VSyM::new(self.digest.clone()),
            field_4: z2VUxk::from_octets(&self.artifact),
        }
    }

    fn to_receipt(&self) -> z2VLxP {
        z2VLxP {
            field_0: z2Ve8p::new(self.store.clone()),
            field_1: z2VUKn::new(self.checkpoint_sequence),
            field_2: z2VSAK::new(self.covered_end_sequence),
        }
    }
}

impl RetentionSetting {
    fn from_order(order: &meta_signal_mirror::z2VXLU) -> Self {
        Self {
            scope: match &order.field_0 {
                meta_signal_mirror::z2VTJB::z2VXE8(store) => Some(store.payload().clone()),
                meta_signal_mirror::z2VTJB::z2VcDy => None,
            },
            rule: match &order.field_1 {
                meta_signal_mirror::z2VWXC::z2VaQ8 => RetentionRule::KeepEverything,
                meta_signal_mirror::z2VWXC::z2VXbW(count) => {
                    RetentionRule::KeepLatestCheckpoints(*count.payload())
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

    fn head_row(&self, store: &z2Ve8p) -> Result<Option<StoredHead>> {
        let snapshot = self.engine.match_records(QueryPlan::key(
            self.heads,
            RecordKey::new(store.payload().clone()),
        ))?;
        Ok(snapshot.records().first().cloned())
    }

    fn policy_row(&self, store: &z2Ve8p) -> Result<Option<StorePolicy>> {
        let snapshot = self.engine.match_records(QueryPlan::key(
            self.policies,
            RecordKey::new(store.payload().clone()),
        ))?;
        Ok(snapshot.records().first().cloned())
    }

    fn entry_rows(&self, range: KeyRange) -> Result<Vec<ReceivedEntry>> {
        Ok(self
            .engine
            .match_records(QueryPlan::key_range(self.entries, range))?
            .records()
            .to_vec())
    }

    fn sequence_key(store: &z2Ve8p, sequence: u64) -> RecordKey {
        RecordKey::new(format!(
            "{}{}{:0width$}",
            store.payload(),
            KEY_SEPARATOR,
            sequence,
            width = SEQUENCE_KEY_WIDTH
        ))
    }

    fn latest_checkpoint_row(&self, store: &z2Ve8p) -> Result<Option<StoredCheckpoint>> {
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
        store: &z2Ve8p,
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
                .map(StoredCheckpoint::to_receipt),
            addressing,
        )))
    }

    pub fn check_append(&self, request: z2VTq5) -> Result<CheckedAppend> {
        let range = request.field_2.first().map(|first| {
            let first_sequence = *first.field_0.payload();
            let last_sequence = request
                .field_2
                .last()
                .map_or(first_sequence, |entry| *entry.field_0.payload());
            (first_sequence.saturating_sub(1), last_sequence)
        });
        let ledger = self.load_ledger(&request.field_0, range)?;
        Ok(CheckedAppend {
            entry_suffix: request,
            store_ledger: ledger,
        })
    }

    pub fn check_checkpoint(&self, artifact: z2VTXE) -> Result<CheckedCheckpoint> {
        let ledger = self.load_ledger(&artifact.field_0, None)?;
        Ok(CheckedCheckpoint {
            checkpoint_artifact: artifact,
            store_ledger: ledger,
        })
    }

    pub fn check_object_notice(&self, notice: z2VZWt) -> Result<CheckedObjectNotice> {
        let sequence = *notice.field_1.field_0.payload();
        let ledger = self.load_ledger(&notice.field_0, Some((sequence, sequence)))?;
        Ok(CheckedObjectNotice {
            object_notice: notice,
            store_ledger: ledger,
        })
    }

    pub fn persist_suffix(&mut self, suffix: &NovelSuffix) -> Result<z2VaxY> {
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

    pub fn advance_head(&mut self, suffix: &NovelSuffix) -> Result<z2VaxY> {
        self.engine.mutate(Mutation::new(
            self.heads,
            StoredHead {
                store: suffix.store_name.payload().clone(),
                head: Some(HeadStamp::from_mark(&suffix.head_mark)),
            },
        ))?;
        Ok(z2VaxY {
            field_0: suffix.store_name.clone(),
            field_1: suffix.head_mark.clone(),
        })
    }

    pub fn persist_checkpoint(&mut self, artifact: &z2VTXE) -> Result<z2VLxP> {
        let row = StoredCheckpoint::from_artifact(artifact)?;
        let receipt = row.to_receipt();
        self.engine.assert(Assertion::new(self.checkpoints, row))?;
        Ok(receipt)
    }

    pub fn name_is_keyable(store: &z2Ve8p) -> bool {
        !store.payload().contains(KEY_SEPARATOR)
    }

    pub fn register_store(&mut self, store: &z2Ve8p, addressing: ContentAddressing) -> Result<()> {
        let surviving = self.entry_rows(KeyRange::between(
            Self::sequence_key(store, 0),
            Self::sequence_key(store, u64::MAX),
        ))?;
        self.engine.assert(Assertion::new(
            self.heads,
            StoredHead {
                store: store.payload().clone(),
                head: surviving.last().map(ReceivedEntry::to_head_stamp),
            },
        ))?;
        self.upsert_policy(StorePolicy {
            store: store.payload().clone(),
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

    pub fn retire_store(&mut self, store: &z2Ve8p) -> Result<()> {
        self.engine.retract(Retraction::new(
            self.heads,
            RecordKey::new(store.payload().clone()),
        ))?;
        Ok(())
    }

    pub fn persist_retention(&mut self, order: &meta_signal_mirror::z2VXLU) -> Result<()> {
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

    pub fn load_restore(&self, query: &z2VbvA) -> Result<std::result::Result<z2VYSu, z2VbBN>> {
        let store = query.payload();
        if self.head_row(store)?.is_none() {
            return Ok(Err(z2VbBN {
                field_0: store.clone(),
                field_1: z2VPgu::z2Vf1c,
            }));
        }
        let Some(checkpoint) = self.latest_checkpoint_row(store)? else {
            return Ok(Err(z2VbBN {
                field_0: store.clone(),
                field_1: z2VPgu::z2VXM2,
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
        Ok(Ok(z2VYSu {
            field_0: store.clone(),
            field_1: checkpoint.to_artifact(),
            field_2: suffix,
        }))
    }

    pub fn landed_entries(&self, store: &z2Ve8p) -> Result<Vec<z2VPuU>> {
        Ok(self
            .entry_rows(KeyRange::between(
                Self::sequence_key(store, 0),
                Self::sequence_key(store, u64::MAX),
            ))?
            .iter()
            .map(ReceivedEntry::to_envelope)
            .collect())
    }

    pub fn load_heads(&self, query: &z2Vdqa) -> Result<z2VY7x> {
        let rows = match query.payload() {
            Some(store) => self.head_row(store)?.into_iter().collect(),
            None => self
                .engine
                .match_records(QueryPlan::all(self.heads))?
                .records()
                .to_vec(),
        };
        Ok(z2VY7x {
            field_0: rows
                .iter()
                .map(|row| z2Vbm6 {
                    field_0: z2Ve8p::new(row.store.clone()),
                    field_1: row.head.as_ref().map(HeadStamp::to_mark),
                })
                .collect(),
        })
    }

    pub fn load_registry(&self) -> Result<meta_signal_mirror::z2VQWA> {
        let rows = self.engine.match_records(QueryPlan::all(self.heads))?;
        Ok(meta_signal_mirror::z2VQWA::new(
            rows.records()
                .iter()
                .map(|row| meta_signal_mirror::z2VT8Z::new(z2Ve8p::new(row.store.clone())))
                .collect(),
        ))
    }

    pub fn engine(&self) -> &Engine {
        &self.engine
    }
}
