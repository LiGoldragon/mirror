//! Mirror's durable, payload-blind ledger model.
//!
//! The public Ethos contracts own the wire vocabulary. These types own only
//! Mirror's private persistence and decision state; they never duplicate the
//! external Interface under readable public aliases.

use rkyv::{Archive, Deserialize, Serialize};
use signal_mirror::{z2VLxP, z2VPuU, z2VTXE, z2VTq5, z2VZWt, z2VcqM, z2Ve8p};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NovelSuffix {
    pub store_name: z2Ve8p,
    pub head_mark: z2VcqM,
    entries: Vec<z2VPuU>,
}

impl NovelSuffix {
    pub fn new(store_name: z2Ve8p, head_mark: z2VcqM, entries: Vec<z2VPuU>) -> Self {
        Self {
            store_name,
            head_mark,
            entries,
        }
    }

    pub fn entries(&self) -> &[z2VPuU] {
        &self.entries
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KnownEntry {
    pub sequence: u64,
    pub digest: String,
}

#[derive(Archive, Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContentAddressing {
    Opaque,
    SemaVersionedLog,
}

impl ContentAddressing {
    pub fn from_meta(addressing: &meta_signal_mirror::z2VMYP) -> Self {
        match addressing {
            meta_signal_mirror::z2VMYP::z2Vf8Y => Self::Opaque,
            meta_signal_mirror::z2VMYP::z2VbgN => Self::SemaVersionedLog,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredLedger {
    head: Option<z2VcqM>,
    known: Vec<KnownEntry>,
    latest_checkpoint: Option<z2VLxP>,
    addressing: ContentAddressing,
}

impl RegisteredLedger {
    pub fn new(
        head: Option<z2VcqM>,
        known: Vec<KnownEntry>,
        latest_checkpoint: Option<z2VLxP>,
        addressing: ContentAddressing,
    ) -> Self {
        Self {
            head,
            known,
            latest_checkpoint,
            addressing,
        }
    }

    pub fn head(&self) -> Option<&z2VcqM> {
        self.head.as_ref()
    }

    pub fn known(&self) -> &[KnownEntry] {
        &self.known
    }

    pub fn latest_checkpoint(&self) -> Option<&z2VLxP> {
        self.latest_checkpoint.as_ref()
    }

    pub fn addressing(&self) -> ContentAddressing {
        self.addressing
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreLedger {
    Registered(RegisteredLedger),
    Unregistered,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckedAppend {
    pub entry_suffix: z2VTq5,
    pub store_ledger: StoreLedger,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckedCheckpoint {
    pub checkpoint_artifact: z2VTXE,
    pub store_ledger: StoreLedger,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckedObjectNotice {
    pub object_notice: z2VZWt,
    pub store_ledger: StoreLedger,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct HeadStamp {
    pub sequence: u64,
    pub digest: String,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct StoredHead {
    pub store: String,
    pub head: Option<HeadStamp>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ReceivedEntry {
    pub store: String,
    pub sequence: u64,
    pub previous_digest: Option<String>,
    pub digest: String,
    pub payload: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct StoredCheckpoint {
    pub store: String,
    pub checkpoint_sequence: u64,
    pub covered_end_sequence: u64,
    pub digest: String,
    pub artifact: Vec<u8>,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum RetentionRule {
    KeepEverything,
    KeepLatestCheckpoints(u64),
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RetentionSetting {
    pub scope: Option<String>,
    pub rule: RetentionRule,
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct StorePolicy {
    pub store: String,
    pub addressing: ContentAddressing,
}

pub struct RecordFamily;

impl RecordFamily {
    pub const STORE_NAME: &'static str = "mirror:sema";

    const HEAD_SHAPE: &'static str =
        "mirror.StoredHead.v2{store:String,head:Option<{sequence:u64,digest:String}>}";
    const ENTRY_SHAPE: &'static str = "mirror.ReceivedEntry.v2{store:String,sequence:u64,previous_digest:Option<String>,digest:String,payload:Bytes}";
    const CHECKPOINT_SHAPE: &'static str = "mirror.StoredCheckpoint.v2{store:String,checkpoint_sequence:u64,covered_end_sequence:u64,digest:String,artifact:Bytes}";
    const RETENTION_SHAPE: &'static str = "mirror.RetentionSetting.v2{scope:Option<String>,rule:KeepEverything|KeepLatestCheckpoints(u64)}";
    const POLICY_SHAPE: &'static str =
        "mirror.StorePolicy.v2{store:String,addressing:Opaque|SemaVersionedLog}";

    pub fn versioning_policy() -> sema_engine::VersioningPolicy {
        sema_engine::VersioningPolicy::new(sema_engine::VersionedStoreName::new(Self::STORE_NAME))
            .with_recovery_topology(sema_engine::VersionedRecoveryTopology::Mirror)
    }

    pub fn head_family() -> sema_engine::TableDescriptor<StoredHead> {
        Self::descriptor("store-heads", "Head", Self::HEAD_SHAPE)
    }

    pub fn entry_family() -> sema_engine::TableDescriptor<ReceivedEntry> {
        Self::descriptor("received-entries", "Entry", Self::ENTRY_SHAPE)
    }

    pub fn checkpoint_family() -> sema_engine::TableDescriptor<StoredCheckpoint> {
        Self::descriptor("checkpoint-artifacts", "Checkpoint", Self::CHECKPOINT_SHAPE)
    }

    pub fn retention_family() -> sema_engine::TableDescriptor<RetentionSetting> {
        Self::descriptor("retention-rules", "Retention", Self::RETENTION_SHAPE)
    }

    pub fn policy_family() -> sema_engine::TableDescriptor<StorePolicy> {
        Self::descriptor("store-policies", "Policy", Self::POLICY_SHAPE)
    }

    fn descriptor<Record>(
        table: &'static str,
        family: &'static str,
        shape: &'static str,
    ) -> sema_engine::TableDescriptor<Record> {
        sema_engine::TableDescriptor::new(
            sema_engine::TableName::new(table),
            sema_engine::FamilyName::new(family),
            sema_engine::SchemaHash::for_label(shape),
        )
    }
}
