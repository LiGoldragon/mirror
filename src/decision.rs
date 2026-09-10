//! Pure Mirror ledger decisions.
//!
//! Wire values enter with their allocated Ethos identities. Mirror projects
//! only the private state needed to decide, then returns a value of the same
//! authority-owned Interface.

use signal_mirror::{
    AppendReceipt, AppendRejection, AppendRejectionReason, CheckpointArtifact, CheckpointReceipt,
    EntryEnvelope, HeadMark, ObjectNoticeReceipt, ObjectNoticeRejection,
    ObjectNoticeRejectionReason, PublishRejection, PublishRejectionReason, StoreName,
};

use crate::ledger::{
    CheckedAppend, CheckedCheckpoint, CheckedObjectNotice, ContentAddressing, KnownEntry,
    NovelSuffix, RegisteredLedger, StoreLedger,
};
use crate::readback::LandedBody;

#[derive(Clone, Debug, PartialEq)]
pub enum AppendDecision {
    Accept(NovelSuffix),
    Duplicate(AppendReceipt),
    Refuse(AppendRejection),
}

#[derive(Clone, Debug, PartialEq)]
pub enum CheckpointDecision {
    Accept(CheckpointArtifact),
    Duplicate(CheckpointReceipt),
    Refuse(PublishRejection),
}

#[derive(Clone, Debug, PartialEq)]
pub enum ObjectNoticeDecision {
    Accept(ObjectNoticeReceipt),
    Refuse(ObjectNoticeRejection),
}

impl CheckedAppend {
    pub fn into_decision(self) -> AppendDecision {
        let Self {
            entry_suffix: request,
            store_ledger: ledger,
        } = self;
        let StoreLedger::Registered(ledger) = ledger else {
            return AppendDecision::Refuse(append_rejection(
                request.store_name,
                AppendRejectionReason::UnknownStore,
                None,
            ));
        };
        let entries = request.entry_envelope_vector;
        if entries.is_empty() {
            return AppendDecision::Refuse(append_rejection(
                request.store_name,
                AppendRejectionReason::EmptySuffix,
                ledger.head().cloned(),
            ));
        }
        if let Some(reason) = ledger.suffix_inconsistency(&entries) {
            return AppendDecision::Refuse(append_rejection(
                request.store_name,
                reason,
                ledger.head().cloned(),
            ));
        }
        if let Some(reason) =
            ledger.expected_head_violation(request.head_mark_option.as_ref(), &entries)
        {
            return AppendDecision::Refuse(append_rejection(
                request.store_name,
                reason,
                ledger.head().cloned(),
            ));
        }
        if let Some(reason) = ledger.known_divergence(&entries) {
            return AppendDecision::Refuse(append_rejection(
                request.store_name,
                reason,
                ledger.head().cloned(),
            ));
        }
        if let Some(reason) = ledger.body_addressing_violation(&entries) {
            return AppendDecision::Refuse(append_rejection(
                request.store_name,
                reason,
                ledger.head().cloned(),
            ));
        }

        let last = entries.last().expect("empty suffix refused");
        let suffix_end = HeadMark {
            commit_sequence: last.commit_sequence,
            object_digest: last.object_digest.clone(),
        };
        let novel = entries
            .into_iter()
            .filter(|entry| {
                ledger
                    .known_digest(
                        u64::try_from(entry.commit_sequence).expect("nonnegative sequence"),
                    )
                    .is_none()
            })
            .collect::<Vec<_>>();
        if novel.is_empty()
            && u64::try_from(suffix_end.commit_sequence).expect("nonnegative sequence")
                <= ledger.head_sequence()
        {
            let Some(head) = ledger.head().cloned() else {
                return AppendDecision::Refuse(append_rejection(
                    request.store_name,
                    AppendRejectionReason::SequenceGap,
                    None,
                ));
            };
            return AppendDecision::Duplicate(AppendReceipt {
                store_name: request.store_name,
                head_mark: head,
            });
        }
        AppendDecision::Accept(NovelSuffix::new(request.store_name, suffix_end, novel))
    }
}

fn append_rejection(
    store: StoreName,
    reason: AppendRejectionReason,
    head: Option<HeadMark>,
) -> AppendRejection {
    AppendRejection {
        store_name: store,
        append_rejection_reason: reason,
        head_mark_option: head,
    }
}

impl RegisteredLedger {
    fn head_sequence(&self) -> u64 {
        self.head().map_or(0, |head| {
            u64::try_from(head.commit_sequence).expect("nonnegative sequence")
        })
    }

    fn known_digest(&self, sequence: u64) -> Option<&KnownEntry> {
        self.known().iter().find(|entry| entry.sequence == sequence)
    }

    fn suffix_inconsistency(&self, entries: &[EntryEnvelope]) -> Option<AppendRejectionReason> {
        for window in entries.windows(2) {
            let previous = &window[0];
            let next = &window[1];
            if u64::try_from(next.commit_sequence).expect("nonnegative sequence")
                != u64::try_from(previous.commit_sequence).expect("nonnegative sequence") + 1
            {
                return Some(AppendRejectionReason::SequenceGap);
            }
            if next.object_digest_option.as_ref() != Some(&previous.object_digest) {
                return Some(AppendRejectionReason::DigestMismatch);
            }
        }
        None
    }

    fn expected_head_violation(
        &self,
        expected: Option<&HeadMark>,
        entries: &[EntryEnvelope],
    ) -> Option<AppendRejectionReason> {
        let first = entries.first()?;
        let first_sequence = u64::try_from(first.commit_sequence).expect("nonnegative sequence");
        match expected {
            None if first_sequence != 1 || first.object_digest_option.is_some() => {
                Some(AppendRejectionReason::SequenceGap)
            }
            None => None,
            Some(mark) => {
                let mark_sequence =
                    u64::try_from(mark.commit_sequence).expect("nonnegative sequence");
                if mark_sequence + 1 != first_sequence {
                    return Some(AppendRejectionReason::SequenceGap);
                }
                if first.object_digest_option.as_ref() != Some(&mark.object_digest) {
                    return Some(AppendRejectionReason::HeadForked);
                }
                match self.known_digest(mark_sequence) {
                    Some(known) if known.digest == mark.object_digest.as_str() => None,
                    Some(_) => Some(AppendRejectionReason::DigestMismatch),
                    None => Some(AppendRejectionReason::SequenceGap),
                }
            }
        }
    }

    fn known_divergence(&self, entries: &[EntryEnvelope]) -> Option<AppendRejectionReason> {
        let head_sequence = self.head_sequence();
        for entry in entries {
            let sequence = u64::try_from(entry.commit_sequence).expect("nonnegative sequence");
            match self.known_digest(sequence) {
                Some(known) if known.digest == entry.object_digest.as_str() => {}
                Some(_) => return Some(AppendRejectionReason::HeadForked),
                None if sequence <= head_sequence => {
                    return Some(AppendRejectionReason::SequenceGap);
                }
                None => {}
            }
        }
        None
    }

    fn body_addressing_violation(
        &self,
        entries: &[EntryEnvelope],
    ) -> Option<AppendRejectionReason> {
        match self.addressing() {
            ContentAddressing::Opaque => None,
            ContentAddressing::SemaVersionedLog => entries
                .iter()
                .find(|entry| {
                    entry
                        .payload_bytes
                        .iter()
                        .map(|value| u8::try_from(*value))
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_or(true, |octets| {
                            !LandedBody::new(&octets).addresses_to(&entry.object_digest)
                        })
                })
                .map(|_| AppendRejectionReason::HeadForked),
        }
    }
}

impl CheckedCheckpoint {
    pub fn into_decision(self) -> CheckpointDecision {
        let Self {
            checkpoint_artifact: artifact,
            store_ledger: ledger,
        } = self;
        let StoreLedger::Registered(ledger) = ledger else {
            return CheckpointDecision::Refuse(PublishRejection {
                store_name: artifact.store_name,
                publish_rejection_reason: PublishRejectionReason::UnknownStore,
            });
        };
        match ledger.latest_checkpoint() {
            None => CheckpointDecision::Accept(artifact),
            Some(latest) => {
                let latest_sequence =
                    u64::try_from(latest.checkpoint_sequence).expect("nonnegative checkpoint");
                let artifact_sequence =
                    u64::try_from(artifact.checkpoint_sequence).expect("nonnegative checkpoint");
                if artifact_sequence == latest_sequence
                    && artifact.commit_sequence == latest.commit_sequence
                {
                    return CheckpointDecision::Duplicate(CheckpointReceipt {
                        store_name: latest.store_name.clone(),
                        checkpoint_sequence: latest.checkpoint_sequence,
                        commit_sequence: latest.commit_sequence,
                    });
                }
                if artifact_sequence <= latest_sequence
                    || artifact.commit_sequence < latest.commit_sequence
                {
                    return CheckpointDecision::Refuse(PublishRejection {
                        store_name: artifact.store_name,
                        publish_rejection_reason: PublishRejectionReason::CoverageRegressed,
                    });
                }
                CheckpointDecision::Accept(artifact)
            }
        }
    }
}

impl CheckedObjectNotice {
    pub fn into_decision(self) -> ObjectNoticeDecision {
        let Self {
            object_notice: notice,
            store_ledger: ledger,
        } = self;
        let StoreLedger::Registered(ledger) = ledger else {
            return ObjectNoticeDecision::Refuse(ObjectNoticeRejection {
                store_name: notice.store_name,
                object_notice_rejection_reason: ObjectNoticeRejectionReason::UnknownStore,
                head_mark_option: None,
            });
        };
        if ledger.has_known_head(&notice.head_mark) {
            return ObjectNoticeDecision::Accept(ObjectNoticeReceipt {
                store_name: notice.store_name,
                head_mark: notice.head_mark,
            });
        }
        ObjectNoticeDecision::Refuse(ObjectNoticeRejection {
            store_name: notice.store_name,
            object_notice_rejection_reason: ObjectNoticeRejectionReason::HeadBehind,
            head_mark_option: ledger.head().cloned(),
        })
    }
}

impl RegisteredLedger {
    fn has_known_head(&self, head: &HeadMark) -> bool {
        self.known().iter().any(|known| {
            known.sequence == u64::try_from(head.commit_sequence).expect("nonnegative sequence")
                && known.digest == head.object_digest.as_str()
        })
    }
}
