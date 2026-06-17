//! The mirror's Nexus decisions, attached to the schema-emitted checked
//! nouns.
//!
//! `CheckedAppend::into_decision` is the engine's load-bearing internal
//! feature (declared in `schema/nexus.schema` as `AppendDecision`):
//! expected-head validation, idempotent dedup by entry digest against
//! the LOADED KNOWN ROWS (including crash-window rows above the head),
//! and gap/fork rejection. `CheckedCheckpoint::into_decision` validates
//! registration and coverage monotonicity. Both are pure projections
//! from looked-up ledger state to a typed decision — no storage access
//! here; the single-writer engine actor makes read-decide-write atomic.

use signal_mirror::{
    AppendReceipt, AppendRejection, AppendRejectionReason, EntryEnvelope, HeadMark,
    ObjectNoticeReceipt, ObjectNoticeRejection, ObjectNoticeRejectionReason, PublishRejection,
    PublishRejectionReason,
};

use crate::schema::nexus::{AppendDecision, CheckpointDecision, ObjectNoticeDecision};
use crate::schema::sema::{
    CheckedAppend, CheckedCheckpoint, CheckedObjectNotice, KnownEntry, NovelSuffix,
    RegisteredLedger, StoreLedger,
};

impl CheckedAppend {
    /// Decide one append against the looked-up ledger state.
    pub fn into_decision(self) -> AppendDecision {
        let Self { request, ledger } = self;
        let StoreLedger::Registered(ledger) = ledger else {
            return AppendDecision::RefuseAppend(AppendRejection {
                store: request.store,
                reason: AppendRejectionReason::UnknownStore,
                head: None,
            });
        };
        let refuse = |reason: AppendRejectionReason,
                      store: signal_mirror::StoreName,
                      head: Option<HeadMark>| {
            AppendDecision::RefuseAppend(AppendRejection {
                store,
                reason,
                head,
            })
        };
        if request.entries.is_empty() {
            return refuse(
                AppendRejectionReason::EmptySuffix,
                request.store,
                ledger.head,
            );
        }
        if let Some(reason) = ledger.suffix_inconsistency(&request.entries) {
            return refuse(reason, request.store, ledger.head);
        }
        if let Some(reason) =
            ledger.expected_head_violation(request.expected_head.as_ref(), &request.entries)
        {
            return refuse(reason, request.store, ledger.head);
        }
        if let Some(reason) = ledger.known_divergence(&request.entries) {
            return refuse(reason, request.store, ledger.head);
        }
        let Some(suffix_end) = request.entries.last().map(|entry| HeadMark {
            sequence: entry.sequence.clone(),
            digest: entry.digest.clone(),
        }) else {
            // Unreachable: the empty suffix was refused above.
            return refuse(
                AppendRejectionReason::EmptySuffix,
                request.store,
                ledger.head,
            );
        };
        // The novel remainder is computed against the LOADED KNOWN ROWS,
        // never the head sequence: a crash between the entry transaction
        // and the head advance leaves rows ABOVE the head, and the
        // shipper's re-send must dedup against them (digest-verified by
        // known_divergence above) instead of re-asserting and faulting.
        let novel: Vec<EntryEnvelope> = request
            .entries
            .into_iter()
            .filter(|entry| {
                ledger
                    .known_digest(entry.sequence.clone().into_u64())
                    .is_none()
            })
            .collect();
        if novel.is_empty() && suffix_end.sequence.clone().into_u64() <= ledger.head_sequence() {
            // Every entry already stored at or below the head with
            // matching digests: the idempotent acknowledgement — same
            // head, no rewrite. A headless ledger cannot hold such a
            // re-send; refuse rather than panic if the state is
            // inconsistent.
            let Some(head) = ledger.head else {
                return refuse(AppendRejectionReason::SequenceGap, request.store, None);
            };
            return AppendDecision::AcknowledgeDuplicate(AppendReceipt {
                store: request.store,
                head,
            });
        }
        // A nonempty remainder persists and advances the head. An EMPTY
        // remainder whose suffix end lies above the head is the healed
        // crash window: a head-only re-advance (`Store::persist_suffix`
        // skips the empty entry transaction).
        AppendDecision::AcceptSuffix(NovelSuffix {
            store: request.store,
            head: suffix_end,
            entries: novel,
        })
    }
}

impl RegisteredLedger {
    fn head_sequence(&self) -> u64 {
        self.head
            .as_ref()
            .map(|head| head.sequence.clone().into_u64())
            .unwrap_or(0)
    }

    fn known_digest(&self, sequence: u64) -> Option<&KnownEntry> {
        self.known
            .iter()
            .find(|entry| entry.sequence.clone().into_u64() == sequence)
    }

    /// The suffix must be internally consecutive and digest-chained.
    fn suffix_inconsistency(&self, entries: &[EntryEnvelope]) -> Option<AppendRejectionReason> {
        for window in entries.windows(2) {
            let previous = &window[0];
            let next = &window[1];
            if next.sequence.clone().into_u64() != previous.sequence.clone().into_u64() + 1 {
                return Some(AppendRejectionReason::SequenceGap);
            }
            if next.previous_digest.as_ref() != Some(&previous.digest) {
                return Some(AppendRejectionReason::HeadForked);
            }
        }
        None
    }

    /// The expected head names the entry just before the suffix: absent
    /// for a genesis suffix, otherwise its digest must match what the
    /// ledger stored at that sequence.
    fn expected_head_violation(
        &self,
        expected: Option<&HeadMark>,
        entries: &[EntryEnvelope],
    ) -> Option<AppendRejectionReason> {
        let first = entries.first()?;
        let first_sequence = first.sequence.clone().into_u64();
        match expected {
            None => {
                if first_sequence != 1 || first.previous_digest.is_some() {
                    return Some(AppendRejectionReason::SequenceGap);
                }
            }
            Some(mark) => {
                let mark_sequence = mark.sequence.clone().into_u64();
                if mark_sequence + 1 != first_sequence {
                    return Some(AppendRejectionReason::SequenceGap);
                }
                if first.previous_digest.as_ref() != Some(&mark.digest) {
                    return Some(AppendRejectionReason::DigestMismatch);
                }
                // The mark must name a STORED row — the loaded known
                // rows, not the head: after a crash between the entry
                // transaction and the head advance, the row the shipper
                // continues from may sit above the head. A missing row
                // means the shipper believes the mirror holds history
                // it does not have yet.
                match self.known_digest(mark_sequence) {
                    Some(known) if known.digest == mark.digest => {}
                    Some(_) => return Some(AppendRejectionReason::HeadForked),
                    None => return Some(AppendRejectionReason::SequenceGap),
                }
            }
        }
        None
    }

    /// Entries the ledger already holds — at or below the head, or in
    /// the crash window above it — must match the stored digests
    /// exactly: a divergent re-send is a fork, never a rewrite. An
    /// entry at or below the head with no stored row is a hole the
    /// shipper cannot fill; reject it as a gap.
    fn known_divergence(&self, entries: &[EntryEnvelope]) -> Option<AppendRejectionReason> {
        let head_sequence = self.head_sequence();
        for entry in entries {
            let sequence = entry.sequence.clone().into_u64();
            match self.known_digest(sequence) {
                Some(known) if known.digest == entry.digest => {}
                Some(_) => return Some(AppendRejectionReason::DigestMismatch),
                None if sequence <= head_sequence => {
                    return Some(AppendRejectionReason::SequenceGap);
                }
                None => {}
            }
        }
        None
    }
}

impl CheckedCheckpoint {
    /// Decide one checkpoint publication against the looked-up ledger
    /// state: unknown stores are refused, coverage never regresses, and
    /// a re-publish of the held checkpoint acknowledges idempotently.
    pub fn into_decision(self) -> CheckpointDecision {
        let Self { artifact, ledger } = self;
        let StoreLedger::Registered(ledger) = ledger else {
            return CheckpointDecision::RefuseCheckpoint(PublishRejection {
                store: artifact.store,
                reason: PublishRejectionReason::UnknownStore,
            });
        };
        match &ledger.latest_checkpoint {
            None => CheckpointDecision::AcceptCheckpoint(artifact),
            Some(latest) => {
                let latest_sequence = latest.sequence.clone().into_u64();
                let artifact_sequence = artifact.sequence.clone().into_u64();
                if artifact_sequence == latest_sequence
                    && artifact.covered_end == latest.covered_end
                {
                    return CheckpointDecision::AcknowledgeCheckpoint(latest.clone());
                }
                if artifact_sequence <= latest_sequence
                    || artifact.covered_end.clone().into_u64()
                        < latest.covered_end.clone().into_u64()
                {
                    return CheckpointDecision::RefuseCheckpoint(PublishRejection {
                        store: artifact.store,
                        reason: PublishRejectionReason::CoverageRegressed,
                    });
                }
                CheckpointDecision::AcceptCheckpoint(artifact)
            }
        }
    }
}

impl CheckedObjectNotice {
    /// Decide whether a routed-object notice names history this mirror
    /// already holds. The current receiver surface is intentionally a
    /// notice/ack boundary: fetching missing history from `source` is a
    /// later synchronization step, so an unknown or missing head is a
    /// typed refusal rather than an implicit network fetch.
    pub fn into_decision(self) -> ObjectNoticeDecision {
        let Self { notice, ledger } = self;
        let StoreLedger::Registered(ledger) = ledger else {
            return ObjectNoticeDecision::RefuseObjectNotice(ObjectNoticeRejection {
                store: notice.store,
                reason: ObjectNoticeRejectionReason::UnknownStore,
                head: None,
            });
        };
        if ledger.has_known_head(&notice.head) {
            return ObjectNoticeDecision::AcceptObjectNotice(ObjectNoticeReceipt {
                store: notice.store,
                head: notice.head,
            });
        }
        ObjectNoticeDecision::RefuseObjectNotice(ObjectNoticeRejection {
            store: notice.store,
            reason: ObjectNoticeRejectionReason::HeadBehind,
            head: ledger.head,
        })
    }
}

impl RegisteredLedger {
    fn has_known_head(&self, head: &HeadMark) -> bool {
        self.known
            .iter()
            .any(|known| known.sequence == head.sequence && known.digest == head.digest)
    }
}
