//! Pure Mirror ledger decisions.
//!
//! Wire values enter with their allocated Ethos identities. Mirror projects
//! only the private state needed to decide, then returns a value of the same
//! authority-owned Interface.

use signal_mirror::{
    z2VLxP, z2VQTe, z2VTXE, z2VUTH, z2VWFj, z2VaxY, z2VbP4, z2VcqM, z2Vcs2, z2VcyE, z2VdLR, z2Ve8p,
};

use crate::ledger::{
    CheckedAppend, CheckedCheckpoint, CheckedObjectNotice, ContentAddressing, KnownEntry,
    NovelSuffix, RegisteredLedger, StoreLedger,
};
use crate::readback::LandedBody;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AppendDecision {
    Accept(NovelSuffix),
    Duplicate(z2VaxY),
    Refuse(z2VUTH),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CheckpointDecision {
    Accept(z2VTXE),
    Duplicate(z2VLxP),
    Refuse(z2VbP4),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObjectNoticeDecision {
    Accept(z2VWFj),
    Refuse(z2VQTe),
}

impl CheckedAppend {
    pub fn into_decision(self) -> AppendDecision {
        let Self {
            entry_suffix: request,
            store_ledger: ledger,
        } = self;
        let StoreLedger::Registered(ledger) = ledger else {
            return AppendDecision::Refuse(append_rejection(request.field_0, z2VcyE::z2VcFn, None));
        };
        let entries = request.field_2;
        if entries.is_empty() {
            return AppendDecision::Refuse(append_rejection(
                request.field_0,
                z2VcyE::z2VRBT,
                ledger.head().cloned(),
            ));
        }
        if let Some(reason) = ledger.suffix_inconsistency(&entries) {
            return AppendDecision::Refuse(append_rejection(
                request.field_0,
                reason,
                ledger.head().cloned(),
            ));
        }
        if let Some(reason) = ledger.expected_head_violation(request.field_1.as_ref(), &entries) {
            return AppendDecision::Refuse(append_rejection(
                request.field_0,
                reason,
                ledger.head().cloned(),
            ));
        }
        if let Some(reason) = ledger.known_divergence(&entries) {
            return AppendDecision::Refuse(append_rejection(
                request.field_0,
                reason,
                ledger.head().cloned(),
            ));
        }
        if let Some(reason) = ledger.body_addressing_violation(&entries) {
            return AppendDecision::Refuse(append_rejection(
                request.field_0,
                reason,
                ledger.head().cloned(),
            ));
        }

        let last = entries.last().expect("empty suffix refused");
        let suffix_end = z2VcqM {
            field_0: last.field_0.clone(),
            field_1: last.field_2.clone(),
        };
        let novel = entries
            .into_iter()
            .filter(|entry| ledger.known_digest(*entry.field_0.payload()).is_none())
            .collect::<Vec<_>>();
        if novel.is_empty() && *suffix_end.field_0.payload() <= ledger.head_sequence() {
            let Some(head) = ledger.head().cloned() else {
                return AppendDecision::Refuse(append_rejection(
                    request.field_0,
                    z2VcyE::z2VQma,
                    None,
                ));
            };
            return AppendDecision::Duplicate(z2VaxY {
                field_0: request.field_0,
                field_1: head,
            });
        }
        AppendDecision::Accept(NovelSuffix::new(request.field_0, suffix_end, novel))
    }
}

fn append_rejection(store: z2Ve8p, reason: z2VcyE, head: Option<z2VcqM>) -> z2VUTH {
    z2VUTH {
        field_0: store,
        field_1: reason,
        field_2: head,
    }
}

impl RegisteredLedger {
    fn head_sequence(&self) -> u64 {
        self.head().map_or(0, |head| *head.field_0.payload())
    }

    fn known_digest(&self, sequence: u64) -> Option<&KnownEntry> {
        self.known().iter().find(|entry| entry.sequence == sequence)
    }

    fn suffix_inconsistency(&self, entries: &[signal_mirror::z2VPuU]) -> Option<z2VcyE> {
        for window in entries.windows(2) {
            let previous = &window[0];
            let next = &window[1];
            if *next.field_0.payload() != *previous.field_0.payload() + 1 {
                return Some(z2VcyE::z2VQma);
            }
            if next.field_1.as_ref() != Some(&previous.field_2) {
                return Some(z2VcyE::z2VZT1);
            }
        }
        None
    }

    fn expected_head_violation(
        &self,
        expected: Option<&z2VcqM>,
        entries: &[signal_mirror::z2VPuU],
    ) -> Option<z2VcyE> {
        let first = entries.first()?;
        let first_sequence = *first.field_0.payload();
        match expected {
            None if first_sequence != 1 || first.field_1.is_some() => Some(z2VcyE::z2VQma),
            None => None,
            Some(mark) => {
                let mark_sequence = *mark.field_0.payload();
                if mark_sequence + 1 != first_sequence {
                    return Some(z2VcyE::z2VQma);
                }
                if first.field_1.as_ref() != Some(&mark.field_1) {
                    return Some(z2VcyE::z2VPd1);
                }
                match self.known_digest(mark_sequence) {
                    Some(known) if known.digest == mark.field_1.as_str() => None,
                    Some(_) => Some(z2VcyE::z2VZT1),
                    None => Some(z2VcyE::z2VQma),
                }
            }
        }
    }

    fn known_divergence(&self, entries: &[signal_mirror::z2VPuU]) -> Option<z2VcyE> {
        let head_sequence = self.head_sequence();
        for entry in entries {
            let sequence = *entry.field_0.payload();
            match self.known_digest(sequence) {
                Some(known) if known.digest == entry.field_2.as_str() => {}
                Some(_) => return Some(z2VcyE::z2VPd1),
                None if sequence <= head_sequence => return Some(z2VcyE::z2VQma),
                None => {}
            }
        }
        None
    }

    fn body_addressing_violation(&self, entries: &[signal_mirror::z2VPuU]) -> Option<z2VcyE> {
        match self.addressing() {
            ContentAddressing::Opaque => None,
            ContentAddressing::SemaVersionedLog => entries
                .iter()
                .find(|entry| {
                    entry.field_3.octets().map_or(true, |octets| {
                        !LandedBody::new(&octets).addresses_to(&entry.field_2)
                    })
                })
                .map(|_| z2VcyE::z2VPd1),
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
            return CheckpointDecision::Refuse(z2VbP4 {
                field_0: artifact.field_0,
                field_1: z2Vcs2::z2VWLf,
            });
        };
        match ledger.latest_checkpoint() {
            None => CheckpointDecision::Accept(artifact),
            Some(latest) => {
                let latest_sequence = *latest.field_1.payload();
                let artifact_sequence = *artifact.field_1.payload();
                if artifact_sequence == latest_sequence && artifact.field_2 == latest.field_2 {
                    return CheckpointDecision::Duplicate(latest.clone());
                }
                if artifact_sequence <= latest_sequence
                    || *artifact.field_2.payload() < *latest.field_2.payload()
                {
                    return CheckpointDecision::Refuse(z2VbP4 {
                        field_0: artifact.field_0,
                        field_1: z2Vcs2::z2VLE1,
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
            return ObjectNoticeDecision::Refuse(z2VQTe {
                field_0: notice.field_0,
                field_1: z2VdLR::z2VZJ4,
                field_2: None,
            });
        };
        if ledger.has_known_head(&notice.field_1) {
            return ObjectNoticeDecision::Accept(z2VWFj {
                field_0: notice.field_0,
                field_1: notice.field_1,
            });
        }
        ObjectNoticeDecision::Refuse(z2VQTe {
            field_0: notice.field_0,
            field_1: z2VdLR::z2VLZN,
            field_2: ledger.head().cloned(),
        })
    }
}

impl RegisteredLedger {
    fn has_known_head(&self, head: &z2VcqM) -> bool {
        self.known().iter().any(|known| {
            known.sequence == *head.field_0.payload() && known.digest == head.field_1.as_str()
        })
    }
}
