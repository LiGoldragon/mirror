//! Re-derive the content address of a body the mirror handed back.
//!
//! The mirror is payload-blind: it lands and returns the carried octets
//! verbatim. A witness that wants to prove a landed body is the genuine,
//! intact record — not a substituted or corrupted payload — re-derives the
//! body's content address through sema-engine's OWN content-addressing and
//! compares it to the head the mirror reports.
//!
//! `LandedBody` is the single source of truth for that re-hash: both the
//! in-process `Restore` readback test (`tests/landed_body_readback.rs`) and the
//! two-VM witness verifier bin (`bin/mirror-landed-body-verifier`) re-derive
//! through it, so the logic the live VM exercises is exactly the logic a fast
//! Nix check proves.

use sema_engine::{EntryDigest, VersionedCommitLogEntry};

use crate::error::{Error, Result};

/// The octets a mirror handed back for one landed entry — the body the witness
/// re-hashes. Borrowed because every caller already holds the payload slice
/// (`EntryEnvelope::payload`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LandedBody<'octets> {
    octets: &'octets [u8],
}

impl<'octets> LandedBody<'octets> {
    pub fn new(octets: &'octets [u8]) -> Self {
        Self { octets }
    }

    pub fn octets(&self) -> &[u8] {
        self.octets
    }

    /// Decode the body as the `rkyv` `VersionedCommitLogEntry` it must be,
    /// reconstruct it through the PUBLIC `VersionedCommitLogEntry::new` (which
    /// recomputes the digest from the entry's structured fields), and return
    /// the re-derived content address. A substituted or corrupted body
    /// re-derives to a different digest; a body that is not a versioned-log
    /// entry fails to decode.
    pub fn content_address(&self) -> Result<EntryDigest> {
        let decoded = rkyv::from_bytes::<VersionedCommitLogEntry, rkyv::rancor::Error>(self.octets)
            .map_err(|error| Error::LandedBodyDecode(error.to_string()))?;
        let rederived = VersionedCommitLogEntry::new(
            decoded.store_name().clone(),
            decoded.schema_hash(),
            decoded.commit_sequence(),
            decoded.snapshot(),
            decoded.previous_entry_digest(),
            decoded.operations().clone(),
        );
        Ok(rederived.entry_digest())
    }
}
