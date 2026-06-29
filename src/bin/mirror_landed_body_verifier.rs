//! `mirror-landed-body-verifier` — the two-VM witness's in-VM full-body proof.
//!
//! The witness forwards a REAL content-addressed record body across the criome
//! gate; node-b's mirror lands it. This bin runs ON node-b, reads that landed
//! body back OUT of the running mirror daemon over its own working contract
//! (`Restore`, after a zero-coverage checkpoint the testScript published),
//! re-derives the body's content address through sema-engine's OWN
//! content-addressing (`mirror::LandedBody::content_address`), and asserts the
//! re-derived 32 digest BYTES equal the real spirit head the witness forwarded.
//!
//! It decodes the binary `Output::Restored` wire reply directly — no NOTA
//! byte-list parsing in shell — and exits nonzero on any mismatch, so the
//! testScript proves the full-body landing by the bin's exit code, not by a
//! shell string compare against a Debug render.
//!
//! Environment (the spirit/mirror CLI convention — one socket var, no flags):
//!   MIRROR_SOCKET      the mirror daemon's working Unix socket
//!   WITNESS_STORE      the store name to restore (default `spirit`)
//!   EXPECTED_HEAD_HEX  the real forwarded head as 64 lowercase hex chars

use std::process::ExitCode;

use mirror::LandedBody;
use mirror::client::DaemonSocket;
use signal_mirror::{Input, Output, RestoreQuery, StoreName};
use thiserror::Error;

/// The witness verifier resolved from its environment: where to read the landed
/// body back from, which store, and the 32 digest bytes the body must re-hash
/// to.
struct LandedBodyVerifier {
    socket: DaemonSocket,
    store: StoreName,
    store_label: String,
    expected: [u8; 32],
}

/// The non-secret evidence of a successful full-body re-hash, printed for the
/// witness transcript.
struct Verdict {
    store_label: String,
    octets: usize,
    rederived: sema_engine::EntryDigest,
    carried_hex: String,
    expected_hex: String,
}

#[derive(Debug, Error)]
enum VerifyError {
    #[error("MIRROR_SOCKET is not set")]
    SocketUnset,
    #[error("EXPECTED_HEAD_HEX is not set")]
    ExpectedUnset,
    #[error("EXPECTED_HEAD_HEX must be 64 lowercase hex chars: {0}")]
    BadExpectedHex(String),
    #[error("mirror round-trip: {0}")]
    Mirror(#[from] mirror::Error),
    #[error("expected Restored, got {0}")]
    UnexpectedReply(String),
    #[error("the restore suffix is empty — no landed body to verify")]
    EmptySuffix,
    #[error(
        "re-derived content address {rederived} != the real forwarded head {expected} — the landed body does NOT re-hash to the head"
    )]
    DigestMismatch { rederived: String, expected: String },
    #[error(
        "the mirror's carried head digest {carried} != the real forwarded head {expected} — the landed head is not the body's content address"
    )]
    CarriedMismatch { carried: String, expected: String },
}

impl LandedBodyVerifier {
    fn from_environment() -> Result<Self, VerifyError> {
        let socket =
            DaemonSocket::from_environment("MIRROR_SOCKET").ok_or(VerifyError::SocketUnset)?;
        let store_label = std::env::var("WITNESS_STORE").unwrap_or_else(|_| "spirit".to_owned());
        let expected_hex =
            std::env::var("EXPECTED_HEAD_HEX").map_err(|_| VerifyError::ExpectedUnset)?;
        let expected = Self::parse_expected_digest(&expected_hex)?;
        Ok(Self {
            socket,
            store: StoreName::new(store_label.clone()),
            store_label,
            expected,
        })
    }

    /// Decode the expected head's 64 hex chars into the 32 digest bytes the
    /// landed body must re-hash to. A fixed-width digest decode, not a grammar.
    fn parse_expected_digest(hex: &str) -> Result<[u8; 32], VerifyError> {
        let hex = hex.trim();
        if hex.len() != 64 {
            return Err(VerifyError::BadExpectedHex(hex.to_owned()));
        }
        let mut digest = [0_u8; 32];
        for (index, slot) in digest.iter_mut().enumerate() {
            let byte = hex
                .get(index * 2..index * 2 + 2)
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                .ok_or_else(|| VerifyError::BadExpectedHex(hex.to_owned()))?;
            *slot = byte;
        }
        Ok(digest)
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    /// Read the landed body back over `Restore`, re-derive its content address,
    /// and prove both the body and the mirror's carried head equal the real
    /// forwarded head — by 32-byte comparison.
    fn verify(&self) -> Result<Verdict, VerifyError> {
        let reply = self
            .socket
            .request(Input::Restore(RestoreQuery::new(self.store.clone())))?;
        let bundle = match reply {
            Output::Restored(bundle) => bundle,
            other => return Err(VerifyError::UnexpectedReply(format!("{other:?}"))),
        };
        let landed = bundle.suffix().first().ok_or(VerifyError::EmptySuffix)?;
        let body = landed.payload.as_slice();

        let rederived = LandedBody::new(body).content_address()?;
        let expected_hex = Self::hex(&self.expected);
        if rederived.bytes() != &self.expected {
            return Err(VerifyError::DigestMismatch {
                rederived: rederived.to_string(),
                expected: expected_hex,
            });
        }

        let carried = landed.digest.as_bytes();
        if carried != &self.expected {
            return Err(VerifyError::CarriedMismatch {
                carried: Self::hex(carried),
                expected: expected_hex,
            });
        }

        Ok(Verdict {
            store_label: self.store_label.clone(),
            octets: body.len(),
            rederived,
            carried_hex: Self::hex(carried),
            expected_hex,
        })
    }
}

fn main() -> ExitCode {
    match LandedBodyVerifier::from_environment().and_then(|verifier| verifier.verify()) {
        Ok(verdict) => {
            println!(
                "LANDED_BODY_REHASH store={} octets={} rederived={} carried={} expected={} MATCH",
                verdict.store_label,
                verdict.octets,
                verdict.rederived,
                verdict.carried_hex,
                verdict.expected_hex,
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("LANDED_BODY_REHASH MISMATCH: {error}");
            ExitCode::from(3)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::LandedBodyVerifier;

    #[test]
    fn a_valid_64_hex_head_decodes_to_its_32_digest_bytes() {
        let hex = "326640ace33a02dac238e313cd91bcbd9a5a3dc75759fef49a3476e7fe35b85a";
        let bytes = LandedBodyVerifier::parse_expected_digest(hex).expect("valid digest hex");
        assert_eq!(bytes[0], 0x32);
        assert_eq!(bytes[1], 0x66);
        assert_eq!(bytes[31], 0x5a);
        // round-trips back to the same lowercase hex
        assert_eq!(LandedBodyVerifier::hex(&bytes), hex);
    }

    #[test]
    fn a_wrong_length_head_is_rejected() {
        assert!(LandedBodyVerifier::parse_expected_digest("326640ace3").is_err());
    }

    #[test]
    fn a_non_hex_head_is_rejected() {
        let not_hex = "z26640ace33a02dac238e313cd91bcbd9a5a3dc75759fef49a3476e7fe35b85a";
        assert!(LandedBodyVerifier::parse_expected_digest(not_hex).is_err());
    }
}
