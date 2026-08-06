# mirror agent notes

Read this repo's `ARCHITECTURE.md` before editing.

`mirror` is the daemon of the mirror triad — the payload-blind sema
version-control remote (Spirit `0yx5`).

Load-bearing rules for this repo:

- The mirror is payload-blind: never decode component payload or artifact
  bytes except when a store explicitly selects `SemaVersionedLog` addressing.
- The append decision (expected head, idempotent dedup, gap/fork) stays a pure,
  visible decision in `src/decision.rs`; do not move it into hidden store logic.
- The reply is sent only after the persisting redb transaction committed
  (ack-after-durable-write); never acknowledge before the engine commit.
- The meta surface (registration, retirement, retention, configure) is
  Unix-owner-only; the TCP ingress decodes the ordinary contract
  exclusively. Never make meta reachable over TCP.
- The daemon takes exactly one binary rkyv argument and never parses Dotos.
- Consume the published Ethos Interfaces directly. Do not add a local schema
  language, build-time emitter, readable wire alias module, or compatibility
  copy of an Interface.
- Retention is a stored placeholder, not enforced (deferred by decision).
