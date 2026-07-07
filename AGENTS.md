# mirror agent notes

Read this repo's `ARCHITECTURE.md` before editing.

`mirror` is the daemon of the mirror triad — the payload-blind sema
version-control remote (Spirit `0yx5`).

Load-bearing rules for this repo:

- The mirror is payload-blind: never decode component payload or artifact
  bytes; validation is digest-chain validation only.
- The append decision (expected head, idempotent dedup, gap/fork) lives in
  the Nexus plane as the schema-declared `AppendDecision`; do not move it
  into hidden store logic.
- The reply is sent only after the persisting redb transaction committed
  (ack-after-durable-write); never acknowledge before the engine commit.
- The meta surface (registration, retirement, retention, configure) is
  Unix-owner-only; the TCP ingress decodes the ordinary contract
  exclusively. Never make meta reachable over TCP.
- The daemon takes exactly one binary rkyv argument and never parses NOTA.
- Edit `schema/nexus.schema` / `schema/sema.schema` and regenerate
  (`MIRROR_UPDATE_SCHEMA_ARTIFACTS=1 cargo build`); never hand-edit
  `src/schema/*.rs`. A sema.schema edit that moves a family closure moves
  the generated `family_identity` constants — that is the version-control
  surface working as intended; never paper over it.
- Retention is a stored placeholder, not enforced (deferred by decision).
