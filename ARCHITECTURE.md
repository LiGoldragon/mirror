# mirror — architecture

Mirror is the payload-blind version-control remote of the Mirror triad. One
daemon serves many component stores. It validates append history, keeps the
received bytes opaque, commits them durably, and acknowledges only after the
commit returns.

Mirror was briefly deployed on ouranos and is currently disabled on every
host. It remains an unshipped daemon, not a place or an assumed service.

## Interface authority

The wire surfaces are consumed directly from the published Ethos Interfaces:

- `signal-mirror` owns ordinary append, checkpoint, object-notice, head, and
  restore traffic.
- `meta-signal-mirror` owns registration, retirement, retention, and startup
  configuration.
- `signal-standard` owns shared digest and network vocabulary.

Their encoded identities are the runtime types. Mirror publishes no readable
copies or aliases of those types. Dotos is used only at human/agent CLI edges;
the daemon receives allocated binary Signal frames.

Mirror's build is an ordinary Cargo build. Its private ledger types live in
`src/ledger.rs`; they are not another public Interface.

## One durable path

```text
Ethos input
    → load private ledger state
    → pure decision
    → sema-engine commit
    → Ethos output
```

`Engine` performs this path directly. `decision.rs` owns the pure append,
checkpoint, and object-notice decisions. `Store` owns sema-engine tables and
all durable mutation. The split keeps policy visible without interposing a
generic plane runner.

The private store is itself versioned as `mirror:sema`, with Mirror recovery
topology. Its record-family identities describe the ordinary Rust persistence
shape. Schema version 2 deliberately separates this representation from the
retired emitter-owned layout.

## Transports and ownership

```text
Unix working socket ──┐
Unix owner socket   ──┼── Service actor ── Engine ── Store
tailnet TCP ingress ──┘
```

`Service` is the only engine owner. Every transport forwards through its
mailbox, so there is exactly one writer. The ordinary Unix socket and tailnet
TCP listener decode only `signal-mirror`. The owner-only Unix socket has mode
`0600` and alone decodes `meta-signal-mirror`; meta traffic is structurally
impossible over TCP.

The current TCP trust boundary is the configured tailnet bind address. TCP
peers are recorded as typed `PeerIdentity::Tcp`; SSH-forwarded sockets are not
a third identity.

## Append law

An append is accepted only when:

- the store is registered;
- the suffix is non-empty, consecutive, and digest-chained;
- its expected head names a stored row immediately before the suffix;
- every already-known sequence carries the same digest;
- every sequence at or below the durable head has a stored row; and
- for a `SemaVersionedLog` store, each body re-derives to its carried digest.

Known rows, including rows ahead of the head after a crash window, are the
deduplication authority. A complete duplicate below the head writes nothing. A
crash-window resend with all entry rows already present advances only the head.
An opaque store never decodes payload or artifact bytes.

## Acknowledgement boundary

`Store::persist_suffix` commits entry rows and then the head row. A crash
between them leaves rows ahead of the head; idempotent resend heals that state.
`Store::persist_checkpoint` commits the artifact before producing its receipt.
No transport can encode a success before these methods return.

## Registration and retention

Registration is owner authority. Names containing `/` are rejected because
ordered keys use `<store>/<sequence>`. Retirement removes the head but retains
history; registering the name again resumes from the highest surviving entry.
Retention rules are durably stored policy and are not yet enforced.

## Executables

- `mirror-daemon` accepts exactly one binary rkyv configuration file.
- `mirror` accepts one Dotos ordinary request over `MIRROR_SOCKET`.
- `meta-mirror` accepts one Dotos owner request over `MIRROR_META_SOCKET`.
- `mirror-write-configuration` turns one Dotos configuration write into the
  binary startup file.
- `mirror-landed-body-verifier` re-hashes a restored body for the two-VM
  witness.

## Witnesses

- `daemon_logic` proves append decisions, crash-window healing, registry
  behavior, object notices, checkpoint/restore, and self-versioning.
- `append_addressing_refusal` proves addressed stores reject a mismatched body
  before persistence while opaque stores remain payload-blind.
- `landed_body_readback` proves Restore returns the exact landed bytes and that
  they re-derive to the carried digest.
- `end_to_end_arc` proves real TCP shipping, durable acknowledgement,
  checkpoint publication, and import into a fresh component store.
