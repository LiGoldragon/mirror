# INTENT — mirror

`mirror` is the payload-blind append-ingest mirror daemon — the sema
version-control remote, serving every component store from one daemon.

Psyche intent, quoted from the Spirit store:

Spirit `0yx5` (Decision, High): [The sema version-control remote is a
dedicated mirror component triad: mirror, signal-mirror, and
meta-signal-mirror. One payload-blind append-ingest mirror daemon on the
ouranos tailnet host serves every component store: it validates sequence
continuity and expected head, deduplicates idempotently, fsyncs before
acknowledging, and carries retention and privacy policy behind its meta
signal. The mirror daemon's own durable state is a sema-engine store. The
psyche authorizes creating the three new repositories.]

Spirit `rj9y` (Decision, High): [Cross-host component transport for the
version-control mirror is a tailnet-bound TCP listener in triad-runtime,
reusing the length-prefixed frame codec, with peer identity as a typed closed
sum distinguishing kernel-vouched Unix-socket peers from tailnet TCP peers.
Ssh-forwarded sockets are rejected as the transport shape.]

Spirit `29pb` (Constraint, High): [Component Sema databases, the daemon
durable state, must be backed up to a server atomically, and state loss is
unacceptable. Pursue native version-controlled component databases rather
than treating the store as an opaque binary blob. Mechanism is under design
and Dolt-informed, with the strict-typed hard-migration-per-schema-change
shape as the core constraint to solve.]

Spirit `x0ja` (Constraint, High): [One consistent cryptographic basis spans
the entire version-control and backup system: blake3 for all content
addressing and criome BLS for signing and attesting history. No component
diverges in hash function or crypto.]

Load-bearing consequences for this daemon:

*Payload-blind: bytes stay bytes.* The mirror validates the blake3 digest
chain (sequence continuity, expected head, idempotent dedup, gap/fork
rejection) and stores opaque payload and artifact bytes. It never decodes a
component's record types.

*The mirror's own ledger is itself a versioned sema-engine store* (`0yx5`
dogfooding): the daemon's `sema.schema` declares its record families; every
registration, received entry, checkpoint artifact, and retention setting
lands in the mirror's own versioned commit log and mirror outbox.

*Ack after durable write.* sema-engine commits through redb are durable at
commit; the working reply leaves only after the persisting write transaction
committed. There is no second fsync layer (see `ARCHITECTURE.md`).

*Tailnet-trusted in this cut* (`rj9y`): TCP peers carry typed
`PeerIdentity::Tcp` and are working traffic with no per-request
authentication; criome BLS signing/attestation (`x0ja`) is deferred by
decision. The meta surface stays Unix-owner-only and is structurally
unreachable over TCP.

*Retention is stored, not enforced* — the typed placeholder from
`meta-signal-mirror`; enforcement is deferred by decision.
