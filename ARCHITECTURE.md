# mirror — Architecture

`mirror` is the daemon of the mirror triad (`mirror` runtime,
`signal-mirror` ordinary contract, `meta-signal-mirror` meta policy
contract): the payload-blind sema version-control remote. It cites
`primary/skills/component-triad.md` and states only the component-specific
shape below.

## Runtime triad

The daemon is schema-derived on the emitted daemon runtime. The working
tier's `Input`/`Output` come from the dependency contract `signal-mirror`
(single source of the wire types); the meta tier's from
`meta-signal-mirror`; the daemon-local plane schemas declare Nexus and SEMA.

| Plane | Schema | Role |
|---|---|---|
| Signal | `signal-mirror` + `meta-signal-mirror` (dependency `WireContract`s) | the wire surfaces; the emitted spine decodes/encodes |
| Nexus | `schema/nexus.schema` | the feature catalog: `AppendDecision` (expected-head validation, idempotent dedup by entry digest, gap/fork rejection) and `CheckpointDecision` (registration + coverage monotonicity) |
| SEMA | `schema/sema.schema` | the mirror's OWN versioned store: declared record families (`StoredHead`, `ReceivedEntry`, `StoredCheckpoint`, `RetentionSetting`) emit the `RecordFamily` surface and `VersioningPolicy` (versioned store name `mirror:sema`, pinned by a witness) |

`MirrorEngine` (src/engine.rs) is the data-bearing noun implementing the
generated `NexusEngine` and `SemaEngine`; `Store` (src/store.rs) owns the
sema-engine database; the decisions are methods on the schema-emitted
checked nouns (src/decision.rs). One working request flows
Signal -> Nexus decide -> SEMA check (read) -> Nexus decision ->
SEMA persist (write) -> Signal reply.

## Listeners — generated Unix tiers + hand-wired tailnet TCP

```text
Unix working socket  ──┐  (generated AsyncMultiListenerDaemon, signal-mirror)
Unix meta socket     ──┤  (generated, owner-only 0o600, meta-signal-mirror)
tailnet TCP ingress  ──┘  (hand-wired triad_runtime::TcpListenerDaemon)
            all three -> ActorRef<MirrorService> -> MirrorEngine -> Store
```

`MirrorService` (src/service.rs) is the kameo actor owning the engine — the
ONE component runtime both transports share. The generated daemon's
`ComponentDaemon::Engine` is a cloneable `ServiceLink` into its mailbox;
the service's own `on_start` binds `triad_runtime::TcpListenerDaemon`
around its own `ActorRef` (the `TailnetIngress` connection runtime), using
the same length-prefixed frame codec and the same `signal-mirror` contract
as the Unix working tier. Every request from every transport serialises
through one mailbox: the single writer is structural.

Hand-wiring is the honest scope: schema-rust-next does not emit TCP
listener tiers yet, and the emitted `DaemonBinder` owns its engine actor
privately, so a second transport cannot share it. Generalising this into
emission (a TCP listener tier on `NexusDaemonShape` plus a shared-engine
seam) is a named follow-up.

Trust shape (Spirit `rj9y`, this cut): TCP peers carry typed
`PeerIdentity::Tcp` and are tailnet-trusted WORKING traffic — the bind
address is the deployment's trust boundary. The TCP ingress decodes only
the ordinary contract; meta orders are structurally impossible over TCP.
Ssh-forwarded sockets are rejected as a transport shape (no third peer
identity exists).

## fsync-then-ack — how the ack maps onto the engine commit boundary

sema-engine commits through redb, which is durable at commit (shadow
paging + fsync inside `commit()`). The mirror does not invent a second
fsync layer: `Store::persist_suffix` / `persist_checkpoint` return only
after the underlying write transaction committed, and the Nexus reply
(`Appended` / `CheckpointPublished`) is composed strictly after that
return — the ack IS ack-after-durable-write.

A persisted suffix is two transactions (entry rows, then the head row).
A crash between them leaves entry rows ahead of the head row. The window
self-heals on the shipper's idempotent re-send because the append
decision dedups against the LOADED KNOWN ROWS, never the head sequence:
re-sent entries that match surviving rows (digest-verified — divergence
is `DigestMismatch`) drop out of the novel remainder, and when the
remainder is empty but the suffix end lies above the head,
`Store::persist_suffix` skips the entry transaction (sema-engine rejects
empty commits) and re-advances only the head. Witnessed by
`crash_window_resend_re_advances_the_head_and_the_store_stays_live`
(tests/daemon_logic.rs), which commits the entry transaction through the
public `Store::commit_entry_rows` seam without the head advance.
Single-transaction multi-table persist is a sema-engine follow-up.

## The append decision

`CheckedAppend::into_decision` (declared as `AppendDecision` in
`schema/nexus.schema`):

- **Unknown store** — registration is meta authority; unregistered names
  are refused.
- **Expected head** — names the entry just before the suffix; must match
  the stored digest at that sequence (absent only for a genesis suffix).
  The stored rows are the authority, so a suffix may legitimately
  continue from a crash-window row above the head.
- **Idempotent dedup** — every entry the ledger already holds (at or
  below the head, or in the crash window above it) must match the stored
  digest exactly; the novel remainder is what has no stored row. A
  fully-duplicate suffix at or below the head acknowledges with the same
  head and writes nothing; a duplicate suffix ending above the head
  re-advances only the head; a partially-novel suffix persists only the
  remainder.
- **Gap / fork** — non-consecutive sequences are `SequenceGap`; digest
  chain breaks are `HeadForked`; divergent re-sends are `DigestMismatch`.
  Rejections carry the mirror's current head for shipper resync.

## The meta tier

Meta orders (owner-only Unix socket) run through the DECLARED SEMA plane
verbs, not around them: `LoadRegistry` backs the registration pre-checks
and the registry observation, and `RegisterStore` / `RetireStore` /
`PersistRetention` are SEMA writes. The schema is the one source of
truth for every plane operation; meta-borne SEMA traffic carries its own
origin route so traces tell the planes apart.

Registration semantics:

- **Separator-bearing names are refused typed** (`StoreNameInvalid`):
  entry and checkpoint keys are `<store>/<sequence>`, so a name carrying
  `/` would collide with the ordering suffix.
- **Re-registration RESUMES a surviving chain.** Retirement retracts
  only the head row; received entries and checkpoints remain until
  retention enforcement lands (deferred by decision). Registering a name
  with surviving history restores the head from the highest surviving
  entry row — the only outcome consistent with an append-only ledger
  that cannot delete history yet: a genesis restart would wedge the name
  (every persist would re-assert surviving rows and fault), and refusal
  would wedge it until retention enforcement exists. Witnessed by
  `retire_then_reregister_resumes_the_surviving_chain`.

## Binaries

| Binary | Role |
|---|---|
| `mirror-daemon` | the daemon; exactly one argument: a binary rkyv `MirrorDaemonConfiguration` file (never parses NOTA) |
| `mirror` | thin working CLI: one NOTA argument over `MIRROR_SOCKET` |
| `meta-mirror` | thin meta CLI: one NOTA argument over `MIRROR_META_SOCKET` |
| `mirror-write-configuration` | deploy text edge: NOTA `ConfigurationWrite` -> binary startup file |

## Witnesses

| Test | Proves |
|---|---|
| `tests/daemon_logic.rs` | accept + head advance, idempotent duplicate (no new log entries), partial-duplicate remainder, the crash-window re-send healing (head-only re-advance; store stays live), retire-then-re-register resuming the surviving chain, typed gap/fork/digest-mismatch/empty/name-invalid rejections, the pinned `mirror:sema` store name, restore bundle shape, and the mirror's own ledger being versioned (dogfooding) |
| `tests/end_to_end_arc.rs` | the whole arc across two engines: component outbox -> real loopback TCP frames -> running mirror -> `ServerCommitted` -> fresh store restores via `ImportSession` and reads identical records (Spirit `29pb`, first cut) |

## Not owned

Component record types (payload-blind), retention ENFORCEMENT (stored
placeholder only; deferred), BLS attestation (deferred), and the
production component-side shipper actor (the test fixture carries the
first shipper; a production shipper is a named follow-up).
