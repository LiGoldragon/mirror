# mirror

## Status: deprecated

The living marked Mirror deprecated on 2026-09-10. Retain this repository as
historical evidence, not an active development or stack migration target.
Do not add new consumers or resume the unfinished migration without new
explicit direction. The documentation below describes historical behavior
and does not establish active status.

The payload-blind append-ingest mirror daemon: the sema version-control
remote. One daemon serves every component store — it validates sequence
continuity and expected head, deduplicates idempotently, persists into its
own versioned sema-engine store before acknowledging, and carries
registration and retention policy behind its owner-only meta signal.
See `ARCHITECTURE.md`.
