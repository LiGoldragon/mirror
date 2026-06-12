# mirror

The payload-blind append-ingest mirror daemon: the sema version-control
remote. One daemon serves every component store — it validates sequence
continuity and expected head, deduplicates idempotently, persists into its
own versioned sema-engine store before acknowledging, and carries
registration and retention policy behind its owner-only meta signal.
See `ARCHITECTURE.md`.
