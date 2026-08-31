# 0002. Object-store CAS is the only coordination substrate

Status: accepted
Date: 2026-08-29
Requirements: FR-11, NFR-9, NFR-11
Research: docs/research/30-design-space/03-metadata-and-cas.md

## Context

Ordering needs a sequencer, and a sequencer needs durable state and safe
failover. Every comparable system reaches for an external store: WarpStream uses
DynamoDB/Spanner, AutoMQ uses KRaft, KIP-1150 adds a Batch Coordinator, Quickwit
ends up at PostgreSQL.

## Decision

The object store is the only dependency. Sequencer leadership is a **CAS lease
with an epoch counter**, the epoch is **in the object path**, and the commit log
is a **write-once** chain claimed with `If-None-Match: *`. Failover seals the old
chain by racing for the next sequence number.

Safety is therefore **independent of failure detection**: two processes that both
believe they lead cannot both win a sequence number, so an undetected zombie is
wasteful, not incorrect.

## Alternatives considered

- **A hosted metadata store (DynamoDB/Spanner).** Rejected: it is the dependency
  the project exists to avoid, and it reintroduces a per-operation cost that
  scales with partitions.
- **Read-modify-write CAS on a single shared manifest (`If-Match`).** Rejected on
  Quickwit's published experience — a file metastore on object storage *"does not
  handle concurrent writers well"* — and on mechanics: every commit becomes
  GET+PUT of a manifest that grows with the retention window, and under
  contention throughput *decreases* with concurrency.
- **A Raft group among the ingester nodes.** Rejected: it makes the pods stateful,
  which forfeits scale-to-zero and trivial rescheduling.

## Consequences

- No external dependency, and failover costs no coordination service.
- ⚠️ **Highest-risk component in the system.** The seal protocol needs
  deterministic simulation against invariants I1–I4; it is not proven.
- Requires conditional writes from every backend, expressed differently per
  provider (S3/Azure ETag, GCS generation) — hence the conformance suite.
- Bounds commit throughput to one sequencer's rate per slot.
