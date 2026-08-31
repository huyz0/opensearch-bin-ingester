# 0009. Segment replication with primary-only ingest is the default

Status: accepted
Date: 2026-08-30
Requirements: NFR-4, NFR-10
Research: docs/research/30-design-space/08-failure-domains-and-resilience.md §5 (Q23)

## Context

`index.ingestion_source.all_active` decides whether replicas consume the stream
themselves. OpenSearch 3.8.0 enforces a strict XOR in
`IndexMetadata.INGESTION_SOURCE_ALL_ACTIVE_INGESTION_SETTING`'s validator:

- `replication_type=SEGMENT` with `all_active=true` → **rejected**
- `replication_type=DOCUMENT` with `all_active=false` → **rejected**

So there are exactly two legal configurations. The setting **defaults to
`false`** and is `Property.Final` — chosen at index creation, never changed.

## Decision

**Default to `SEGMENT` replication with `all_active=false`**: the primary
consumes, replicas receive segments. Support `DOCUMENT` + `all_active=true` as a
documented option.

## Alternatives considered

- **`DOCUMENT` + `all_active=true` as the default.** It gives instant failover,
  since every copy is already current, and avoids OpenSearch's own segment
  replication traffic. Rejected as the default because it multiplies **our**
  serving bandwidth by the copy count — at 100 MiB/s and two copies, 33 MiB/s out
  per pod instead of 17 — and because document replication is the older model and
  not the OpenSearch default. An earlier version of the research leaned this way;
  the validator and the default settle it.

## Consequences

- **Exactly one consumer per partition at a time**, which is the assumption that
  makes compaction deferrable and the read path a single-consumer problem.
- Serving bandwidth is 1×, not copy-count×.
- Failover is slower: a promoted replica re-ingests the tail from its recovered
  `batch_start`. Bounded by the Lucene commit interval, and safe because replay
  is at-least-once and offsets are stable (ADR-0001).
- ⚠️ It simplifies but does not remove the GC-watermark rule: with one consumer
  the `min()` across copies is trivial, but the consumer's **identity changes on
  promotion** and the new primary may be behind the old one. "Silence freezes the
  watermark" (ADR-0005) still holds and is now the load-bearing half.
- OpenSearch performs its own segment replication, which with a remote store is
  additional object-store traffic **outside our cost model**. Note it when
  quoting a total cost of ownership.
- `all_active` is `Final`: the choice is made once, at index creation, and the
  registration API must record it.
