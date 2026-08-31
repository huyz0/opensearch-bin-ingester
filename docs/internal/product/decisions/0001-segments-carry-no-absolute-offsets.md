# 0001. Segments carry no absolute offsets

Status: accepted
Date: 2026-08-29
Requirements: FR-2, FR-3, FR-11, NFR-11
Research: docs/research/30-design-space/01-object-layout-and-format.md §2

## Context

Any pod may accept records for any partition (FR-1, stateless service). Something
must impose a per-partition total order. If a segment embedded the absolute
offsets of the records it contains, the offsets would have to be allocated
*before* the PUT.

## Decision

A segment records only per-run record **counts** and byte layout. The mapping
`(stream, [startOffset, startOffset+count)) -> (segmentKey, byteRange)` lives in
the commit log. **Ordering is assigned at commit, not at write.**

## Alternatives considered

- **Pre-allocate offset ranges before the PUT.** Rejected: a failed or abandoned
  PUT leaves a hole in the log that every consumer of that partition blocks on
  forever. Recovering from holes requires a timeout-and-fill protocol that is
  strictly more complex than assigning at commit.
- **Partition leadership — route each partition's writes to one pod.** Rejected:
  producers would cross AZs to reach the owner, at $0.02/GB. At 100 MiB/s that is
  ~$340/day, which is most of the cost advantage the project exists to capture.
  (AutoMQ can do this because it *is* the broker; we sit behind a load balancer.)

## Consequences

- Makes leaderless, coordination-free writes possible — the reason the write path
  is cheap and pods are stateless.
- Makes compaction a pure metadata swap: moving bytes never changes an offset,
  which is what keeps Lucene's `_offset` field valid across compaction.
- Costs a commit round trip before a record is visible (~20–100 ms).
- The same decomposition WarpStream, AutoMQ and Apache Kafka's KIP-1150 reached
  independently.
