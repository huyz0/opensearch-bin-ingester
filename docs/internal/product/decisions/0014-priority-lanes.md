# 0014. Priority lanes are scheduling priority, not offset reordering

Status: accepted
Date: 2026-08-30
Requirements: FR-18
Research: docs/research/20-opensearch/01-pull-based-ingestion-spi.md §1

## Context

Some ingestion should reach the index sooner than the rest. The request: a small
number of priority lanes (2–3, fewer than 10), expressible in a single byte.

## Decision

**A signed `i8 lane` in the record frame — a bucket, not a rank.**

```
  -2  bulk        deep background: backfill, reindex, replay
  -1  low         yield to live traffic
   0  standard    the default; an unset field is 0
  +1  high        interactive, user-visible writes
  +2  urgent      reserved; use sparingly
```

**Zero is the default and needs no configuration**, so a producer that sets
nothing lands in the standard bucket. Signed numbering extends in **both**
directions without renumbering: adding a deeper background bucket is `-3`, not a
migration. The convention is deliberately the one people already know from `nice`
and Kubernetes PriorityClass.

The active set is configured per index and capped (hard cap 8 buckets). One signed
byte leaves room to widen without a format change.

⚠️ **Negative buckets are not a courtesy, they are cheaper.** A `-1` lane gets a
*longer* `maxStreamLatency` and a *larger* `minRunBytes`, so its runs are fuller,
its compression better and its segments fewer. Bulk backfill that yields to live
traffic also costs less per MiB than standard traffic — the incentives line up.

⚠️ **A lane changes *scheduling*, never *order*.** A partition has one monotonic
offset space and one pointer (`IngestionShardPointer` is a single `long`, indexed
into Lucene as `_offset` and range-queried). Reordering committed records would
break the pointer contract and every resume path.

Priority is therefore expressed as **preferential progress through the pipeline**:

| Stage | What a high lane gets |
|---|---|
| Admission | its own token bucket; not queued behind a bulk burst |
| Buffering | a shorter `maxStreamLatency` — e.g. lane 0 at 250 ms, lane 2 at 5 s |
| Flush | can trigger a segment flush on its own |
| Segment layout | runs placed first, so they are in the early bytes |
| Commit | can force a commit-batch flush rather than waiting out the interval |
| Push | events emitted before lower lanes |
| Client queue | drained first among what has arrived |

Negative buckets get the mirror image: longer `maxStreamLatency`, larger
`minRunBytes`, no flush trigger of their own, runs placed last, and a smaller
share at admission.

Because a high-lane record moves through the pipeline faster, it *reaches commit
sooner* and therefore **naturally receives a lower offset**. No reordering is
needed, and nothing in the OpenSearch contract is bent.

## Why lanes work where a per-record ack mode does not

⚠️ The general principle, and it explains both this ADR and
[ADR-0013](0013-fast-mode-wal-and-quorum.md):

> **Priority may be applied before the offset is assigned. Anything applied after
> it is subject to the log's total order.**

A lane reorders records **in the buffer**, where reordering is free — a `+1`
record overtakes a `-1` record *before* either has an offset, so it simply
receives the lower one. A per-record ack mode changes when *an already-assigned*
offset becomes visible, which the consumer's in-order processing then defeats.
Same intent, opposite side of the ordering boundary.

## Anti-starvation

Strict priority starves. Lane scheduling is **weighted fair share with a floor**,
not strict precedence: every active bucket has a guaranteed minimum share of
admission and buffer capacity, and a **hard latency ceiling** past which its
records flush regardless of what higher buckets are doing.

⚠️ Without the ceiling, a sustained `+1` load makes `-1` data invisible
indefinitely, and the symptom — data that is accepted, acked, and never appears —
is far worse than backpressure.

## ⚠️ Mutations to one document must not cross lanes

Because lanes reorder before offset assignment, a `+1` record can receive a lower
offset than a `-1` record that arrived **earlier**. For independent documents that
is the whole point. For two mutations of the **same** document it is a correctness
hazard: a delete in `-1` and an index in `+1` could apply in the wrong order.

Two safe patterns, at least one of which is required:

1. **Same document ⇒ same lane.** The natural choice when lanes map to workloads.
2. **Carry an external `_version`.** OpenSearch's external versioning rejects an
   older version, so out-of-order application is safe. This is the existing
   mechanism (`FieldMappingIngestionMessageMapper`'s `version_field`) and is the
   more robust of the two.

The ingester cannot enforce this — it does not parse documents — so it belongs in
producer documentation and in the consumer library's API shape.

## Alternatives considered

- **Reorder within the consumer** so high-lane records are returned first.
  Rejected: `readNext` returns records the poller indexes in order, and the
  pointer advances monotonically. Returning offset 105 before 103 would make the
  resume pointer meaningless and break `newRangeQueryGreaterThan`.
- **A separate source partition per lane**, giving each lane its own offset space.
  This is the *correct* design and is **not available**: it needs OpenSearch to
  wire the `MODULO` strategy (`SourcePartitionAssignment.assignSourcePartitions`
  has no callers in 3.8.0) **and** a composite pointer covering several partitions,
  which `DefaultStreamPoller`'s single `batchStartPointer` does not support.
  Recorded as the upgrade path if OpenSearch ships both.
- **Reorder within a commit batch**, so a high lane gets a lower offset inside the
  same delta. Rejected as insufficient on its own — it only reorders within one
  flush — but it is a free bonus of "runs placed first" and is retained.
- **Unbounded lanes.** Rejected: every lane is a scheduling class through seven
  stages, and the cost of the mechanism is in the number of classes, not the byte
  width.
- **Unsigned `u8`, 0 = highest** (this ADR's first draft). Rejected: it makes the
  default value the *most privileged*, so an unset field silently becomes top
  priority; it offers no way to express "below standard" without renumbering; and
  it inverts the convention every operator already knows.

## Consequences

- Lane is a **first-class field in the wire frame and the segment run entry**, so
  adding it later would be a format change ([`wire-format-change`](../../../../.agents/skills/wire-format-change/SKILL.md)). It goes in now
  even though the scheduling behaviour can be phased.
- **The honest limitation to document:** a lane accelerates *reaching* commit; it
  cannot jump ahead of something already committed. If a bulk load committed
  10,000 records at offsets 1..10,000, a high-lane record arriving afterwards is
  10,001 and the consumer processes it last. Priority is not preemption.
- `maxStreamLatency` becomes per-lane rather than per-index — a natural fit with
  the trickle policy, and the single most effective lane mechanism.
- Lanes interact with fast mode (ADR-0013), and the two now sit at different
  granularities on purpose: **ack mode is per index** (it cannot be per record),
  **lane is per record** (it must be, to be useful). They are not the same control
  and should not be merged.
- ⚠️ If OpenSearch ever ships `MODULO` plus a composite pointer, **lane-per-source-
  partition would give each bucket its own offset space** — which would in turn
  make a per-lane ack mode sound, since the head-of-line blocking that rules it
  out today is an artefact of sharing one ordered log. Worth revisiting together.
