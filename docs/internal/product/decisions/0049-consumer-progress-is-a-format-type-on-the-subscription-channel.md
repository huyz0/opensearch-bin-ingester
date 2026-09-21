<!-- SPDX-License-Identifier: Apache-2.0 -->
# 0049. Consumer progress is a `format` type on the subscription channel, and `consumedUpTo` is exclusive

Status: accepted
Date: 2026-09-17
Requirements: FR-9, NFR-13, NFR-2
Implements: [ADR-0005](0005-no-consumer-offset-store.md) §"a position feed exists for exactly one purpose"
Research: docs/research/30-design-space/09-consumer-position-and-watermarks.md §5, §6

## Context

ADR-0005 decided **that** a position feed is piggybacked on the subscription and
**why** it may only extend retention. It did not decide **what shape those bytes
take**, and bytes crossing a process boundary are a wire format — so this is a
`wire-format-change` with its own record, exactly as ADR-0047 was for index
registration.

The module constraint is the same one ADR-0047 met: `client` and `plugin` may
not import `io.github.huyz0.os.biningester.binstore` (ADR-0023) and `plugin` may not import `ingest`
(architecture.md). The only module both ends already depend on is `format`.

M7 is the milestone where deletion starts, so unlike every earlier frame the
consequence of getting this one wrong is not a failed parse. It is a deleted
segment.

## Decision

### 1. `ConsumerProgress` is a record in `format`, with its own magic

`BPCG`, version 1, big-endian magic so an operator reading a hex dump can tell
one frame from another. It carries a non-empty list of entries, each
`(indexUuid, partition, shardCopy, consumedUpTo)`.

### 2. One frame per node, not per shard

The entries are batched across every partition the node hosts. The reporting
unit is the node because the node is what holds the subscription; a frame per
shard is eight times the channel at eight partitions for the same information,
and M6.7 already made this mistake once with registration and had it found by
review.

### 3. `consumedUpTo` is **exclusive**: the offset the copy would resume FROM

Every record below it is durably indexed; the record at it is not. That is what
`StreamPoller.BATCH_START` means, and it is stated on the record itself because
reading it as "the last offset consumed" is off by one **in the deleting
direction** on every stream — a defect no round-trip test and no golden file can
see.

### 4. Identity is `(indexUuid, partition, shardCopy)`, and the copy is carried

With `all_active = true` — which document replication requires — every shard
copy consumes the partition independently (ADR-0009, research 08 §5), so a
lagging replica in another AZ is a consumer. A frame keyed by partition alone
cannot express it, and the resulting `min()` would delete the replica's data
silently. `shardCopy` is the allocation id, which is also what distinguishes a
relocation from a restart (research 09 §6.4).

### 5. A frame with no entry is **refused**, and a duplicate copy in one frame is
**refused**

Silence already means something in this design: a copy that stops reporting
**freezes** at its last value rather than being dropped from the `min()`
(research 09 §6.3). An empty frame that still counted as a report would keep a
dead node's copies fresh forever, which is the one thing freshness exists to
notice — so a node hosting nothing sends nothing. Two positions for one copy in
one frame is undecidable, and the arbitrary answer is the dangerous one: taking
the later entry deletes the data the earlier one still needs.

### 6. A position that goes **backwards** between frames is believed

The reported pointer is the in-memory one, so a copy that restarts resumes from
its last Lucene commit and reports lower than it did before the crash. The later
frame is the true one. Monotonicity is a property of the retention **rule**,
never of this **feed** — a per-copy `max()` pins the value at the pre-crash
position and deletes exactly the records the restarted shard is about to read
again.

## Alternatives considered

- **A Kafka-style offset store in the ingester.** Rejected by ADR-0005 and not
  re-opened: the position OpenSearch commits atomically with the documents is
  the true one, and a second asynchronous copy loses data when the two disagree.
- **Polling `GetIngestionStateAction` from the ingester.** Rejected by ADR-0005:
  OpenSearch credentials in the ingester, and the same in-memory value anyway.
- **An inclusive `consumedUpTo`.** Rejected: it does not match
  `BATCH_START`, so every translation site would have to add or subtract one and
  the site that forgot would be invisible.
- **Keying entries by partition alone, dropping the copy.** Rejected — §4.
- **Accepting an empty frame as a heartbeat.** Rejected — §5. A heartbeat that
  carries no position is indistinguishable from a report that a copy is alive
  and has not moved, and the two must not be the same bits.
- **Carrying the node id as a field.** Rejected: nothing reads it. The
  allocation id already identifies the copy, and a field with no reader is a
  field nothing can be wrong about (the same argument ADR-0047 made for
  `replicationMode` and `lanes[]`).

## Consequences

- **No object-store request and no new endpoint.** The frame rides the
  authenticated subscription the plugin already holds: metadata-only, same-AZ,
  zero requests (NFR-2, cost rule R3). Nothing in the cost model moves.
- **The position is optimistic**, by up to one Lucene commit interval, and
  `safetyMargin` is how that is paid for rather than fixed. Sizing it is
  measurement M5, assigned to M7 (M7.14).
- **Nothing reads these bytes yet.** M7.2 builds the table that consumes them
  and M7.3 the reporter that sends them, which is the same staging ADR-0047 and
  M6.2 used: ship the read side before the write side, so a new frame never
  arrives at a build that cannot parse it.
- ⚠️ **No production `SubscriptionTransport` exists** (M5.6e, owned by M8), so
  the channel this frame travels on is a fixture until then, exactly as index
  registration's is.
