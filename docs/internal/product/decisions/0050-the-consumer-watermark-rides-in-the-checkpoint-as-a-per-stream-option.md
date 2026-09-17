<!-- SPDX-License-Identifier: Apache-2.0 -->
# 0050. The consumer watermark rides in the checkpoint, as a per-stream option

Status: accepted
Date: 2026-09-17
Requirements: FR-9, NFR-13
Amends: [ADR-0033](0033-a-checkpoint-carries-offsets-and-flush-seqs-never-the-segment-index.md)
Research: docs/research/30-design-space/09-consumer-position-and-watermarks.md §8

## Context

M7's retention rule needs, per stream, how far the slowest consumer copy has
got. The watermark table (M7.2) holds that in memory on whichever ingester node
received the reports, and memory does not survive a sequencer failover — so GC
would restart from "nothing is known", keep everything, and only recover once
every copy had reported again.

Research 09 §8 already answers where it goes: the **existing checkpoint**, which
is written on a schedule that does not change, already carries `nextOffset` and
`oldestRetainedOffset` per stream, and is found through a pointer rather than by
listing (ADR-0034). No new object, no new CAS, no new request.

⚠️ ADR-0033 says a checkpoint carries offsets and flush seqs and **never the
segment index**. This adds a third offset per stream, not an index, so it is
within that rule rather than an exception to it — but it is the first addition
since, and the reason the rule exists (a checkpoint that grows with history) is
worth re-checking against: one varint per stream is bounded by the stream count,
exactly as the two offsets beside it are.

## Decision

### 1. `StreamOffsets` gains `consumerWatermark`, as an `OptionalLong`

Absent means **nobody has reported**, which is not the same fact as **nobody has
read**. A zero written for an absent watermark is indistinguishable from a real
zero to the next build that reads it, and the retention rule's answer to the two
differs by everything: unknown keeps the stream, a real zero also keeps it, but
an unknown *read as* zero becomes "consumed up to 0" the moment someone tidies
the sentinel.

### 2. Version 2, chosen by CONTENT, and a **superset** of version 1

A checkpoint whose streams carry no watermark encodes byte-for-byte as it did
before, so peers on the previous release keep reading what this one writes
through a rolling deploy. A v2 object carries the ADR-0036 pod attribution as
well — the per-slot flag is written even when no pod has a pointer, because a
reader could not otherwise tell which of two pod layouts follows a v2 header.

### 3. The watermark is a **per-stream flag**, not a version-wide one

Mixed is the normal state: a stream whose consumers have reported sits beside
one whose have not. A version-wide flag would force the writer to invent a
watermark for the second — the same argument ADR-0036 made for the per-slot pod
flag, and for the same reason.

### 4. It may sit **below** `oldestRetainedOffset`, and that state is data loss

A copy behind the oldest retained offset has had its records deleted: the
`maxRetention` ceiling, which alarms. A format that refused the state would make
the incident unrecordable and the alarm unprovable.

### 5. It may **equal** `nextOffset`, because the position is exclusive

ADR-0049 fixes `consumedUpTo` as the offset a copy would resume FROM, so a copy
that has read everything committed reports exactly `nextOffset`. Refusing
equality would refuse the fully-caught-up consumer, which is the normal state of
a healthy cluster. Greater than `nextOffset` is refused: a copy cannot have
consumed past the offsets that exist, and one claiming to would delete every
segment of the stream on the next pass.

## Alternatives considered

- **A separate watermark object per stream.** Rejected: it is a request per
  stream to write and another to read, which is exactly the scaling
  non-negotiable 6 forbids.
- **One watermark object for the whole fleet.** Rejected: a second CAS chain
  with its own failover semantics, to carry a number that the checkpoint is
  already being written to carry.
- **Keeping it only in memory.** Rejected: a failover would restart GC from
  "nothing known". That direction is SAFE — it keeps — so this is a cost
  decision rather than a correctness one, and the cost is storage that nobody
  can collect until every copy has reported again.
- **A `long` with `-1` for absent.** Rejected: the sentinel is readable, and
  `Checkpoint` is decoded by builds that will not have read this ADR. The same
  argument the M7.2 review made about `Watermark.position()`.
- **A version-wide watermark flag.** Rejected — §3.

## Consequences

- **The checkpoint grows by one varint per stream**, plus one flag byte, on an
  object already bounded by ADR-0033 and already written on the same schedule.
  No new object, no new CAS, no new request (cost rule R6). At 1,600 streams
  that is a few kilobytes.
- **v0 and v1 objects keep decoding**, with their streams coming back as
  "nobody has reported" rather than as zero — pinned by
  `GoldenCheckpointV2Test`.
- ⚠️ **THE ROLLING-DEPLOY PROPERTY HOLDS ONLY WHILE NOTHING SUPPLIES A
  WATERMARK.** A checkpoint carrying none encodes as it always did, so today a
  new build and an old one interoperate in both directions. The commit that
  wires a supplier makes EVERY checkpoint v2, and a peer still on the previous
  release then fails with `unsupported checkpoint version: 2` — which for a
  checkpoint stops recovery rather than degrading it, because the checkpoint is
  what bounds the chain walk. So that commit owes a read-side release first:
  ship the version-2 reader everywhere, then start writing it. The row that
  wires the supplier carries this warning.
- ⚠️ **Nothing supplies a watermark yet.** `CheckpointWriter` has no watermark
  table to read: the table lives in `ingest` and the writer in `sequencer`, and
  joining them is a seam that does not exist. The field is carried for the
  reason `oldestRetainedOffset` was carried from M4.8a — adding it later is a
  format change, and this is the commit where the change costs nothing.
