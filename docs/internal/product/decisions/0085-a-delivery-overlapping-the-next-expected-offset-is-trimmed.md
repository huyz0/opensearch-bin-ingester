# 0085. A delivery overlapping the next expected offset is trimmed, not refused

Status: accepted
Date: 2026-10-13
Requirements: FR-10, FR-17
Research: docs/research/20-opensearch/02-poller-semantics-and-cost.md (an exception from `readNext` pauses the shard)

## Context

A consumer tracks each stream's next expected offset (`GapTracker`). Until now
every delivery it was handed fell into one of four cases against that offset.

| Delivery | What happened |
|---|---|
| wholly below | dropped as a duplicate |
| at it | decoded |
| above it | a gap |
| starting below it and ending above it | `IllegalStateException` |

The fourth case was rare. Every delivery was one run of one committed
segment, and the chain's runs of a stream never overlap. It was reachable only
when a gap repair's replay committed part of a live delivery still queued
behind it, which `GapTracker` already anticipated and which threw too. The
trim below covers that case as well.

Fast mode makes it happen. ADR-0081 §6 pushes each exposed entry to the
interested pods **before** the upload, at its offsets. §7 then uploads every
exposed entry of a stream as **one** run from the chain's next offset
(`FastUploader`). A subscriber pushed entries `[10, 15)` and `[15, 20)` is then
delivered the committed run `[10, 30)`. That run starts below its expected
offset of 20 and ends above it.

The throw is the worst answer available. It escapes `readNext`, and an
exception there pauses the shard until an operator resumes it (research doc 02
§6, the reason `GapTracker` reports gaps rather than throwing them). One upload
after any push would pause every fast shard that had a pushed entry.

## Decision

A delivery starting below the next expected offset and ending above it is
**decoded and trimmed**. Its records below the expected offset are dropped as
the duplicates they are, and those from the expected offset on are handed on.
The expected offset becomes the delivery's end.

- The offsets kept are decided under the same lock as the commit
  (`GapTracker.Commit.keepFrom`), not when the delivery is decoded. A catch-up
  commit racing between the two moves the expected offset, and trimming by the
  earlier reading would hand on a record twice or drop one.
- "Covered" counts only the newly kept offsets, as a replay's already does
  (M13.25e).
- Nothing else changes: a delivery wholly below is still dropped, a gap is
  still reported, and a replay still advances to the maximum of the two ends.

## Alternatives considered

- **Upload one run per pushed entry.** `SegmentWriter` keeps one run per stream
  in a segment (ADR-0074, ADR-0081), so a stream's entries cannot be separate runs of one
  segment. One segment per entry would multiply the upload's PUTs and deltas
  by the entries per stream per upload (a stream at `B / 2` holds up to
  `B / 2` single-record entries). That breaks non-negotiable 6: requests would
  scale with records.
- **The pod trims the committed run before delivering it.** A non-leader pod
  never delivers the committed fast segment: its subscribers read it from the
  chain themselves (`TierTwoChainPoller`, catch-up). Only the consumer sees
  both copies, so only the consumer can trim.
- **Refuse the push instead, and deliver fast entries from the chain only.**
  That is ADR-0081 without §6, which loses the seconds of latency publication
  exists for.
- **Keep the throw and report the overlap as a gap.** A gap triggers a repair
  that replays offsets already decoded. That is a whole catch-up read per
  upload per stream, for records the consumer already holds.

## Consequences

- A fast stream's committed run after its pushes costs a decode of the whole
  run, of which the overlapped prefix is discarded. The run is at most `B`
  records, and this is consumer CPU, not a request.
- An overlap is no longer evidence of a defect. A bug that produced
  overlapping committed runs would now be trimmed silently. The chain's own
  commit checks (`commitAt` refuses a run not at the chain's next offset)
  remain what rules that out.
- Fast mode needs a consumer with this change. A plugin without it pauses its
  shard on the first upload after a push. Fast mode has not shipped, and the
  plugin and the ingester are built together, so no deployed plugin lacks it.
