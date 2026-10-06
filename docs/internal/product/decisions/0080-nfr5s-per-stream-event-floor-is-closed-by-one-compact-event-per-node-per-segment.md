# 0080. NFR-5's per-stream event floor is closed by one compact event per node per segment

Status: accepted
Date: 2026-09-28
Requirements: NFR-5, NFR-4
Research: docs/research/30-design-space/05-az-topology-and-data-flow.md, docs/research/30-design-space/04-discovery-and-tailing.md

## Context

ADR-0076 took the PAYLOAD off the cross-zone path: a cross-zone consumer is
told `proxy` with no bytes and fetches from its own zone's ingester. What
still crosses is the EVENT, and it is per stream: one `SubscriptionEvent` per
session, a session is one stream (`id@RunKey`), and each event carries the
session string, the index UUID, the partition, the segment key, offsets and
the `via` name -- about 166 B. K cross-zone-served streams in a segment cost
K x ~166 B of cross-zone bytes, so NFR-5's 0.1% holds only for segments
carrying more than about K x 166 KB. An 8 MiB segment serving 400 such
streams sends ~66 KB of events: 0.79%, about 8x NFR-5. `CrossAzBytesIT`
measured K = 1 only (M10.34).

M10.34 named three options:

1. Batch events per session across segments.
2. A compact event encoding.
3. Restate NFR-5 per workload shape.

M10.34 measured K > 1 on RustFS (`CrossAzBytesIT#theEventFloorIsPerCrossZoneStreamAtKOverOne`):
four partitions' bulks written together (~230 KB each, two rounds), four az-b
consumers each on its own session. The writer packed the streams two to a
segment: 4 segments, 8 (segment, cross-zone stream) deliveries, and
**1,334 B crossed the zone -- 166 B per delivery, exactly the K = 1 figure**,
all of it `proxyRead` event frames (`inlinePush` = 0), against 1,848,880
consumed producer bytes (0.072%). The term is linear in K, as ADR-0076 stated.

## Decision

1. **NFR-5 is NOT restated.** The per-stream term is a property of the
   protocol, not of the workload, and restating the requirement around it
   would make the protocol's cost the requirement's definition.
2. **The floor is closed by options 1 and 2 together, at NODE scope: one event
   per (node, segment) that lists the node's runs in it compactly.** The
   segment key, the index UUID and the session identity travel once; each run
   is a partition, a first offset and a record count, delta- and
   varint-encoded -- about 8 B a run. A segment serving 400 cross-zone
   streams on one node then sends ~166 B + 400 x 8 B ~ 3.4 KB, 0.04% of
   8 MiB. Batching per SESSION across segments (option 1 alone) does not help:
   K is streams per segment, not segments per stream. A compact per-stream
   event (option 2 alone) divides the term by ~3 and leaves 400 streams at
   ~0.3%.
3. **It is a wire-format change and is NOT implemented by M11.** It needs a
   node-scoped session on the subscription channel (the plugin already holds
   one subscriber per node, `NodeSubscriptions`, M5.62, but its transport
   still opens one session per stream), a new event version, and every reader,
   writer, fake and golden file with it (non-negotiable 8,
   `wire-format-change`). The roadmap's deferred table carries it, milestone
   not yet assigned.
4. **Until then the bound is the one measured here**: at most 200 B of
   cross-zone bytes per (segment, cross-zone-served stream), so NFR-5 holds
   for segments carrying more than K x 200 KB for their K cross-zone
   streams, and `CrossAzBytesIT` asserts both at K > 1 (166 B measured,
   200 B asserted, so a regression in the event size is caught).
   ⚠️ **Over the data path** (amended by M13.66): the cross-zone bytes the
   bound counts are NFR-5's numerator -- every transport but the fast-mode
   control frames, which have their own per-pod, per-term budget (ADR-0081
   §12).

## Alternatives considered

- **Restate NFR-5 per workload shape** (option 3). Rejected, decision 1. It
  stays the fallback if the node-scoped event proves not to fit the channel,
  and would then be its own ADR with the number.
- **Batch per session across segments** (option 1 alone). Rejected: the
  multiplier is streams per segment.
- **Compact per-stream encoding** (option 2 alone). Rejected as the whole
  answer: a factor of ~3 on a term that is ~8x over at 400 streams.
- **Inline small segments for cross-zone consumers again.** Rejected by
  ADR-0076; the payload is what the bytes were.

## Consequences

- NFR-5 is met today only above K x 200 KB per segment; a trickle
  index with cross-zone consumers is over budget on its events, as ADR-0076
  said. The deployment-level number to watch is `crossAzBytes` against
  ingested bytes, which the ingester already exports.
- The fix removes a per-stream term from a per-segment path, which is the
  shape non-negotiable 6 asks of request rates, applied to bytes.
