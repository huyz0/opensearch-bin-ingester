# 0076. A cross-AZ subscriber is served proxy, never inline

Status: accepted
Date: 2026-09-27
Requirements: FR-6, NFR-4, NFR-5
Research: docs/research/30-design-space/04-discovery-and-tailing.md (rule for when to inline), docs/research/30-design-space/05-az-topology-and-data-flow.md (takeaway), docs/research/00-problem/02-cost-model.md R12

## Context

`FetchPolicy` has two inline bounds: the 256 KiB (262,144 B) `inlineCapBytes`
when the bytes are already in the serving pod's AZ, and
`min(cap, crossAzCrossoverBytes)` when they are not. The crossover is cost
model R12's **~19.5 KiB** (one GET at $4 x 10^-7 over $0.02 per GB cross-AZ =
20,000 B): below it, shipping bytes across a zone costs less than the GET it
saves.

`SegmentServingPath.publishSegment` asked the policy ONCE per segment and
applied the answer to every subscriber. For a segment the pod holds it passes
`bytesInServingAz = true`, so the 256 KiB cap applied, and the subscriber's
zone was never consulted. M10's milestone review (finding F1) found the result:
a sub-cap segment was pushed INLINE, payload included, to a consumer in another
AZ. For that consumer cross-AZ bytes equal payload bytes, where NFR-5 allows
0.1%. M10.4's `fullNfr5IncludesProxyPayloads` avoided the case by sizing each
bulk past the cap. Low and medium rates are the common case, and they produce
sub-cap segments.

Measured by `CrossAzBytesIT#nfr5HoldsBelowTheInlineCap` before this decision:
three bulks of 110 records, each flushed as one 228,363 B segment, consumed by
an az-b consumer from the az-a writer, gave `inlinePush` = 685,597 B against
694,650 consumed producer bytes: **98.7% cross-AZ**, about 1,000x NFR-5.

The zone of a consumer is known where it matters: since M9.2 and M10.3 a poll
carries `az`, the consumer's own label, and `PollAnswer.countAnswer` already
attributes each answer's bytes by it.

## Decision

**The fetch mode is chosen per subscriber. A subscriber whose declared zone is
KNOWN and DIFFERS from the serving pod's zone is never served `inline`; it is
served `proxy`, as an event with no bytes, and its consumer fetches the segment
from its own zone's pod over `GET /seg` (ADR-0073).**

1. **The size policy is asked once per segment, the zone per target.**
   When the policy answers `inline`, the targets split: same-zone and
   zone-unknown subscribers are served `inline` exactly as before; cross-zone
   subscribers are served `proxy`. `proxy` and `direct` answers are unchanged:
   neither carries a payload across a zone. The split is a string comparison
   per consumer. It adds **no store request**: the writer serves the `proxy`
   group from nothing, so a cross-zone delivery costs the writer less than an
   inline one did.
2. **The `proxy` group's sinks are opened and completed empty**, as `direct`'s
   are. The HTTP subscription path drops a `proxy` push's bytes at the wire
   (`eventFor`), so bytes written there would only be charged to the
   sessions' shared 64 MiB queue budget and thrown away.
3. **An unknown zone keeps today's policy, on either side.**
   - A consumer that declares no zone may have no segment source. ADR-0073 made
     such a consumer fail loudly on a `proxy` event ("served `proxy` to a
     consumer with no segment source"). M9's own
     `crossAzDeliveryIsCountedAndStaysBelowTheProducerByteBudget` failed that way
     when a zoned consumer without a source was switched to `proxy`. Forcing
     `proxy` on every consumer built before M9.2 would break them. Its bytes stay
     counted as cross-AZ and reported as unattributed (`unknownPeerBytes`), so
     a report does not read them as local.
   - A hub built without its pod's zone (`new SubscriptionHub()`, every fixture)
     cannot call anyone remote, so it also keeps the size policy. Production
     builds it from `pod.az`, which `ServerProperties` refuses to default.
4. **What a consumer that declares a zone must provide:** a `SegmentSource`
   whose endpoint is **its own zone's ingester route** (`HttpSegmentSource` to
   the same-AZ service, sending the same `az`). Declaring a zone is now a
   promise that the consumer can fetch a `proxy` segment in that zone.
   ⚠️ **The shipped plugin keeps it only in a degenerate way.** It has ONE
   `binstore.ingester.endpoint`, used both to subscribe and for `/seg`
   (`NodeSubscriptions` builds its `HttpSegmentSource` from the same endpoint
   and zone). So either that endpoint is in the node's own zone, the serving
   pod's zone equals the declared one, and the split never fires; or it is
   not, and the plugin fetches `/seg` from that same cross-zone endpoint,
   which moves the payload from `inlinePush` to cross-AZ `proxyRead` without
   reducing it. Only a consumer that subscribes to one endpoint and fetches
   from its own zone's (as `CrossAzBytesIT` wires it) gets the saving. **The
   effect on a production plugin fleet is unmeasured.**
5. **The zone is the consumer's unverified self-report, and that is tolerable
   because it can only take a payload away.** A consumer claiming the pod's zone
   gets `inline`, which is what every consumer got before. A consumer claiming
   another zone gets an event without bytes and pays its own fetch. The label
   routes no request and authorises nothing. `SubscriptionService.AZ_PARAM`'s
   javadoc said it was "never compared for a placement decision"; that is now
   amended there.

## Alternatives considered

- **Keep one mode per segment, and apply `min(cap, crossover)` whenever any
  subscriber is cross-zone.** Rejected on two counts. First, it still inlines
  every segment under 19.5 KiB across the zone, which is 100% of that
  consumer's payload, 1,000x NFR-5. Second, it makes every same-zone subscriber
  fetch a 20-256 KiB segment it could have had for free.
- **Per target, but inline cross-zone below the ~19.5 KiB crossover** (research
  04's refined rule: "a trickle index's 8 KiB batch ... is worth inlining even
  cross-AZ"). Rejected for two reasons.
  - NFR-5 is a BYTE ratio, not a dollar comparison, and an inlined 8 KiB
    segment is 100% payload cross-AZ whatever it costs.
  - The crossover compares ONE transfer against ONE GET, and neither side of
    that holds here. An event is per stream: a node holding K shards of a
    segment has K sessions, and each session is handed the payload inline, so
    it crosses K times per node. The `proxy` side costs at most one GET per
    segment per serving pod (ADR-0073 §2), or none when the durable-segment
    signal has warmed the zone's ring owner (ADR-0066, NFR-4). Its fetch is
    per node, not per shard.

  So the real break-even is 19.5 KiB divided by the cross-zone copies, and that
  falls below any useful segment at the fan-out ADR-0004 prices (~400 streams a
  node).
- **Force `proxy` on a subscriber that declares no zone.** Rejected: see
  decision 3. It turns a counting gap into a delivery failure for every
  consumer without a segment source.
- **Take the consumer's zone from membership rather than from the poll.**
  Deferred, not rejected. Consumers are OpenSearch nodes, not ingesters, and no
  membership record names them. Closing it belongs with the subscription
  route's authentication (security.md's trust-domain work, as ADR-0073 defers).
  Until then the self-report is safe for the reason decision 5 gives.
- **Keep writing the payload into the `proxy` group's sinks**, as the
  segment-level `proxy` arm does for held bytes. Rejected: the HTTP path
  discards it (decision 2), and a sub-cap segment per cross-zone session would
  be queued against the shared budget for nothing.

## Consequences

- **NFR-5 holds below the inline cap.** Measured by
  `CrossAzBytesIT#nfr5HoldsBelowTheInlineCap`, same topology as before:
  - 499 B crossed the zone against 694,650 consumed producer bytes
    (**0.072%**), all of it `proxyRead` event frames;
  - `inlinePush` = 0;
  - all 685,089 payload bytes (three 228,363 B segments) were served by the
    az-b pod's `/seg` as `sameAzProxyRead`.
- **The floor is now the event, not the payload, and it is PER STREAM.**
  One event is sent per session, and a session is one stream (`id@RunKey`),
  so about 166 B cross per cross-zone-served stream per segment (499 B over 3
  one-stream segments here, 335 B over 2 in M10.4). 0.1% therefore holds only
  for segments above roughly **K x 166 KB**, where K is the number of
  cross-zone-served streams in the segment. Worked example: an 8 MiB segment
  holding 400 such streams sends ~66 KB of events, **0.79%, about 8x NFR-5**,
  with no payload crossing at all. A trickle index's few-KiB segment is over
  budget at K = 1. `CrossAzBytesIT` measures K = 1 only. This is a property of
  the per-stream event protocol and is NOT closed by this decision.
- **A cross-zone consumer pays one same-zone round trip per segment per node**
  for a sub-cap segment it used to receive inline. In the IT topology, which
  wires no durable-segment signal, the az-b pod also paid **one cold GET per
  segment** (the az-b pod counted 4 GETs over the run, for 3 fetches). That
  rate scales with segments and AZs, which NFR-4 allows, and not with
  consumers. Where the signal is wired, the ring owner's prefetch has already
  paid it.
- **A cross-zone consumer is served before every same-zone one** for an
  `inline` segment: two groups are two hand-offs, and the cross-zone group
  writes no bytes, so it delays the inline group by nothing but its sink
  opens. Within each group, ADR-0074's highest-lane-first order holds.
- **`proxy` now reaches a hub subscriber in two shapes.** With the held bytes
  (segment-level `proxy`, as before), or empty (zone split). Only the HTTP
  session declares a zone, and it drops the bytes at the wire in both cases.
  An in-process subscriber that overrides `Subscriber.az()` must not expect a
  `proxy` payload.
- M9's `crossAzDeliveryIsCountedAndStaysBelowTheProducerByteBudget` asserted
  `inlinePush > 0` to a cross-zone consumer, which pinned F1's defect as
  intended behaviour. It now places its second pod in the consumer's zone,
  gives the consumer that pod's route, and asserts `inlinePush` = 0.
