# M10 — Proxy segment serving and the full NFR-5 proof

## Completion condition

From [roadmap.md](../../roadmap.md), as amended by M10.0 on the user's decision
of 2026-09-26: *"The same-AZ proxy segment route exists end to end — an
ingester node streams a segment to a consumer that was pushed a `proxy`
event, and the consumer decodes it — and NFR-5 is measured with that route's
payload bytes counted, on RustFS, across three AZ labels. FR-21's refusing
half has an owning milestone by ADR. The M9 review harvest is closed or
dispositioned row by row."*

⚠️ **THE ROUTE IS NOT MERELY UNMEASURED TODAY, IT IS BROKEN.** M9 recorded the
proxy payload term as NOT-RUN. Reading the tree for this spec found the
stronger fact: over HTTP a `proxy` event carries no bytes
(`SubscriptionService.eventFor` drops them), no route serves them, and
`ConsumerClient.bytesOf` returns the empty array to `SegmentReader.open`,
which throws. With the default configuration -- `direct` off, a 256 KiB
inline cap (`FetchPolicyConfig.DEFAULT_INLINE_CAP_BYTES`) -- **every segment
larger than 256 KiB, and every cold publish of any size, is served `proxy`**:
the policy is handed the SEGMENT's length (`publishSegment` passes
`heldBytes.length`, and `coldMode` passes `Long.MAX_VALUE`), not a run's. At
the default 8 MiB segment that is essentially every flush, so an assembled
deployment stops decoding the first time any pod flushes more than 256 KiB --
however small each stream's share of it. It has stayed
latent because every end-to-end test writes small runs, and because
`ThreeModesAgreeTest` bridges `proxy` in-process with the assembled bytes
the wire drops. M10 closes the defect first and measures second.

⚠️ **THE HTTP SUBSCRIPTION ALSO BUFFERS WHAT IT THEN DROPS.**
`SubscriptionService` subscribes each session through
`SubscriptionHub.assembling`, which copies a whole `proxy` segment into a
`ByteArrayOutputStream` per session, charges it to the 64 MiB queue budget,
and discards it at encode. ADR-0004's "stream through, never buffer" does
not hold on the HTTP path today; NFR-6's evidence was taken below it.

⚠️ **SCOPE WAS DECIDED BY THE USER ON 2026-09-26.** The roadmap assigned FR-18
priority lanes to M10 too. They move to M11, and fast mode (FR-17) moves to
M12. Which milestone owns FR-21's refusing governor is NOT decided here: M10.6's
ADR decides it. No requirement changes; the order does.

## Requirements

| ID | What M10 does to it |
|---|---|
| **FR-6** (three fetch modes, the ingester chooses) | `proxy` becomes a working mode over HTTP: a route serves the bytes and the consumer fetches them |
| **NFR-5** (cross-AZ bytes < 0.1% of ingested) | Measured with the proxy payload term counted -- the term M9 left NOT-RUN -- across three AZ labels, beside a deliberately cross-AZ run proving the counter is live |
| **NFR-4** (read rate scales with segments, AZs, nodes) | The route issues at most one store GET per (serving node, segment) on a cache miss, single-flight; consumer-to-ingester requests are once per (consumer node, segment) |
| **NFR-6** (ingester memory bounded, independent of request size) | The route streams in `SegmentProxy` chunks; the HTTP subscription stops assembling non-inline segments per session |
| **FR-21** (cost governor) | Not built. Its refusing half gets an owning milestone by ADR (M10.6) |

## Scope

1. **An ingester route that streams one segment** (M10.1), with its ADR, and
   the writer's held bytes admitted to its cache so it serves its own segments
   at zero GETs (M10.15).
2. **The consumer fetches `proxy` deliveries** through it, coalesced once per
   (consumer node, segment) (M10.2).
3. **The HTTP subscription stops assembling** `proxy` and `direct` segments
   per session (M10.3).
4. **The plugin reports its AZ**, so a deployment's bytes are attributed by
   zone rather than counted as unknown (M10.4).
5. **NFR-5 measured with the payload term** on RustFS across three AZ labels
   (M10.5), results committed.
6. **FR-21's owner decided** by ADR (M10.6) and the roadmap amended.
7. **The M9 review harvest** (M10.7–M10.13), each row closed or
   dispositioned.

### Not in scope

- **Priority lanes (FR-18)** -- M11, by the user's decision.
- **Building the governor (FR-21)** -- M10.6 assigns it; it does not build it.
- **Consumer-side AZ failover across ingester endpoints.** Research 10 §8's
  "retry another same-AZ pod, then cross-AZ" needs the plugin to know more
  than its one configured endpoint. A failed proxy fetch surfaces as a failed
  delivery, which the existing reconnect/fallback ladder already handles.
  Recorded in Risks.
- **A peer-ingester hop** (ADR-0012's ring-owner fetch). The prefetcher
  (M8.56) already warms the ring owner's cache from the store; the route
  serves from that cache or the store, never from a peer.
- **Changing `FetchPolicy`.** Which mode is chosen is unchanged; M10 makes
  the chosen mode work.
- **S3 latency, TTFB and dollars** -- NOT-RUN as in M9 (no AWS account).

## Design

### The route (M10.1, with its ADR)

`GET /seg?key=<segmentKey>&az=<consumerAz>` on the ingester's front door,
served by a new `SegmentService` in `http`.

- **The key must parse as a canonical data-segment key** (`SegmentKey.parse`)
  **whose prefix is this deployment's own segment prefix**, or the answer is
  400 with a message that does not echo it. ⚠️ The prefix check is not
  redundant: `SegmentKey.parse` accepts any prefix, so without it a bucket
  shared by two deployments would serve one's segments to the other's
  consumers. Nothing under `ctl/`,
  `log/`, leases or checkpoints is addressable. security.md rules 5 and 7:
  the input is bounded by `SegmentKey`'s own length limit before any parse.
- **Trust domain.** A deployment is one trust domain (security.md rule 1,
  ADR-0010): any consumer that can reach `/sub` may already subscribe to any
  of its indices, so serving any of its segments adds no new exposure. The
  route is on the same listener and inherits whatever fronts `/sub`.
- **Chunked from the shared cache, never a per-request buffer.** The body
  is written through `SegmentProxy.streamTo(key, [responseSink])` in its
  64 KiB chunks. Nothing proportional to the segment is allocated per
  request: the one copy is the node's bounded `SegmentCache`, shared by
  every request, which is ADR-0004's point -- memory must not scale with
  fan-out.
- **Single-flight per key, and the gate never spans a client socket.** On a
  miss, the first request takes a per-key gate and FILLS the cache from the
  store (`streamTo(key, [])`, one GET, admitted); concurrent requests for the
  key wait on the gate, then every request streams from the cache. Holding
  the gate while writing to a client would let one slow consumer stall every
  other consumer of that segment on the node, which is the failure
  `SegmentProxy`'s javadoc names for a blocking sink.
  ⚠️ **THE COST, STATED:** a cold miss's first byte waits for the whole store
  read (~80 ms for 8 MiB at 100 MB/s) instead of arriving after the first
  chunk. On the writer (M10.15) and on each non-writing AZ's ring owner (the
  prefetcher, M8.56) the segment is cached before the push reaches the
  consumer; any OTHER serving node in an AZ misses once per segment.
  ⚠️ **A SEGMENT THE CACHE CANNOT HOLD COSTS K + 1 GETS FOR K READS**: the fill
  reads it for nobody, keeps none of it, and each read then streams from the
  store. Segments are bounded by the flush size, so this needs a segment above
  the cache's ceiling (4× `maxSegmentBytes`); stated here and pinned by a test
  rather than optimised. A capacity-0 cache is not filled at all: one GET per
  read.
- **Status mapping before the first byte.** A store failure is 503, a
  missing object included: the SPI's `get` throws a plain `IOException` for
  both (the conformance suite requires only that), and telling them apart
  would cost a HEAD per failure. After the first chunk the status is committed; a failure then
  truncates the chunked body, which the consumer's HTTP client reports as an
  error and `SegmentReader.open`'s footer check would refuse regardless.
- **Counted.** Every body byte is counted as `PROXY_READ` against the
  consumer's `az` parameter -- the same self-reported, accounting-only value
  the poll already uses (`SubscriptionService.AZ_PARAM`). It never routes.

**Alternatives rejected (the ADR carries them with numbers):**

- **Carry the segment on the poll answer.** That is `inline`, capped at
  256 KiB because the poll answer is held whole; an 8 MiB segment on it
  breaks NFR-6 and the queue budget.
- **Stream `proxy` bytes on the long-poll itself as a second frame kind.**
  One segment is shared by every run a node holds (up to ~178 of ~1,600);
  per-session delivery sends it once per run subscription, which the
  node-level coalescing below avoids.
- **Serve a range.** ADR-0044: nothing the consumer holds names the object
  length, and `SegmentReader.open` needs the footer. Whole-object only.
- **Reuse the catch-up POST.** It answers a node's replay request with
  INLINE event frames over `DurableCatchUpResponder`'s direct store GET,
  bypassing the cache; routing the live tail through it would cost one GET
  per request.

### The writer's held bytes warm its own cache (M10.15)

⚠️ **FOUND BY M10.0's REVIEW, AND IT CHANGES THE ROUTE's COST.** The node that
wrote a segment still holds it (ADR-0004: "the writing AZ needs no GET"), but
`SegmentServingPath.writeHeldBytesChunked` hands the held array to subscribers
and never admits it to `SegmentCache` -- the only admission in the tree is
`SegmentProxy.streamTo`'s, after a store GET. So without M10.15 the writer's
first `GET /seg` for every segment misses and buys a GET the research prices
at zero, and the writing AZ has no warmer at all (`SegmentPrefetcher` declines
there by design). M10.15 admits the held bytes at publish, bounded by the
cache's own ceiling. Then a segment costs **≤ (AZs − 1) prefetch GETs** -- one
per non-writing AZ's ring owner -- **plus ≤ 1 cold GET per other serving node
that misses**, which is M9 criterion 5(a)'s budget restated for the route.
⚠️ The ring owner's own cold term is not zero either: its prefetch, its cold
publish and the route share no single-flight, so overlapping reads each GET
(criterion 9 reports the rate).
The peer hop that would remove the second term is out of scope.

### The consumer (M10.2)

- `ProxySource` -- a seam in `client`: `byte[] fetch(String segmentKey)`.
  `HttpProxySource` implements it with the Helidon web client the module
  already uses (`HttpSegmentSource`), so `check-io-seam` needs no new
  exemption.
- `ConsumerClient.bytesOf`: `PROXY` -> `proxySource.fetch(segmentKey)`;
  no source configured is an `IllegalStateException` naming the
  misconfiguration, mirroring `DIRECT`. A failed fetch propagates; it never
  becomes an empty array.
- **Coalesced per (node, segment).** The plugin's `NodeSegmentSource` gains
  the same per-key gate and byte-bounded hold for proxy keys, so K runs of one
  segment on one node cost one request. This is what keeps M9 criterion 5(b)'s
  ingester-served count linear in nodes rather than in runs.

### The subscription stops assembling (M10.3)

`SubscriptionService` subscribes through a subscriber that assembles only
`INLINE` pushes and hands every other push on with an empty segment and no
copy. The queue budget is then charged only for bytes the answer will carry.

### The plugin's AZ (M10.4)

A node setting `binstore.ingester.az` (default empty) is passed to
`HttpSubscriptionTransport` and `HttpProxySource`. Empty keeps today's
behaviour: bytes counted as unknown, which the counter treats as cross-AZ
-- the conservative direction.

## Cost impact

| Path | Before M10 | After M10 | Rule |
|---|---|---|---|
| Publish of a `proxy` segment on the WRITER | 0 GETs (held bytes), not cached | 0 GETs, held bytes admitted to the cache (M10.15) | R10, R11 |
| Publish of a `proxy` segment on another serving node | 1 GET per (node, segment) on a miss, 0 on a hit -- bytes then discarded | unchanged; the GET now also warms the cache the route reads | R10, R11 |
| Consumer fetch of a `proxy` segment | impossible (decode throws) | 0 on the writer and on a prefetched ring owner; ≤ 1 GET per (other serving node, segment) on a miss, single-flight; K + 1 for K reads of a segment above the cache ceiling | NFR-4 (M9 criterion 5(a)) |
| Consumer-to-ingester requests | 0 | 1 per (consumer node, segment) | NFR-4 (M9 criterion 5(b)), may scale with nodes |
| LIST | 0 | 0 | R2, NFR-3 |
| Idle | 0 | 0 -- the route is request-driven | R3, NFR-2 |

Requests per MiB written (NFR-1) are unchanged: nothing on the write path
moves. The one new term is ingester-served HTTP, which is not an object-store
request.

## Acceptance criteria

1. **A `proxy` delivery decodes end to end over HTTP.** An assembled ingester
   with default settings, a flush producing a segment larger than the 256 KiB
   inline cap, and an `HttpSubscriptionTransport` consumer: `readNext` returns
   every record, where today it throws. (M10.2)
2. **The route writes in chunks from one shared copy.** A segment larger than
   the chunk reaches the client whole, and no single write to the response
   exceeds one chunk; N requests for one segment are served from the one
   cached copy. (M10.1)
3. **The route addresses only this deployment's segment keys.** `ctl/`,
   `log/`, lease, checkpoint, traversal, oversize and foreign-prefix keys are
   refused 400 without echoing the key and without a store request; a store
   failure before the first byte -- a missing object included, since the SPI's
   `get` does not tell them apart -- is 503, never 200. (M10.1)
4. **Single-flight.** N concurrent cold requests for one key cost one store
   GET, counted by `CountingBinStore`. (M10.1)
5. **Counted.** Every body byte the route writes is counted once as
   `PROXY_READ` against the consumer's `az`; same-AZ bytes are not cross-AZ.
   (M10.1)
6. **Coalesced on the consumer node.** K runs of one segment on one node cost
   one proxy request. (M10.2)
7. **No per-session copy.** Publishing a `proxy` segment to S HTTP sessions
   charges 0 bytes to the subscription queue budget and allocates no
   segment-sized array per session. (M10.3)
8. **The plugin's AZ reaches the wire**: with `binstore.ingester.az=az-b` the
   poll and the proxy fetch both carry `az=az-b`; unset, neither does. (M10.4)
9. **NFR-5, full, on RustFS across three AZ labels.** Three ingester nodes
   labelled `az-a`, `az-b`, `az-c`; a consumer in each AZ served by its
   same-AZ node, in `proxy` mode: the sum of every node's cross-AZ bytes is
   **< 0.1%** of producer bytes accepted, **and every node's same-AZ
   `PROXY_READ` bytes are at least the segment bytes its consumers fetched
   through the route** (one whole segment per fetch, however many runs it
   decodes) -- the event frames that column already counts cannot satisfy
   that. Reported beside a deliberately cross-AZ-served run whose `PROXY_READ`
   cross-AZ bytes are at least the segment bytes it delivered -- the proof the
   counter is live, which an uninstrumented build would fail. **Store GETs per
   segment ≤ (AZs − 1) + (serving nodes other than the writer) = 2 + 2 = 4**
   on this rig -- M9 criterion 5(a)'s budget, "(AZs − 1) prefetch GETs plus
   ≤ 1 cold GET per serving node", with the writer's term at zero because it
   caches its held bytes (M10.15). ⚠️ **THE COLD TERM IS NOT ZERO ON A CORRECT
   BUILD**: prefetch (`SegmentPrefetcher.onDurable`), the cold publish
   (`streamFromStore`) and the route each read through `SegmentProxy`, the
   cache admits only after a read completes, and no single-flight spans the
   three -- so a publish or a fetch landing during an in-flight prefetch buys
   its own GET. The run therefore also REPORTS GETs per segment by node and
   the fraction of segments above AZs − 1, which is the race rate; a shared
   single-flight that would remove it is not in scope. Results
   committed as `docs/internal/product/measurements/results/cross-az.csv`.
   (M10.5, M10.15)
10. **FR-21 has an owner.** An accepted ADR assigns the refusing governor to a
    named milestone and the roadmap row says so. (M10.6)
11. **The M9 review harvest is closed or dispositioned**: every item M9's
    `VERIFIED.md` carried forward has a row in M10.7–M10.13 that is done --
    fixed with evidence, or dispositioned in writing where the row says a
    disposition is acceptable (M10.8's crash, M10.12's monitor). (M10.7–M10.13)
12. **The writer serves its own segment at zero GETs.** After publishing a
    segment above the inline cap, a `GET /seg` on the writing node costs no
    store request; a segment above the cache ceiling is still served, and K
    reads of it cost at most K + 1 GETs. (M10.15, M10.1)

## Test plan

| Task | Tier | Test that must fail first | Mutation it catches |
|---|---|---|---|
| M10.1 | T1 (Helidon on loopback, `MemoryBinStore`) | `SegmentServiceTest#aSegmentLargerThanAChunkIsServedWhole` | route answers 200 with an empty or truncated body |
| M10.1 | T1 | `SegmentServiceTest#onlyThisDeploymentsSegmentKeysAreAddressable` | route passes any key to the store (reads `ctl/`) |
| M10.1 | T1 | `SegmentServiceTest#aStoreFailureBeforeTheFirstByteIs503` | failures answered 200 with an empty body |
| M10.1 | T1 | `SegmentReadsTest#concurrentColdReadsOfOneSegmentCostOneGetAndEachGetsItWhole` | single-flight removed |
| M10.1 | T1 | `SegmentReadsTest#theGateIsNotHeldWhileASinkIsWriting` | the gate spans a slow consumer's sink |
| M10.1 | T1 | `SegmentReadsTest#aSegmentTooLargeToCacheCostsAtMostOneGetPerReadPlusTheFill` | an uncacheable segment re-filled per read |
| M10.1 | T1 | `SegmentServiceTest#everyBodyByteIsCountedOnceAsProxyReadByConsumerAz` | payload not counted, or counted as cross-AZ for a same-AZ consumer |
| M10.1 | T1 | `SegmentReadsTest#aCachedSegmentIsServedWithoutAGetAndInChunks` | whole-segment write per request |
| M10.15 | T1 | `WriterCacheAdmissionTest#theWriterServesItsOwnSegmentWithoutAGet` | held bytes handed to subscribers but not admitted |
| M10.2 | T2 (assembled server, loopback) | `ProxyDeliveryOverHttpTest#aSegmentAboveTheInlineCapIsDecodedThroughTheProxyRoute` | `bytesOf` returns the delivery's empty array |
| M10.2 | T0 | `ConsumerClientProxyTest#aFailedProxyFetchPropagates` | failure swallowed into an empty poll |
| M10.2 | T1 | `NodeSegmentSourceTest#kRunsOfOneSegmentCostOneProxyFetch` | per-run fetch |
| M10.3 | T1 | `SubscriptionServiceProxyBudgetTest#aProxyPushChargesNothingToTheQueueBudget` | per-session assembly restored |
| M10.4 | T1 | `BinStorePluginAzTest#theConfiguredAzReachesPollAndProxyRequests` | setting read and dropped |
| M10.5 | T3 (RustFS, three `NodeProcess`es) | `CrossAzBytesIT#threeAzProxyServingStaysBelowTheNfr5Budget`, `#aCrossAzProxyFetchCountsItsPayload` | payload not counted; same-AZ counted as cross-AZ; per-consumer GET |

Coverage and mutation: diff-scoped `checkMutants` on `http`, `client` and
`plugin` for M10.1–M10.4, at the repository's 80% floor; the known http
soak-test crash (M10.8's disposition) is stated if it recurs. Suites
extended: the cost assertions (`CountingBinStore` in M10.1 and M10.5) and
the cross-AZ measurement.

## Risks

- **A consumer served cross-AZ in `proxy` mode ships whole segments across a
  zone** -- 419× a GET (research 05 §5b). The policy already shrinks `inline`
  cross-AZ; nothing yet prefers `direct` for a cross-AZ consumer, because `az`
  is self-reported and must not route. The mitigation is deployment (each
  plugin's endpoint in its own AZ, M10.4 makes it observable); M10.5's
  cross-AZ run quantifies the cost of getting it wrong.
- **A proxy fetch failure has no in-AZ failover** (Not in scope). Its effect is
  a failed delivery and the existing reconnect; the measurement would show a
  retry storm as extra ingester-served requests.
- **Cache eviction under load** turns hits into GETs. Single-flight bounds a
  concurrent burst to one; an eviction between fill and serve costs one more
  per request. M10.5 asserts M9 criterion 5(a)'s bound and reports how often
  a segment exceeds AZs − 1.
- **No single-flight spans prefetch, cold publish and the route** (criterion
  9). A read landing during another path's in-flight read of the same segment
  buys its own GET. Bounded by 5(a)'s cold term; M10.5 reports the rate.
- **A cold miss is not streamed through** (Design, single-flight): first-byte
  latency on a miss is the full store read. Measured nowhere yet; M10.5
  records serve latency beside the byte counts.
- **The rig has one Docker host**, so AZs are labels, not networks -- as in
  M9, byte counts hold on a cloud to the extent labels match real zones.

## Decisions

1. **Scope** (user, 2026-09-26): proxy + NFR-5 + FR-21 ownership + harvest;
   FR-18 to M11, FR-17 to M12. -> M10.0
2. **The route's shape** -- an ADR, with M10.1.
3. **FR-21's owner** -- an ADR, M10.6.

## Tasks

Dependency order: the route, then the writer's cache admission its cost
depends on, then the consumer that calls it, then the
subscription that stops paying for bytes it drops, then the AZ that makes the
measurement meaningful, then the measurement. The ADR and the harvest do not
depend on the route.

| ID | Task | Serves |
|---|---|---|
| M10.0 | This spec, the decomposition, and the roadmap amendment (M9 complete; M10 row; FR-18 to M11, FR-17 to M12) | — (planning) |
| M10.1 | An ADR and the ingester's `GET /seg` route: segment keys only, streamed through `SegmentProxy`, single-flight per key, 503 before the first byte, body counted as `PROXY_READ` by consumer AZ | FR-6, NFR-4, NFR-5, NFR-6 |
| M10.15 | The writer admits its held segment bytes to its `SegmentCache` at publish, so the writing node serves `/seg` at zero GETs (found by M10.0's review) | NFR-4, FR-6 |
| M10.2 | The consumer fetches `proxy` deliveries: `ProxySource`, `HttpProxySource`, `ConsumerClient` dispatch, per-(node, segment) coalescing in the plugin | FR-6, NFR-4 |
| M10.3 | The HTTP subscription stops assembling non-inline segments per session | NFR-6 |
| M10.4 | The plugin reports its AZ on the poll and the proxy fetch | NFR-5 |
| M10.5 | NFR-5 on RustFS across three AZ labels with the proxy payload counted, a cross-AZ control run, GETs per segment bounded; results committed | NFR-5, NFR-4 |
| M10.6 | An ADR giving FR-21's refusing governor an owning milestone; roadmap amended | FR-21 |
| M10.7 | Harvest (5f7e242): the mutants gate's UNUSED-baseline inference false-refuses an edit elsewhere in a mutated method; the `CHECK_RANGE` case must assert its fixture index is non-empty | — (gate) |
| M10.8 | Harvest (34e4fcd): `DrainAskBytesTest` gets an independent byte expectation; the http soak-test crash under `checkMutants` is dispositioned in writing | NFR-5 |
| M10.9 | Harvest (2cd990f): the partition chaos poll's `Thread.sleep(25)` becomes a bounded observation | NFR-7 |
| M10.10 | Harvest (872efb9): the curve generator refuses duplicate size-rate rows; zero-GET pricing is pinned | NFR-1 |
| M10.11 | Harvest (8a0e9ec, eef4d88, b65b9f9, bf8877b/c1d9f54): catch-up test gaps -- legacy singleton default, an oversized end marker, partial overlaps, no refusal-path permit residue | FR-9 |
| M10.12 | Harvest (895710a, fb0f56b): Tier 3's reachability flip during the final segment GET is pinned; the Tier 2 monitor held over a bounded read is fixed or its bound is recorded | FR-10 |
| M10.13 | Harvest (1ed333b, 355899e, ae5b4c4): nested-package coverage in the wired scan; the M9 test-plan tier label; testing.md's RustFS wording | — (gate, docs) |
| M10.14 | `VERIFIED.md`, `checkMilestoneVerified`, the milestone review, and the roadmap marked complete | — |
