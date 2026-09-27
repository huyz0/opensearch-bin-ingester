# M10 — Proxy serving, priority lanes and the cost governor

## Completion condition

The roadmap's M10 row, restated: **the same-AZ proxy segment route serves
consumers and NFR-5 is proven in full on it, proxy payloads included; priority
lanes (FR-18) schedule admission, buffering, flush and push; the cost governor
(FR-21) refuses discretionary LIST past its ceiling and halts discretionary
work, never a write, with NFR-16's zero steady-state refusals measured; and
M9's review harvest is closed.**

M10 collects the obligations earlier milestones assigned to it and did not
discharge:

| Obligation | Assigned by |
|---|---|
| Proxy segment serving and the full NFR-5 proof | roadmap § Deferred, M9.58, [M9/VERIFIED.md](../M9/VERIFIED.md) finding 1 |
| Priority lanes (FR-18) | roadmap § Deferred, ADR-0014, ADR-0025, M3 and M5 SPECs |
| The cost governor's refusing half (FR-21, NFR-16) | M7 SPEC (NFR-16 row), M8 SPEC § Not in scope, M9/VERIFIED.md finding 4 |
| Concurrent cold reads of one segment coalesced (M5.63) | pulled in because the proxy route makes it the normal case (below) |

⚠️ **THE RIG IS M9's RIG.** One workstation, Docker, the digest-pinned RustFS
of `docker-compose.test.yml` (ADR-0060). NFR-5 is a BYTE COUNT, so it holds on
S3 for the reason M9's SPEC table gives: the AZ is a label the pods carry, not
a network the rig has. Latencies in this milestone are rig lower bounds.

## Requirements

| ID | What M10 does to it |
|---|---|
| **FR-6** (serve in `inline`, `proxy`, `direct`) | `proxy` becomes executable end to end: an ingester route streams the segment and the consumer fetches it. Until now a `PROXY` event carried coordinates the consumer could not resolve |
| **NFR-5** (cross-AZ bytes < 0.1% of ingested) | Measured IN FULL: every implemented transport, including proxy segment payloads, on a two-AZ fleet |
| **NFR-4** (read rate scales with segments, AZs, nodes) | Preserved and tightened: the route reads through the node-wide `SegmentCache`; concurrent cold fetches of one segment cost one GET (M5.63); the plugin fetches each segment once per node, keyed by segment key |
| **FR-18** (priority lanes) | Carried on the producer request, recorded per run, and expressed as scheduling at admission, flush and push ([ADR-0074](../../decisions/0074-priority-lanes-are-carried-per-request-and-scheduled-per-run.md)) |
| **NFR-1** (write request rate) | **AMENDED by ADR-0074, with the number**: while records of positive lane `+L` are buffered, a pod may flush up to `2^L` times per interval ceiling, so its low-rate bound is `≤ 2 × 2^L` data+commit PUTs per ceiling (≤ 8 at the default top lane `+2`); with no positive-lane traffic the bound is unchanged at 2. Counted by criterion 9 |
| **FR-21** (cost governor) | **PARTIALLY served.** Ratio-to-expected, a LIST ceiling on discretionary LIST, a kill switch and refusal counts attributed by `(op, purpose, domain)` ([ADR-0075](../../decisions/0075-the-cost-governor-refuses-discretionary-work-and-never-a-write.md)). The INDEX half of the attribution, `GET /admin/cost` and quota enforcement (Q10) are re-homed to M11 by the roadmap's Deferred table, with ADR-0075's reason |
| **NFR-3** (LIST: zero on hot paths, ~1/s hard ceiling) | The ceiling becomes a refusing runtime bound for every LIST — **AMENDED by ADR-0075**: the four DECLARED recovery paths (chain-end recovery, chain replay, takeover backfill, inbox drain) are exempt, each bounded by its trigger, and declaration is by the caller, never by key prefix |
| **NFR-16** (governor refusals in steady state: zero) | Asserted over a steady-state run with the governor in the assembled path |
| **NFR-7** | Not re-measured; a negative lane's deadline is the interval ceiling, NFR-7's divisor (ADR-0063), so no lane can breach it |

## Scope

- The segment-fetch route on the ingester and the consumer's `PROXY` fetch,
  with the plugin fetching once per node per segment key.
- M5.63: one in-flight store read per segment key, joined by concurrent callers.
- NFR-5 on a two-AZ fleet on RustFS, proxy payloads included.
- Lanes: the request parameter, the run entry's real lane byte, per-lane flush
  deadlines, lane-ordered push, and weighted fair-share admission.
- The cost governor as a `binstore-spi` decorator, wired into the assembly, with
  the sweeps deferring on refusal and the prefetcher honouring the halt.
- Root-causing the one test that failed under full-suite load on the baseline
  (`RoutedIngestTest`) — a failing test is never written off as a flake.
- M9's review harvest, each row fixed or dropped with its reason.

### Not in scope

- **Fast mode (FR-17)** — M11, unchanged.
- **Compaction (FR-14)** — unchanged, deferred.
- **Per-INDEX cost attribution, `GET /admin/cost` and quota enforcement** — M11,
  by the roadmap's Deferred table. A store request carries one SEGMENT holding
  many indices, so an index share is an apportionment model rather than a count
  (ADR-0075), and quotas (Q10) need that apportionment to enforce anything.
- **A per-INDEX lane active set.** ADR-0014 configures the active set per index;
  M10 configures it per pod, because the catalog that registers index shapes
  carries no lane configuration today. ADR-0074 records the amendment.
- **Lane-ordered DATA layout.** ADR-0014's "runs placed first" buys nothing
  when every reader fetches the whole object (ADR-0044, ADR-0073), and it would
  break the key-contiguity `SegmentWriter` keeps for a ranged read (M5.66).
  ADR-0074 records the amendment.
- **Lane-per-source-partition.** ADR-0014's upgrade path, needing OpenSearch's
  `MODULO` and a composite pointer; neither exists in 3.8.0.
- **A pod serving live events for segments another pod wrote.** Subscriptions
  are served by the writer, as today; the proxy route serves bytes, not events.

## Design

### Proxy route ([ADR-0073](../../decisions/0073-the-proxy-route-is-a-whole-segment-get-on-the-consumers-own-endpoint.md))

`GET /seg?key=<segment key>&az=<consumer AZ>` on every ingester. The key must
parse as a canonical data `SegmentKey` under this deployment's own prefix;
anything else is a `400` **before any store I/O**. The body is the WHOLE
segment, streamed through `SegmentProxy`, served from the node-wide
`SegmentCache` or one store GET that fills it. A missing object is `404`, a
store failure on an object that exists is `502` — never `404`, which a consumer
treats as permanent. Bytes sent are counted as `PROXY_READ` against the
caller's `az`. The consumer fetches from the endpoint it is configured with —
the same-AZ service in production — through `SegmentSource.fetchSegment(key)`,
a default method refusing with an `IOException`, and `ConsumerClient` resolves
an empty `PROXY` delivery through it. The plugin's `NodeSegmentSource` dedups
proxied fetches by SEGMENT KEY (its grant path dedups by URL), so a node fetches
each segment once however many shards it holds.

⚠️ "Zero store requests after a prefetch" holds only when the consumer's
endpoint lands on its AZ's ring owner (ADR-0066); on any other pod of that AZ
the first fetch is one cold GET. The bound this milestone claims is **≤ 1 GET
per segment per serving ingester**, and M5.63 is what makes it hold under
concurrency.

### M5.63 — one in-flight read per key

`SegmentProxy` keeps a per-key in-flight entry. The first caller of a cold key
reads the store once; concurrent callers of the same key JOIN it and are handed
the same bytes when it completes, rather than issuing their own GET. A joiner
holds the whole segment — bounded by the cache's own admission ceiling, and a
segment larger than that is read by each caller as today. ⚠️ So a joiner is
BUFFERED, not streamed: the one stated exception to ADR-0073's "streamed", ~6 ms
per 8 MiB for K−1 callers in exchange for K−1 store GETs. If the winner's read
throws, every joiner fails with it: one store blip becomes K failed fetches,
each of which the consumer retries, which is the cost of never issuing the
duplicate GETs this row exists to remove.

### Lanes ([ADR-0074](../../decisions/0074-priority-lanes-are-carried-per-request-and-scheduled-per-run.md))

A `lane` query parameter on `_bulk`, signed, default `0`, refused `400` outside
the pod's active set (default `-2..2`; at most 8 lanes and none above `+2`, both
refused at configuration, so the lane cost below has a ceiling).
Every record of a request shares its lane. A run's lane is the MAXIMUM lane of
the records in it: one `(index, partition)` has one offset space and one run
per segment, so a lane never splits a run and never reorders a partition.
⚠️ **This amends ADR-0014**: "a +1 record receives the lower offset" held only
if one partition could hold two runs of different lanes; with one run per
partition a lane changes WHEN a partition's records commit, never their order
relative to each other.

| Stage | Positive lane `+l` | Lane 0 | Negative lane |
|---|---|---|---|
| Flush deadline (from that lane's oldest buffered record) | `max(floor, adaptive interval ÷ 2^l)` | the adaptive interval | the interval CEILING — also its anti-starvation bound |
| Push | a segment's runs are pushed highest lane first | | last |
| Admission | weighted fair share of the in-flight budget, each active lane guaranteed a floor (below) | | |

**The in-flight budget** is `ingest.admission.maxInFlightBulk` — the number of
`_bulk` requests a pod processes concurrently, default **256**. Lane `l`'s
weighted share is `budget × 2^l ÷ Σ_{k active} 2^k`; its FLOOR is
`max(1, ⌊share ÷ 2⌋)`. A request is admitted if the pod's total in flight is
below the budget, OR its lane's own in flight is below its floor; otherwise it
is refused `429`. So the hard ceiling is the budget plus the sum of floors, a
saturated pod still admits every lane up to its floor, and a lane is refused
only while the budget is saturated AND it is at or above its floor.

### Governor ([ADR-0075](../../decisions/0075-the-cost-governor-refuses-discretionary-work-and-never-a-write.md))

`GoverningBinStore` wraps the counting store. EVERY LIST passes a token bucket —
default 1/s sustained, burst 300 (so one retention tick's
`MAX_SWEEP_HOURS_PER_TICK` catch-up fits) — and past it is refused with
`GovernorRefusedException`, an `IOException`, UNLESS its caller runs it inside a
declared RECOVERY scope (`GovernorScope.recovery(...)`, a `ScopedValue`).
Exactly four callers declare it — `ChainEnd.of`, `ChainReplay`, `ChainBackfill`
and the inbox drain — because refusing them stalls a commit or an acked intent,
and each is already bounded by its trigger (cost.md rule 2b, ADR-0058). Recovery
is never inferred from a key prefix: a reinstated poll of the commit log would
match one. PUT, GET, STAT and DELETE are never refused.

Ratio-to-expected is computed for data PUTs over a window:
`expected = max(dataBytes ÷ segmentBytes, window ÷ spacing)`, where `spacing`
is the flush spacing IN FORCE that window — `max(floor, adaptive interval ÷
2^L)`, `L` the highest positive lane buffered — read from the pod. At a low rate
with the interval at the ceiling that is 0.2 data PUT/s, so a per-record PUT at
10 records/s reads as 50×. At **3×** the governor alarms; at **10×**
`discretionaryAllowed()` is false; at **100×** the KILL SWITCH trips and stays
tripped until `reset()` — which a pod restart also performs. LIST refusal is
not a trip condition: the bucket already bounds it. ⚠️ A controller bug that
holds the interval itself at the floor is invisible to the ratio, which trusts
the spacing the controller reports; NFR-1's counted bound catches that one.

## Cost impact

| Change | Store requests |
|---|---|
| Proxy route | ≤ 1 GET per segment per serving ingester, concurrent fetches included (M5.63); 0 on the AZ's ring owner once prefetched; the plugin issues none |
| Positive lane `+L` | ≤ `2^L` flushes per interval ceiling while its records are buffered; no active lane may exceed `+2`, so ≤ 8 data+commit PUTs per pod per ceiling — NFR-1's amended low-rate bound; no positive traffic, no change |
| Negative lane | flushes no sooner than lane 0 and no later than the ceiling: never more PUTs |
| Governor | none added; it can only remove discretionary LISTs |

## Acceptance criteria

1. **The route serves a durable segment whole and streamed**, T1: the body
   equals the stored bytes; a second fetch is a cache hit with zero further
   store GETs; a key outside the data grammar or the trust-domain prefix is
   `400` with zero store requests; a missing object is `404`; a store failure
   on an object that exists is `502`. (M10.1)
2. **`PROXY_READ` counts payload bytes by the caller's AZ**, T1: a same-AZ fetch
   adds to same-AZ bytes and not to cross-AZ; a fetch with another AZ adds its
   exact byte count to cross-AZ; a fetch naming no AZ is cross-AZ and
   unattributed. (M10.1)
3. **A `PROXY` delivery decodes at the consumer**, T1: `ConsumerClient` resolves
   an empty one through `SegmentSource.fetchSegment` by its exact key and yields
   the written records; one carrying bytes is not fetched; a consumer with no
   source fails loudly; the HTTP source sends the key and its AZ and refuses any
   answer but 200. (M10.2)
4. **The plugin fetches a proxied segment once per node**, T1: ≥16 shard
   subscriptions on one node reading the same segment key issue exactly one
   route fetch. (M10.3)
5. **Concurrent cold fetches of one segment cost one GET**, T1: K ≥ 8 callers
   racing on one cold key are served identical bytes from exactly one store
   GET; when that GET throws, every joiner fails and nothing is cached. (M5.63)
6. **NFR-5 IN FULL**, T3 on RustFS: pods in `az-a` and `az-b`; the producer
   writes to the `az-a` pod; an `az-b` consumer takes its events from the
   writer and its payloads from the `az-b` pod's proxy route. Cross-AZ bytes on
   every transport, INCLUDING proxy payloads, are < 0.1% of accepted producer
   bytes; same-AZ proxy payload bytes are ≥ the segment bytes delivered; and a
   control fetch of the same segment from the `az-a` pod as `az-b` adds its
   exact size to cross-AZ `PROXY_READ`, so the instrumentation is shown to fire.
   (M10.4)
7. **The run entry carries a real lane**, T0: a run's lane is the maximum of its
   records' lanes and round-trips through `SegmentReader`; an all-lane-0
   segment is byte-identical to the M9 golden; a mixed-lane golden is pinned.
   (M10.5)
8. **The producer names a lane**, T1: `lane` absent is 0; a value in the active
   set reaches the run entry; a value outside it, non-integer, or outside `i8`
   is `400`; an active set above 8 lanes, or containing a lane above `+2`, is
   refused at configuration. (M10.6)
9. **Per-lane flush deadlines, and their cost**, T0 with an injected clock:
   lane `+l` alone is due at `max(floor, interval ÷ 2^l)` and not before; a
   negative lane alone is due only at the ceiling; under a sustained stream of
   positive appends a buffered negative record is flushed no later than the
   ceiling after it arrived; and a lane `+2` trickle at the ceiling interval
   causes **≤ 4 flushes per ceiling** — while a lane-0 trickle causes 1. (M10.7)
10. **Push follows lanes**, T1: within one flushed segment the push reaches a
    positive-lane run before a lane-0 run before a negative-lane run. (M10.7)
11. **Weighted fair-share admission**, T0 over the formula above: with the
    budget saturated, a lane at or above its floor is refused `429` (never 5xx)
    while a lane below its floor is admitted; with the budget free every lane
    is admitted; a lane's floor is proportional to `2^l`. (M10.8)
12. **A high lane is committed sooner, end to end**, T2 single JVM with an
    injected clock, the interval lengthened to the ceiling: with lane −1 records
    buffered, a lane +2 append is acknowledged after at most `ceiling ÷ 4` of
    clock time — ⚠️ a lane-ignoring build waits the full ceiling and FAILS this
    — and the −1 records commit in that same flush, at their original offsets.
    (M10.9)
13. **The governor refuses undeclared LIST and nothing else**, T0: an
    undeclared LIST past the bucket is refused and counted WHATEVER its prefix —
    a commit-log prefix included; a LIST inside a declared recovery scope is
    never refused however many are issued; data, commit, checkpoint and lease
    PUTs, GETs, STATs and DELETEs are never refused even with the kill switch
    tripped; ratio-to-expected alarms at 3×, halts discretionary work at 10×,
    trips the kill switch at 100×, and the switch holds until `reset()`; and a
    low-rate regression — per-record PUTs at 10 records/s with the spacing at
    the 5 s ceiling — reads above the 10× halt. (M10.10)
14. **The governor is wired and NFR-16 holds**, T1/T2: the assembled store is
    governed; the four recovery callers declare their scope and a takeover's
    chain-end recovery succeeds with the bucket empty; a refused sweep LIST
    defers the pass rather than failing the loop; the prefetcher issues no GET while discretionary work is halted; and
    a steady-state append/commit/read/sweep run records **zero** refusals.
    (M10.11)
15. **The baseline's load-sensitive failure is root-caused**, T1:
    `RoutedIngestTest#aROUTEDWriteToAnUNREGISTEREDIndexWAITSAndIsRELEASED`'s
    failure under full-suite load is explained and fixed at its cause, not by a
    longer timeout. (M10.12)
16. **M9's harvest is closed**: every item of M9/VERIFIED.md finding 5 is fixed
    by a named test or dropped with its reason in the backlog row. (M10.20)

## Test plan

| Criterion | Tier | First failing test | Mutation it must kill |
|---|---|---|---|
| 1, 2 | T1 | `SegmentFetchServiceTest` | serve without the key check; count against the local AZ; 404 on outage |
| 3 | T1 | `ProxyFetchTest`, `HttpSegmentSourceProxyTest` | `bytesOf` returning the empty inline array for `PROXY` |
| 4 | T1 | `NodeSegmentSourceProxyTest` | per-shard fetch instead of per-node |
| 5 | T1 | `SegmentProxyInFlightTest` | a GET per concurrent caller |
| 6 | T3 | `CrossAzBytesIT#fullNfr5IncludesProxyPayloads` | payload served by the writer's pod |
| 7 | T0 | `SegmentWriterLaneTest`, `GoldenSegmentV1Test` | lane written as 0; min instead of max |
| 8 | T1 | `BulkServiceLaneTest` | default lane non-zero; range unchecked |
| 9 | T0 | `AccumulatorLaneTest` | positive lane waiting the adaptive interval; `2^l` dropped; negative lane never due |
| 10 | T1 | `DefaultIngestLanePushTest` | push in key order |
| 11 | T0 | `LaneAdmissionTest` | strict priority; floor ignored |
| 12 | T2 | `LaneOvertakeTest` | lane dropped between HTTP and ingest |
| 13 | T0 | `CostGovernorTest`, `GoverningBinStoreTest` | refusing a data PUT or a declared recovery LIST; exempting by prefix; LIST unbounded; switch self-clearing; expected taken at the floor |
| 14 | T1 | `GovernorAssemblyTest`, `OrphanSweepGovernorTest` | refusal propagating as a loop failure |
| 15 | T1 | the existing test, green under `./gradlew test` | — |

## Risks

- **A lane parameter producers never send.** Lanes default to 0, so the feature
  is inert until used. Accepted.
- **Admission 429 is new backpressure.** It fires only past 256 concurrent bulk
  requests per pod and a lane's floor, where before M10 requests queued on the
  accumulator without bound. Producers already retry 429 (OpenSearch bulk
  semantics).
- **A joiner holds a whole segment (M5.63).** Bounded by the cache's admission
  ceiling; a larger segment is read per caller as before.
- **A positive lane spends PUTs.** Bounded at `2^L` flushes per ceiling and
  amended into NFR-1 with the number, rather than hidden.

## Decisions

Recorded as ADRs in this commit: ADR-0073 (the proxy route), ADR-0074
(lane carriage and scheduling, amending ADR-0014 and NFR-1), ADR-0075 (the
governor, partially serving FR-21).

## Tasks

| ID | Task | Serves |
|---|---|---|
| M10.0 | This spec, its decomposition and ADRs 0073–0075 | — (planning) |
| M10.1 | The ingester segment-fetch route: whole, streamed, key-checked before I/O, 404/502 distinguished, counted by caller AZ, wired in the front door | FR-6, NFR-4, NFR-5 |
| M10.2 | The consumer resolves an empty `PROXY` delivery through `SegmentSource.fetchSegment` over HTTP | FR-6 |
| M10.3 | The plugin fetches a proxied segment once per node by key and reports its AZ | FR-6, NFR-4, NFR-5 |
| M5.63 | One in-flight store read per segment key, joined by concurrent callers (inherited, keeps its M5 row) | NFR-4 |
| M10.4 | NFR-5 in full on a two-AZ RustFS fleet, proxy payloads included | NFR-5 |
| M10.5 | The run entry's real lane: max of its records', golden | FR-18 |
| M10.6 | The producer's `lane` parameter, the active set, and the ingest seam | FR-18 |
| M10.7 | Per-lane flush deadlines, their counted cost, the NFR-1 amendment, and lane-ordered push | FR-18, NFR-1, NFR-7 |
| M10.8 | Weighted fair-share lane admission with a floor | FR-18 |
| M10.9 | A high lane is committed sooner, end to end | FR-18 |
| M10.10 | `CostGovernor`, `GoverningBinStore` and the declared recovery scope | FR-21, NFR-3 |
| M10.11 | The governor wired; the four recovery callers declare; sweeps defer; prefetch halts; NFR-16 zero | FR-21, NFR-3, NFR-16 |
| M10.12 | Root-cause the `RoutedIngestTest` failure under full-suite load | — (quality) |
| M10.20 | M9's review harvest, each item fixed or dropped with its reason | — (quality) |
| M10.21 | Close M10: VERIFIED.md, roadmap row, the milestone gate's default | — (evidence) |
