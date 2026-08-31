# 0016. Writer count scales with throughput; consolidate only when traffic is thin

Status: **superseded by [0017](0017-every-pod-writes.md)** (2026-08-30) — the
$311/month problem was real, but decomposition showed the **adaptive interval
delivered 95% of the fix and forwarding only $7.78/month**, which does not justify
a hop, a lease and a ramp-time chokepoint. Everything below about the adaptive
interval, `fillRatio` and commit-on-arrival is **carried forward** by 0017; only
the writer consolidation is withdrawn. Retained because *why consolidation looked
attractive* is the instructive part.

Previously: accepted; corrected 2026-08-30 before first commit — the decision
originally said "one designated writer per AZ" as a fixed rule. It is a *function
of throughput*, and stating it as a constant would have imposed a ceiling that
does not exist. See §Decision 1.
Date: 2026-08-30
Requirements: NFR-1, FR-12
Research: docs/research/30-design-space/13-worked-example-multi-tenant.md §8

## Context

Every pod writing its own segment makes the PUT rate `pods ÷ flushInterval`, which
is independent of throughput. At high volume that is fine — segments fill on size
and the rate is `bytes ÷ segmentSize` instead. **At low volume it is a disaster.**

Measured at the realistic normal load of **60,000 records/minute (~1 MiB/s)**:

| Writers | Interval | Segment size | PUT/s | Cost |
|---|---|---|---|---|
| 6 (every pod) | 250 ms | **41.7 KiB** | 24.0 | **$311/mo** |
| 6 | 5 s | 833 KiB | 1.2 | $15.55/mo |
| **3 (one per AZ)** | **2 s** | **667 KiB** | **1.5** | **$19.44/mo** |
| 3 | 5 s | 1.6 MiB | 0.6 | $7.78/mo |

$311/month to move 2.5 TB is $0.12/GB — worse than the network bill this project
exists to remove.

## Decision

**1. The number of writers per AZ scales with that AZ's throughput.**

A writer can fill a segment inside the latency ceiling iff
`interval = segmentSize × W ÷ azBytes ≤ ceiling`, so:

```
writersPerAZ = clamp( 1 , podsInAZ , floor( azBytes × latencyCeiling ÷ segmentSize ) )
```

| Total ingest | Per AZ | maxWriters/AZ | In practice |
|---|---|---|---|
| **1 MiB/s (normal)** | 0.3 | **0.21** | **1 writer; others forward** |
| 10 MiB/s | 3.3 | 2.08 | every pod writes |
| 50 MiB/s (backfill) | 16.7 | 10.4 | every pod writes |
| 500 MiB/s | 166.7 | 104 | every pod writes |

⚠️ **Above ~10 MiB/s every pod writes and there is no forwarding at all.**
Consolidation applies only in the thin-traffic regime — which is exactly the
regime where a single writer is trivially sufficient (0.33 MiB/s, 5 Mbps,
**0.002 cores**).

**Why writer count is free above that threshold:** once the interval adapts to
hit the segment-size target, the object count is `azBytes ÷ segmentSize`
regardless of how many writers share the work —

| Load | 1 writer/AZ | 2 writers/AZ | 6 writers/AZ |
|---|---|---|---|
| 50 MiB/s | 6.25 obj/s | 6.25 obj/s | 6.25 obj/s |
| 100 MiB/s | 12.50 obj/s | 12.50 obj/s | 12.50 obj/s |
| 1 MiB/s | 0.60 obj/s | 1.20 obj/s | 3.60 obj/s |

Writer count only moves the object count when the **time** trigger binds — i.e.
only when traffic is thin. That is the whole content of this decision.

Writers hold leases, same mechanism as the sequencer
([ADR-0002](0002-object-store-cas-is-the-only-coordination-substrate.md)).
Non-writers forward by consistent hash over the active writer set. The count is a
**control loop with hysteresis** — require the threshold to be crossed by a margin
and rate-limit changes, or a workload oscillating around 10 MiB/s will flap.

**2. The flush interval adapts to hit a segment-size target.** Target 8 MiB;
lengthen the interval (250 ms → 30 s, capped by a per-lane latency ceiling) while
segments come in under target, shorten it as volume rises. PUT cost then tracks
`bytes ÷ segmentSize` in both regimes instead of `writers ÷ interval` in one of
them.

## Alternatives considered

- **Every pod always writes** (the original design). Rejected *only in the thin
  regime*: $311/month at 1 MiB/s, and the failure is invisible at the throughput
  the cost model was first written for. Above ~10 MiB/s it is exactly right, which
  is why the rule is a function and not a constant.
- **One writer per AZ always** (this ADR's first draft). ⚠️ **Rejected on the
  scaling objection**: it would cap an AZ's write path at one pod's NIC and CPU for
  no benefit, since above the threshold writer count does not affect object count
  at all. A fixed consolidation would have made mass bootstrap a bottleneck that
  the arithmetic says need not exist.
- **One cluster-wide writer**, halving PUTs again. ⚠️ **Rejected on a computed
  margin, not intuition:** forwarding ⅔ of ingest across AZs costs **$35.39/month**
  at 1 MiB/s, against a PUT saving of $5–26/month depending on interval. It loses
  at every interval we would use, and it adds a cluster-wide write dependency.
  ⚠️ The margin narrows as interval shortens — re-check it if the latency floor
  ever drops below ~1 s.
- **A fixed long interval** instead of adapting. Rejected: it prices low traffic
  correctly and starves high traffic of latency, or the reverse. The regimes differ
  by 100× in volume and cannot share a constant.

**2b. The control signal is `fillRatio`, measured locally — no rate aggregation.**

⚠️ A pod cannot see its AZ's aggregate ingest rate, and **it does not need to.**
The rule `W ≤ azBytes × ceiling ÷ segmentSize` rearranges into something a single
writer observes directly at every flush:

```
fillRatio = actualSegmentBytes ÷ targetSegmentSize
          = (azBytes ÷ W) × interval ÷ segmentSize
```

A writer sees only its own share `azBytes ÷ W` — and that ratio is **exactly** what
the rule depends on. No gossiped rates, no summing across pods, no global view.

| Observation | Meaning | Action |
|---|---|---|
| `fillRatio ≥ 0.9` sustained (size trigger firing before the timer) | segments are full; another writer would not add objects | **scale up** |
| `fillRatio ≤ 0.4` at the maximum interval | writers are surplus; the timer is cutting half-empty segments | **scale down** |

**How the signal reaches a pod that can act on it:** the writer returns
`{fillRatio, currentInterval}` on the **forward-ack** — piggybacked on traffic
that already flows ([ADR-0012](0012-peer-mesh-without-gossip.md)). A forwarder
seeing sustained `fillRatio ≥ 0.9` acquires a writer lease; the lease serialises
the addition so only one pod joins per round.

**Sampling window adapts for free.** One sample per flush means ~1.5 s of history
when busy (250 ms flushes) and ~30 s when idle (5 s flushes) — fast where it must
be, stable where it can be. And a burst is detected as soon as **one segment
fills**, which is by definition quick:

| Load | Writer's share | Time to fill 8 MiB = detection lag |
|---|---|---|
| 1 MiB/s | 0.33 MiB/s | never fills — timer cuts at 5 s (under-full, correct) |
| 10 MiB/s | 3.3 MiB/s | **2.4 s** |
| 50 MiB/s (backfill) | 16.7 MiB/s | **0.48 s** |
| 200 MiB/s | 66.7 MiB/s | **0.12 s** |

**Scale up fast, scale down slow — deliberately asymmetric.** Being
over-provisioned at 1 MiB/s costs ~$38/month (6 writers vs 1); being
under-provisioned during a burst costs *latency*, which is worse operationally.
So: `T_up` a few seconds, `T_down` a few minutes, with a wide 0.4/0.9 band. Only
the lowest-ranked surplus writer steps down per round, and never below one.

⚠️ Writer saturation (CPU or NIC) produces the **same** `fillRatio ≥ 0.9` signal
as healthy size-triggering, and both mean "add a writer" — so no separate
saturation detector is needed. Queue depth remains a *backpressure* signal to the
producer, which is a different thing.

**3. The sequencer commits on arrival, rate-limited — not on a fixed timer.**
⚠️ A fixed 250 ms commit interval writes 4 deltas/s regardless of whether anything
arrived. At normal load that is **7× the segment rate** and the dominant cost:

| Load | Segments/s | Fixed-timer deltas | On-demand deltas | Saving |
|---|---|---|---|---|
| **1 MiB/s** | 0.60 | 4.00/s → **$59.62/mo** | 0.60/s → **$15.55/mo** | **$44/mo** |
| 50 MiB/s | 6.25 | 4.00/s | 4.00/s (cap binds) | — |
| 100 MiB/s | 12.50 | 4.00/s | 4.00/s (cap binds) | — |

`deltas/s = min(commitArrivalRate, 1 ÷ minCommitInterval)`. Commit as soon as a
request arrives; batch only when a second arrives inside the minimum interval.
Waiting 250 ms to batch 0.15 commits is pure loss.

## Consequences

- PUT cost at normal load drops from **$311/month to ~$8–19/month**, a 20–40×
  reduction from two changes that cost nothing structurally.
- ⚠️ **GET falls with PUT, not against it.** Because the ingester proxies reads
  ([ADR-0004](0004-the-service-serves-reads.md)), prefetch GETs are
  `segments/s × (AZ−1)`, so fewer, larger segments mean fewer GETs too:
  24 GET/s at 250 ms becomes 1.2 GET/s at 5 s. **There is no PUT/GET trade-off in
  bundling** — that trade-off only exists when consumers fetch whole objects
  themselves, which ADR-0004 removed.
- The receiving pod holds records slightly longer before forwarding; the forward
  hop adds ~0.5 ms intra-AZ — **and only in the thin regime**, where the forwarded
  volume is a fraction of a MiB/s.
- ⚠️ **The real capacity limit is pods, not writers.** A pod is network-bound at
  roughly **500–600 MB/s** (10 Gbps NIC, in + out), needing ~3 cores of zstd-3 to
  keep up. Size pod count for peak bootstrap volume; the writer rule never
  constrains it.
- A writer holding un-forwarded buffers must flush or hand off before terminating —
  the same shutdown obligation fast mode introduces
  ([ADR-0013](0013-fast-mode-wal-and-quorum.md)).
- The interval is now a **measured control loop**, so it needs a metric
  (`segment_fill_ratio`) and a guard against oscillation.
