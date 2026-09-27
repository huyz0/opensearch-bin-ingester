# 0075. The cost governor refuses discretionary work and never a write

Status: accepted
Date: 2026-09-27
Requirements: FR-21, NFR-16
Research: docs/research/30-design-space/15-cost-governor.md §2, §3, §5

## Context

Research 15 argues that counting is not controlling: a reinstated per-shard poll
is 1.2M requests/s, $21,600/hour as LIST, and a counter only reports it. Since
M1 every store operation is counted (`CountingBinStore`), and since M9 PUTs are
attributed by purpose and priced by `CostMeter`. Nothing refuses. M7 bounded
LIST by pacing (one hour prefix per sweep pass, `MAX_SWEEP_HOURS_PER_TICK` = 4
per retention tick), and M9's review found FR-21's refusing half unowned.
cost.md rules 12–15 state the policy: govern on ratio-to-expected, alarm at 3×,
hard-stop discretionary work at 10×, never refuse a data write or a commit, and
cap LIST at ~1/s sustained.

LIST has two kinds of caller in this tree, and they fail differently when
refused. The orphan and retention SWEEPS list `<prefix>/data/` hour prefixes;
refusing one defers GC. RECOVERY lists the commit log and the inbox:
`ChainEnd.of` at takeover, `ChainReplay`, `ChainBackfill`, and the ADR-0058
inbox drain. Refusing one of those stalls a commit or an acked intent — a
write-path failure — and each is already bounded by its trigger: once per
takeover or heal (cost.md rule 2b, ADR-0058). A PREFIX cannot tell them apart
from a regression: a reinstated poll of committed data — research 15 §1's
$21,600/hour class — lists the commit log too.

## Decision

**A `GoverningBinStore` decorator in `binstore-spi`, above `CountingBinStore`,
consulting one `CostGovernor` per pod.**

1. **Every LIST passes the ceiling unless its CALLER declared it recovery.**
   A token bucket — 1 token per second sustained, 300 burst, from an injected
   clock — governs LIST; past it the store throws `GovernorRefusedException`,
   an `IOException`, so every sweep's existing failure path handles it and
   defers the pass. The burst covers one retention tick's catch-up after a
   takeover. The four recovery callers named above run their LISTs inside a
   declared RECOVERY scope (a `ScopedValue` the governor reads, set by the
   caller, never inferred from a key); those are counted and never refused. An
   undeclared LIST — any prefix, any caller, including one not yet written —
   is governed. PUT, GET, STAT and DELETE are never refused at the store: a
   data or commit PUT refused is data loss (rule 14), a read is degraded by the
   fetch policy, and DELETE reduces cost.
2. **Discretionary work asks first.** The orphan sweep, the retention sweep and
   the durable-segment prefetch call `discretionaryAllowed()` before starting,
   and defer when it is false.
3. **Ratio-to-expected, over a window, for data PUTs.**
   `expected = max(dataBytes ÷ segmentBytes, window ÷ spacing)`, where
   `spacing` is the flush spacing IN FORCE, read from the pod each window:
   `max(intervalFloor, adaptiveInterval ÷ 2^L)` with `L` the highest positive
   lane buffered in that window (0 if none) — the amended NFR-1 quantity
   (ADR-0074), taken at the smallest value it held in the window.
   ⚠️ **This departs from research 15 §2 in three ways, deliberately:**
   - the spacing in force stands in for its `flushInterval`, because the
     interval adapts between floor and ceiling (ADR-0017) and positive lanes
     shorten it (ADR-0074). At a low rate with the interval at the 5 s ceiling
     and no lane, expected is 0.2 data PUT/s, so a per-record PUT at 10
     records/s reads as 50×, and a per-partition PUT over 200 partitions as
     200× — the kill switch. ⚠️ **What it cannot see** is a controller bug that
     holds the interval itself at the floor: the ratio is taken against what
     the controller says is in force. That regression is NFR-1's counted
     low-rate bound's to catch (M9 criterion 3), not the governor's;
   - `writers` is 1: the governor is per pod, and every pod writes (ADR-0017);
   - the `1 ÷ commitInterval` term is dropped because the governed series is
     DATA PUTs only; commit PUTs track them 1:1 (ADR-0072's bound).
   At **3×** the governor alarms (a log line and a gauge); at **10×**
   `discretionaryAllowed()` is false for as long as it holds; at **100×** the
   KILL SWITCH trips and holds until `reset()`, which a pod restart performs.
   LIST refusal is not a trip condition: the bucket already bounds it.
4. **Refusals are counted per class** (`list`, `discretionary`), attributed by
   `(op, purpose, domain)`; steady state is zero (NFR-16).

## Alternatives considered

- **One LIST ceiling for every caller, recovery included** (research 15 §3 as
  written). Rejected: a takeover arriving while the bucket is drained could not
  recover its chain end and could not commit, and a refused inbox drain strands
  acked intents. Declared recovery LISTs are bounded by their triggers instead.
- **Exempt LIST by PREFIX** (everything outside `data/`). Rejected in review:
  a reinstated poll of the commit log would be exempt, counted and never
  refused — the exact runaway this ADR exists for.
- **Refuse by a fixed rate per operation.** Rejected by rule 13: a backfill
  legitimately multiplies PUTs, and a fixed rate either refuses it or is set so
  high it catches nothing.
- **Refuse data PUTs past the ratio.** Rejected by rule 14: an acked write whose
  segment PUT is refused is lost. Backpressure belongs upstream, which lane
  admission (ADR-0074) provides with a 429.
- **Govern GETs by ratio.** Deferred: a store-level refusal would turn a cache
  miss into a consumer stall. GETs are counted and priced, not refused.
- **A self-clearing kill switch.** Rejected: research 15 §5 — "never continue
  silently". Halting discretionary work is safe (GC falls behind, prefetch
  misses become cold GETs); silently resuming after a 100× event is not.
- **A governor in each caller.** Rejected: a new LIST site would be ungoverned
  by default. A decorator governs every caller, including ones not yet written.

## Deferred, with an owner

FR-21's attribution is `(op, purpose, domain, index)`, and cost.md rule 16 says
to count per index in memory. **The INDEX half, `GET /admin/cost` and quota
enforcement (Q10) are re-homed to M11** by the roadmap's Deferred table. A store
request carries one SEGMENT of many indices, so an index's share is an
apportionment model — segment bytes by run length — not a count, and quotas need
that apportionment to enforce anything. M10 serves FR-21 partially and says so.

⚠️ **Amended by [ADR-0077](0077-an-index-cost-is-apportioned-by-run-bytes-from-the-segment-directory.md) and
[ADR-0078](0078-per-index-quotas-are-pod-local-token-buckets-on-admitted-bytes-and-records.md) (M11).** The
apportionment is by run bytes; quotas bound admitted bytes and records rather
than apportioned requests, because the cost of an admitted byte is known only
after the flush that bills it.

## Consequences

- A LIST regression anywhere, by any caller that did not declare recovery, is
  refused within seconds instead of billed for an hour.
- **NFR-3 and cost.md rule 15 are amended**: the ~1/s hard ceiling holds for
  every LIST except those four declared recovery paths, each bounded by its
  trigger instead. Declaring a fifth recovery path is a change to this ADR.
- The kill switch is sticky, and an operator clears it by restarting the pod.
