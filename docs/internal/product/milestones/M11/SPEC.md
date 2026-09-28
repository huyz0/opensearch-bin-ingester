# M11 — Per-index cost attribution, `/admin/cost` and quotas

## Completion condition

The roadmap's M11 row, restated: **every store request the ingester issues is
governed and counted, and the governor is exported; each index's share of the
data PUTs and segment GETs is apportioned by run bytes, and of the commit
PUTs by the delta's record counts, summing exactly to the counted requests, and reaches an operator through
`GET /admin/cost` and a top-K log event with zero per-index metric series;
per-index quotas refuse with `429` and `Retry-After` before a body is read,
and every `429` carries `Retry-After`; and M10's open rows and review harvest
are closed.**

M11 collects the obligations M10 handed it:

| Obligation | Assigned by |
|---|---|
| Per-index attribution, `GET /admin/cost`, quota enforcement (the FR-21 remainder) | roadmap § Deferred, ADR-0075 § Deferred, M10 SPEC § Not in scope |
| Open rows M10.24–M10.28, M10.30, M10.34, M10.36 | M10/VERIFIED.md § Harvest, M10.35 |
| Harvest H2–H15 | M10/VERIFIED.md § Harvest for the specification of M11 |

⚠️ **M11 TAKES THE FR-21 REMAINDER, NOT FAST MODE.** The roadmap gave M11 both
and M10's review said "take one"; it recommended the remainder, in the order
used below: the file splits first, then the uncounted store paths and the
export (per-index counts over a stack with raw, uncounted paths would
apportion a wrong total), then attribution, `/admin/cost` and quotas. Fast
mode (FR-17) moves to **M12**, unchanged: "after the default path is measured"
still holds, and M10.25 and M10.28 — which bound the read path's request rate
under failure — land here first.

⚠️ **THE RIG IS M9's RIG** (ADR-0060): one workstation, Docker, the
digest-pinned RustFS. Only M10.34's K > 1 measurement and H14 need it.

## Requirements

| ID | What M11 does to it |
|---|---|
| **FR-21** (cost governor) | **Completed**: the `index` dimension of `(op, purpose, domain, index)` by apportionment ([ADR-0077](../../decisions/0077-an-index-cost-is-apportioned-by-run-bytes-from-the-segment-directory.md)); `GET /admin/cost`; the governor exported; every ingester store path governed (M10.26) |
| **NFR-16** (zero steady-state refusals) | Made observable on a fleet: `binstore_governor_refusals_total` exported (M10.27) |
| **NFR-3** (LIST ceiling) | The commit route's inbox drain — a declared recovery LIST — becomes governed and counted (M10.26) |
| **NFR-4** (read rate) | Preserved: attribution adds zero requests; the plugin's re-fetch fall-back becomes a counted number (M10.25); a failing segment's retries are bounded per node, not per run (M10.28) |
| **NFR-5** (cross-AZ bytes) | The per-stream event floor is decided by ADR and measured at K > 1 (M10.34) |
| **ADR-0010 mechanism 1** (per-index admission rate limit) | Enforced: pod-local token buckets on bytes and records, `429` + `Retry-After` ([ADR-0078](../../decisions/0078-per-index-quotas-are-pod-local-token-buckets-on-admitted-bytes-and-records.md)) |
| **FR-13, FR-16** | An explicit-partition `_bulk` to an unregistered index answers `503`, as the routed path does (M10.30) |

## Scope

- Split `Assembly` and `ConsumerClient` below ~600 lines, no behaviour change (H3).
- The front door's raw-store paths through the governed node store (M10.26).
- The governor's metrics and the refused-sweep log line (M10.27).
- `IndexCostLedger`: write-side and read-side apportionment by run bytes.
- `GET /admin/cost?by=index&top=N` and a periodic top-K log event.
- `Retry-After` on every `429` from one place; no admission permit held across
  the durable-ack wait (H2).
- Per-index quotas, configured per pod.
- M10's open rows and H4–H15, each fixed by a named test or dropped with its
  reason in its row.

### Not in scope

- **Fast mode (FR-17)** — M12, by this spec's re-homing in the roadmap.
- **Compaction (FR-14)** — unchanged, deferred.
- **Fair-share buffer allocation and the segment share cap** (ADR-0010
  mechanisms 2 and 3). Lane admission (ADR-0074) and quotas bound what one
  index admits; the buffer's per-index share is a later milestone's.
- **Quota carried in the index registration** — ADR-0078 rejects it for M11.
- **Attributing the plugin's `direct` GETs.** They are billed to the
  deployment's credentials and never pass through the ingester (ADR-0077).
- **Per-index metric series.** Never (cost.md rule 16).

## Design

### Attribution ([ADR-0077](../../decisions/0077-an-index-cost-is-apportioned-by-run-bytes-from-the-segment-directory.md))

`IndexCostLedger` (in `binstore-spi`, keyed by index id so it needs no
format type) holds, per index id, `LongAdder`s of micro-requests (10^6 per
request) by charge — `(op, purpose)` — and of bytes written, plus an
`unattributed` bucket. `apportion(charge, weights)` splits 10^6 across the
weights' indices with largest-remainder rounding, so the shares sum exactly.
The publisher calls it for each data PUT with the drained segment's run bytes
per index (M11.2). `CommitChargingBinStore`, between the governor and the
counter, charges every commit-log PUT by the delta's record counts (M11.22).

⚠️ **AMENDED BY M11.2: THE COMMIT PUT IS NOT PER FLUSH.** The leader batches
commits into one delta per window (M8.50), from every pod, and a follower's
commit is PUT by the leader, so "each flush's commit PUT" names a request that
does not exist. The commit PUT is apportioned where it is ISSUED — the pod
whose `CommitLog` PUTs the delta — across the delta's runs by their record
counts, the only per-index weight a delta carries; a delta with no runs (a
seal) is `unattributed`. That is M11.22, with ADR-0077 amended there.

On the read side EVERY
data-segment GET the ingester issues is apportioned: `SegmentProxy` for a
whole segment it held, `DurableCatchUpResponder` for the segment it reads
whole; one streamed without being held, or one that failed or did not decode,
is charged to `unattributed`. `CountingBinStore` gains a data-segment GET
count by its existing `isDataSegment` classifier, so the read-side invariant
has a counted denominator. The ledger never touches the store.

`GET /admin/cost?by=index&top=N` (`AdminCostService`, `http`): `top` 1–1000,
default 50, else `400`; `by` must be `index`, else `400`. The answer is JSON:
the pod, the price table's name, the pod totals by `(op, purpose)` from
`CountingBinStore`, the `unattributed` requests, and the top N indices by
estimated USD, each with its name (from the catalog; the id if unregistered),
its bytes, and its apportioned requests by `(op, purpose)`.

The top-K log event (`CostTopKReporter`) emits one line per interval (default
5 min, `0` disables) naming the top 3 indices by estimated USD; it runs on the
node's scheduler with an injected clock.

### Admission (H2, [ADR-0078](../../decisions/0078-per-index-quotas-are-pod-local-token-buckets-on-admitted-bytes-and-records.md))

One method in `BulkService` answers every `429`, and sets `Retry-After`. The
lane admission permit is released once the request's records are BUFFERED,
before the durable-ack wait: `Ingest.append` takes a `buffered` callback, run
once the records are held and before the wait ⚠️ (amended by M11.7: a callback,
not the "handle the caller awaits" first written here). The permit is taken
again before a later chunk's first record is parsed, WAITING rather than
refusing. So the in-flight budget bounds requests parsing and buffering --
load -- rather than producers parked on the flush. ⚠️ And because the permit
was also the only bound on buffered memory, `DefaultIngest` holds at most four
segments' worth buffered and not yet durable, an append past it waiting for
the flush in flight ([ADR-0079](../../decisions/0079-admission-bounds-load-and-an-unflushed-bytes-ceiling-bounds-memory.md)).

`IndexQuotas` (in `ingest`) maps index name → two debt token buckets, lazily
created from the default or the index's override; a request is admitted when
both are non-negative AND the index holds fewer than `maxInFlightPerIndex`
admitted requests (default 8, ADR-0078 decision 2a), charged per appended
chunk, and refused with the seconds until the deeper debt is repaid (or 1 when
refused by the in-flight cap).

### The carried rows

Each is specified by its own backlog row (M10.24–M10.28, M10.30, M10.34,
M10.36) and keeps its ID, as M5.63 did in M10. M10.34's row names the three
options; the task chooses by ADR and measures K > 1.

## Cost impact

| Change | Store requests |
|---|---|
| Attribution, `/admin/cost`, top-K log | **none**: in-memory counters over requests already issued |
| M10.26 | none added; the inbox drain LIST and the absent-key `stat` become counted and governed (the LIST as declared recovery, so never refused) |
| Quotas and `Retry-After` | none; a refused request costs no parse and no buffer |
| Permit released before the durable wait | none: admission is not a store path; flush cadence is unchanged |
| M10.28 | **fewer**: a failing segment's route/store GETs bounded per node, not per run |
| M10.34 | cross-AZ BYTES, not requests; decided by its ADR |

NFR-1's bound and NFR-3's ceiling are unchanged. No budget moves.

## Acceptance criteria

1. **The two largest files are split**: `Assembly.java` and
   `ConsumerClient.java` are each below 600 lines, with no behaviour change —
   the full `./gradlew test` is green after the split. (M11.1)
2. **The front door's store paths are counted and governed**, T1: the commit
   route's inbox drain LIST and the fetch route's absent-key `stat` appear in
   the node's `storeCounts()`; the drain runs in the declared recovery scope,
   so it is counted and never refused with the LIST bucket drained. (M10.26)
3. **The governor is exported**, T1: each refusal increments
   `binstore_governor_refusals_total` with allow-list labels only; the ratio and
   alarm are gauges reading the live governor; a refused retention-sweep inbox
   read logs one line. (M10.27)
4. **Write-side apportionment is exact**, T0 and T1: one flushed segment's data
   PUT is split across its indices by run bytes to within one micro-request,
   and each split sums to exactly 10^6; over a multi-index workload through
   `DefaultIngest`, Σ per-index data-PUT shares equals the counting store's
   data PUTs × 10^6, and per-index bytes equal the directory's run bytes
   (M11.2); every delta PUT a pod's commit log issues, lost races and seals
   included, is split across the delta's runs by record count or charged to
   `unattributed`, and Σ equals the counting store's commit PUTs × 10^6
   (M11.22)
5. **Read-side apportionment is exact**, T1: `CountingBinStore` counts
   data-segment GETs apart from other GETs; a held whole-segment GET through
   `SegmentProxy` and a catch-up read through `DurableCatchUpResponder` are
   each split by the directory of the bytes read; one streamed without being
   held is charged to `unattributed`; over a workload exercising all three,
   Σ index GET shares + unattributed equals the counted data-segment GETs ×
   10^6, concurrent joined readers included (one GET, one apportionment).
   (M11.3)
6. **Attribution adds no request and no series**, T1: the same workload issues
   identical `StoreCounts` with the ledger attached and without it; the
   metric-cardinality gate stays green (no `index` label). (M11.2, M11.4)
7. **`GET /admin/cost` answers from the ledger**, T1: the top N by estimated
   USD, in order, with names from the catalog, bytes, and apportioned requests;
   pod totals and `unattributed` present; `top` outside 1–1000 or `by` other
   than `index` is `400`; T2 on the assembled pod after real writes, the
   route's per-index data PUTs sum to the pod's counted data PUTs. (M11.4)
8. **The top-K log event**, T0: at each interval of an injected clock, exactly
   one line naming the top 3 indices by estimated USD; none before the first
   interval; none when disabled. (M11.5)
9. **Every `429` carries `Retry-After`**, T1: the lane admission refusal and the
   quota refusal both answer `429` with an integer `Retry-After` ≥ 1, set by
   one method. (M11.6)
10. **No admission permit is held across the durable-ack wait**, T1: with an
    in-flight budget of 1 and a flush held on a latch, a second request is
    admitted while the first waits for durability, and both are `202` once
    the flush completes. (M11.7)
11. **Per-index quotas**, T0 and T1: an index past either bucket is refused
    `429` with `Retry-After` equal to its debt's repayment time, rounded up,
    before its body is read; another index on the same pod is admitted; a
    request admitted with tokens may take the bucket into debt and is never
    cut off; K > `maxInFlightPerIndex` CONCURRENT requests to a quota'd index
    with a full bucket admit at most `maxInFlightPerIndex`; with no quota
    configured, nothing is refused. (M11.8)
12. **M10's carried rows are closed**: M10.24, M10.25, M10.28, M10.30 and
    M10.36 each by the named test in its row. (M10.24–M10.36)
13. **NFR-5's event floor is decided and measured**: an ADR chooses among
    M10.34's options, and a T3 run on RustFS measures cross-AZ bytes with
    K > 1 cross-zone streams per segment against its stated bound. (M10.34)
14. **M10's harvest is closed**: H2–H15 each fixed by a named test or dropped
    with its reason in the backlog row. (M11.1, M11.6, M11.7, M11.9–M11.20)

## Test plan

| Criterion | Tier | First failing test | Mutation it must kill |
|---|---|---|---|
| 1 | — | the full suite, green | — |
| 2 | T1 | `FrontDoorGovernedStoreTest` | the raw backend handed to the drain or the fetch route |
| 3 | T1 | `GovernorMetricsTest` | counter not incremented; gauge snapshotting a stale value |
| 4 | T0, T1 | `IndexCostLedgerTest`, `DefaultIngestAttributionTest` | equal split; floor rounding losing micro-requests; bytes from the estimate |
| 5 | T1 | `SegmentProxyAttributionTest`, `CatchUpAttributionTest`, `CountingBinStoreGetPurposeTest` | apportioning per joined caller; dropping streamed GETs; a catch-up GET left out; data GETs counted with other GETs |
| 6 | T1 | `DefaultIngestAttributionTest#theLedgerIssuesNoRequest` | a stat per apportionment |
| 7 | T1, T2 | `AdminCostServiceTest`, `AdminCostAssemblyTest` | ordering by bytes; `top` unbounded |
| 8 | T0 | `CostTopKReporterTest` | a line per tick; firing when disabled |
| 9 | T1 | `BulkServiceRetryAfterTest` | a `429` without the header |
| 10 | T1 | `AdmissionDurableWaitTest` | permit released after the append returns |
| 11 | T0, T1 | `IndexQuotasTest`, `BulkServiceQuotaTest` | per-pod instead of per-index bucket; refusal after body read; cut-off mid-request; the in-flight cap dropped (concurrent admission past it) |
| 12–14 | per row | named in each row | named in each row |

Suites extended: the cost assertions (criterion 4–6's exact sums are cost
assertions); the store conformance and simulation suites are untouched.

## Risks

- **Apportionment is a model an operator may read as a count.** The route and
  log line label shares and prices as apportioned and estimated.
- **Releasing the permit before the durable wait lets more producers park.**
  Parked producers hold no buffer beyond what `maxUnflushedBytes` already
  bounds, so memory stays bounded (NFR-6); that is the argument M10's F2 made.
- **A quota configured too low refuses a legitimate backfill.** Inert by
  default, and every refusal names its index and the time to retry.
- **M10.34 may conclude NFR-5 needs restating.** That is a requirement change
  and its ADR says so with the number, never a threshold quietly moved.

## Decisions

Recorded as ADRs in this commit: ADR-0077 (apportionment by run bytes) and
ADR-0078 (per-index quotas, amending ADR-0075's Deferred paragraph). M10.34's
ADR lands with its task.

## Tasks

| ID | Task | Serves |
|---|---|---|
| M11.0 | This spec, its decomposition, ADR-0077 and ADR-0078, the roadmap's M11 row and fast mode re-homed to M12 | — (planning) |
| M11.1 | Split `Assembly` and `ConsumerClient` below ~600 lines, no behaviour change (H3) | — (structure) |
| M10.26 | The front door through the governed node store (carried) | FR-21, NFR-3 |
| M10.27 | Export the governor (carried) | FR-21, NFR-16 |
| M11.2 | `IndexCostLedger` and data-PUT apportionment | FR-21 |
| M11.22 | Commit-PUT apportionment where the delta is PUT, by record count (added by M11.2) | FR-21 |
| M11.3 | Read-side apportionment: data-segment GETs counted apart; `SegmentProxy` and catch-up reads split | FR-21, NFR-4 |
| M11.4 | `GET /admin/cost?by=index&top=N`, wired in the front door | FR-21 |
| M11.5 | The periodic top-K cost log event | FR-21 |
| M11.6 | `Retry-After` on every `429`, from one place (H2) | FR-21 |
| M11.7 | No admission permit across the durable-ack wait (H2) | FR-18, NFR-6 |
| M11.8 | Per-index quotas | FR-21 |
| M10.30 | Unregistered index on the explicit-partition path answers `503` (carried) | FR-13, FR-16 |
| M10.25 | Count the plugin's re-fetch fall-back (carried) | FR-6, NFR-4 |
| M10.28 | A failed segment fetch retried once per node, not per run (carried) | FR-6, NFR-4 |
| M10.36 | Close Helidon's connection-return race on the other shared clients (carried) | FR-10 |
| M10.24 | Root-cause `SubscriptionChannelTest` under jzap (carried) | — (quality) |
| M10.34 | NFR-5's event floor: ADR and K > 1 measurement (carried) | NFR-5 |
| M11.9 | H4: pin the lane seam | FR-18 |
| M11.10 | H5: flush coordinator hardening and pins | FR-4 |
| M11.11 | H6: governor residue | FR-21 |
| M11.12 | H7: Tier-2 reader residue | FR-10 |
| M11.13 | H8: consumer retry residue | FR-6 |
| M11.14 | H9: proxy-path pins | FR-6 |
| M11.15 | H10: test-timing margins | — (quality) |
| M11.16 | H11: `PushQueue` pins | FR-6 |
| M11.17 | H12: a fresh all-lane-0 write equals `segment-v1.bin` | FR-18 |
| M11.18 | H13: hygiene residue | — (quality) |
| M11.19 | H14: one green `PartitionVisibilityIT` run on an unloaded rig | — (evidence) |
| M11.20 | H15: scope `checkMilestoneVerified` to the criteria section; pin its default | — (harness) |
| M11.21 | Close M11: VERIFIED.md, roadmap row, the milestone gate's default | — (evidence) |
