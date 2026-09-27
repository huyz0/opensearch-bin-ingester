# 0074. Priority lanes are carried per request and scheduled per run

Status: accepted
Date: 2026-09-27
Requirements: FR-18, NFR-1
Research: docs/internal/product/decisions/0014-priority-lanes.md, docs/internal/product/decisions/0025-reserve-the-lane-byte-in-the-segment-directory-entry.md

## Context

ADR-0014 decided WHAT a lane is — a signed `i8` bucket, 0 the default,
negative background, positive elevated, scheduling and never order — and
ADR-0025 reserved the byte in the segment directory entry, where it has been
written as 0 since M3. What was left to M10 is HOW a value reaches that byte
and what each pipeline stage does with it. Four facts constrain the answer:

- `RunKey` is `(indexId, partitionId)`: one partition has one offset space and
  one run per segment, and the directory is binary-searched by that key.
- The producer frame is OpenSearch `_bulk` NDJSON, whose action metadata this
  ingester refuses unknown keys in (M1: an ignored key is a silently misrouted
  document).
- There is one accumulator per pod (non-negotiable 6: request rate scales with
  flushes, never with lanes, indices or partitions).
- NFR-1 (ADR-0072) bounds a low-rate pod at ≤ 2 data+commit PUTs per interval
  CEILING (5 s by default). A lane that flushed at the 250 ms FLOOR would spend
  up to 40 in that window — 20× the bound — so a positive lane's speed-up is a
  cost that must be bounded and stated, not assumed free.

## Decision

1. **Carried per request**: a `lane` query parameter on `POST /{index}/_bulk`,
   next to the existing `partition` and `routing`. Absent is 0. It must be an
   integer in the pod's ACTIVE SET (default `-2..2`; at most 8 lanes, the FR-18
   cap, and no lane above `+2`, both refused at configuration — the second is
   what keeps the cost bound below a number, since `+l` spends up to `2^l`
   flushes per ceiling). Anything else is `400`. Every record of the
   request carries that lane — so ADR-0014's "same document ⇒ same lane" holds
   for any producer that sends a document's mutations in one request.
2. **Recorded per run, as the MAXIMUM.** A run's lane byte is the highest lane
   of the records in it. A lane never splits a run, so offsets, the directory
   key, the data layout and the reader are unchanged; a partition mixing lanes
   in one segment is scheduled at its most urgent member's pace.
3. **Flush deadlines**, from the age of the OLDEST buffered record of each lane:
   - lane `+l`: `max(intervalFloor, adaptiveInterval ÷ 2^l)`;
   - lane `0`: the adaptive interval, as before;
   - lane `< 0`: the interval CEILING, which is also its anti-starvation bound.
   The flush is due when any buffered lane's deadline has passed (or the
   segment is full). A lane-0-only pod behaves exactly as before.
4. **The cost is bounded and amends NFR-1 with the number.** A lane `+l`
   deadline is never shorter than `ceiling ÷ 2^l` at low rate, so while lane
   `+L` records are buffered a pod flushes at most `2^L` times per ceiling:
   **≤ 2 × 2^L data+commit PUTs per pod per interval ceiling**, and since no
   active lane may exceed `+2`, never more than 8. With no positive-lane traffic the bound stays at 2.
   An operator buys latency with PUTs knowingly, by enabling a lane.
5. **Push** follows lanes: a flushed segment's runs are pushed to subscribers
   highest lane first.
6. **Admission** is weighted fair share with a floor over an in-flight budget
   (`ingest.admission.maxInFlightBulk`, default 256 concurrent `_bulk`
   requests). Lane `l`'s share is `budget × 2^l ÷ Σ_active 2^k`, its floor
   `max(1, ⌊share ÷ 2⌋)`. A request is admitted if the pod is under budget or
   its lane is under its floor; otherwise `429`, never 5xx.

### Amendments to ADR-0014, stated

- **No overtaking within a partition.** ADR-0014 says a `+1` record "naturally
  receives the lower offset" than an earlier `−1` record. That needs two runs of
  one partition in one segment. With one run per partition, a lane changes WHEN
  a partition's records commit and never their order within the partition.
  Across partitions — separate offset spaces — a lane still decides which
  commits first.
- **No lane-ordered data layout.** ADR-0014's "runs placed first" pays only for
  a reader fetching a prefix; every reader here fetches the whole object
  (ADR-0044, ADR-0073), and reordering data would break the key-contiguity
  `SegmentWriter` keeps for a future ranged read (M5.66).
- **The active set is per pod, not per index.** The catalog registering index
  shapes carries no lane configuration; per-index sets wait for one.
- **The "client queue drained first" stage is not built.** `readNext` returns
  records in offset order, and the offsets already carry the lane's effect.

## Alternatives considered

- **A per-action `lane` metadata key.** Rejected for now: records of one request
  would split into several appends per chunk, and a document's mutations could
  cross lanes inside one request — the hazard ADR-0014 says the ingester cannot
  detect. A per-action key can be added later without a format change.
- **A lane component in `RunKey`**, giving each `(index, partition, lane)` its
  own run. Rejected: two runs of one partition in one segment need a rule for
  which receives the lower offsets — reordering after all — and the directory's
  binary search would need a composite key: a wire-format change.
- **Positive lanes flush at the floor.** Rejected on the number above: 20× NFR-1
  at a 5 s ceiling. `interval ÷ 2^l` keeps the speed-up proportional to the
  lane and the cost bounded by the highest ACTIVE lane.
- **An accumulator per lane.** Rejected: up to 8× the flushes at low rate — a
  request rate scaling with lanes, which non-negotiable 6 forbids.
- **Strict priority at admission.** Rejected by ADR-0014's anti-starvation
  section: a sustained positive load would make negative data invisible.
- **Negative lanes flush only when something else does.** Rejected: the
  unbounded-latency symptom ADR-0014 calls worse than backpressure. The ceiling
  is the bound, and it is NFR-7's divisor (ADR-0063).

## Consequences

- Lanes are inert until a producer sends one; every existing request is lane 0.
- NFR-1's low-rate bound gains a lane term, counted by M10 criterion 9.
- The governor's expected data-PUT rate is taken at the interval FLOOR
  (ADR-0075), so lane-forced flushes never read as a regression.
- ADR-0014's limitation stands: a lane accelerates reaching commit and cannot
  jump anything already committed.
