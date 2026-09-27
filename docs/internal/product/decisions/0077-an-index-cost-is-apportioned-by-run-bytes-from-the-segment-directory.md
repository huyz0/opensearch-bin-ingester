# 0077. An index's cost is apportioned by run bytes, from the segment directory

Status: accepted
Date: 2026-09-27
Requirements: FR-21, NFR-4
Research: docs/research/30-design-space/15-cost-governor.md §4

## Context

FR-21's attribution is `(op, purpose, domain, index)`. M10 shipped the first
three ([ADR-0075](0075-the-cost-governor-refuses-discretionary-work-and-never-a-write.md))
and re-homed the index half to M11, because a store request carries one
SEGMENT holding many indices: the ingester issues one data PUT, one commit PUT
and, on the read side, one GET per segment, never one per index (cost.md rule
1, non-negotiable 6). An index's "share" of such a request is therefore not a
count that exists anywhere; it is a model, and it has to be chosen.

Research 15 §4 fixes what the model must not cost: counters are kept **in
memory** per index, reach an operator through **top-K log events and
`GET /admin/cost`**, and are **never** a metric label (cost.md rule 16,
observability.md rule 1: an `index` label is 200,000 series).

Three things are known exactly at the moment a request is issued:

- **At flush**, the drained segment's directory: one `RunEntry` per stream, with
  its `indexId` and `byteLen`.
- **At a whole-segment GET**, the same directory, once the bytes are in hand.
- **Neither** for a GET that streams an object larger than the node's cache
  hold, whose bytes are never materialised; nor for the LIST, lease, registry
  and GC requests, which belong to no index.

## Decision

1. **An index's share of a segment request is its runs' bytes over the
   segment's run bytes**, read from the segment's own directory
   (`SegmentReader.directory()`), on the write side and the read side alike. One
   source of truth, the bytes that were billed, rather than a second
   estimate kept beside them.
2. **Shares are kept in integer micro-requests** (10^6 per request) and rounded
   by **largest remainder**, so the shares of one request sum to exactly
   10^6. The per-index totals therefore sum EXACTLY to the counted requests
   they apportion — a checkable invariant, not an approximation that drifts.
3. **What is apportioned:** the data PUT and the commit PUT of every flush
   (`purpose` data and commit) — ⚠️ **the commit half is corrected by M11.2's
   spec amendment and decided in M11.22**: a commit PUT carries a batched delta
   from any pod, not one flush, so it is apportioned where the delta is PUT,
   by the delta's record counts — and **every GET of a data segment the ingester
   issues**, whoever issues it: `SegmentProxy`'s reads (the proxy route, the
   subscription path, the prefetcher) and `DurableCatchUpResponder`'s
   whole-segment reads, each split by the directory of the bytes it already
   holds. Bytes ingested per index are the run byte lengths.
4. **What is not apportioned goes to a named `unattributed` bucket**, never
   dropped and never guessed: a data-segment GET whose bytes were streamed
   without being held (larger than the hold), failed, or did not decode. LIST,
   lease, registry, checkpoint and GC requests belong to no index and are
   reported beside the per-index table as pod totals, by purpose.
4a. **The denominator is counted where the requests are.** `CountingBinStore`
    counts GETs by the same data-segment classifier it already uses for PUTs
    (`isDataSegment`), so "the counted data-segment GETs" is a number the store
    stack keeps, not one the ledger infers, and Σ index GET shares +
    `unattributed` = counted data-segment GETs × 10^6 is checkable against it.
5. **In memory, per pod, keyed by index id**, a `LongAdder` set per index,
   resolved to the index NAME only when reported (the catalog maps id → name).
   The map is bounded by the indices registered on the pod in its lifetime;
   the counters are process-local and reset with the pod, as every other
   counter in this tree does.
6. **Priced by the pod's `CostTable`** (ADR-0075's `CostMeter`), and labelled an
   estimate: counts are exact, prices are a lookup.
7. **Reported two ways, zero metric cardinality:** `GET /admin/cost?by=index&top=N`
   on the ingester, and a periodic top-K log event.

## Alternatives considered

- **Apportion by record count.** Rejected: a PUT is billed per request and a
  segment is sized by bytes (the size trigger), so bytes are what fill a
  segment and bring the next PUT forward; one index of 1 MiB documents and one
  of 100 B documents with equal record counts would read as equal cost.
- **Apportion equally among the indices in a segment.** Rejected: a trickle
  index sharing a segment with a firehose would read as half the bill, which
  sends the operator to the wrong index — the failure research 15 §4 exists
  to prevent.
- **Charge each index a whole request.** Rejected: the per-index totals would
  sum to K times what was billed, and "top indices by request count" would be
  the list of indices that share segments, i.e. all of them.
- **Keep a running byte estimate in the accumulator instead of reading the
  directory.** Rejected: `estimatedFramedBytes` is an estimate for the size
  trigger, and a second estimate beside the billed bytes is a second source of
  truth that the read side, which never saw the accumulator, could not share.
- **Floating-point shares.** Rejected: the sums drift, so the invariant in
  decision 2 could not be asserted, and a report whose rows do not sum to its
  total invites the question of which one is wrong.
- **Export per-index series.** Rejected by cost.md rule 16 and observability.md
  rule 1, with the number: 200,000 series.
- **Apportion streamed GETs by a HEAD or ranged read of the directory.**
  Rejected: it adds a store request to attribute one, the one thing this
  record may not do (NFR-4). They are counted as `unattributed` instead, and
  the bucket's size is itself the signal that the hold is smaller than the
  working set (M10.25's number).

## Consequences

- FR-21's `index` dimension exists, and an operator can name the index behind
  a cost regression without a metric label.
- Attribution adds **zero store requests** and one directory walk per flush
  and per held GET, both already in memory.
- The read side is attributed only on the ingester. The plugin's own `direct`
  GETs are billed to the OpenSearch deployment's credentials and are not
  counted by the ingester at all; that is unchanged.
- A share is a model, and says so in the report; "index X cost $Y" means
  "index X's bytes were Z% of segments that cost $Y".
