# 0079. Admission bounds load; an unflushed-bytes ceiling bounds memory

Status: accepted
Date: 2026-09-28
Requirements: FR-18, NFR-6
Research: docs/research/30-design-space/11-multi-tenancy-and-security.md §2

## Context

M10.8's lane admission (ADR-0074 decision 6) held a `_bulk` request's permit
for the whole request, the durable-ack wait included. M10's review (F2) found
that at a low rate -- where a flush is seconds away -- a budget of 256 capped a
pod at 256 producers PARKED on a flush: a cap on concurrency with nothing to
protect. M11.7 gives the permit back as each chunk buffers.

That permit was also, silently, the only bound on memory: with at most one
chunk outstanding per permit holder, records buffered and not yet durable were
bounded by `budget x APPEND_CHUNK_RECORDS`. Giving the permit back at buffered
removes that bound. Behind a slow or stalled data PUT, every release admits
another chunk, and the accumulator grows with the number of producers --
nothing in `DefaultIngest.append` blocks on buffer size, and `FlushCoordinator`
admits one batch at a time, so the active buffer grows behind the stalled one.
Research 11 §2 names the mechanism that was missing: a global unflushed-bytes
ceiling (`maxUnflushedBytes`).

## Decision

1. **The admission permit bounds LOAD -- requests parsing and buffering a
   chunk.** It is taken before a chunk's first record is parsed and given back
   when the chunk's records are buffered (`Ingest.append`'s `buffered`
   callback), so a producer parked on the durable wait holds none. A later chunk
   of an admitted request WAITS for a permit (`LaneAdmission.acquire`) rather
   than being refused: its prefix is durable-bound.
2. **A pod-wide ceiling bounds MEMORY.** `DefaultIngest` holds at most
   `UNFLUSHED_SEGMENTS` (4) x `maxSegmentBytes` buffered and not yet durable --
   the active buffer plus the batch in flight, measured from the accumulators
   themselves -- and an append past it WAITS, before buffering, until a flush
   completes. It waits only for a flush that will come (a batch in flight, or
   waiters the flusher will flush for), so records a throwing source left
   behind cannot wedge it. The ceiling has a floor of `MIN_UNFLUSHED_BYTES`
   (1 MiB): a segment budget of a few bytes -- a test configuration, never a
   production one -- would otherwise make every append behind a flush in
   flight wait for it, and the "due while a flush is queued" path
   (`DueWhileQueuedAppendTest`) could not be reached at all.
3. So an ingester node's buffered memory is bounded by `max(1 MiB, 4 x maxSegmentBytes)` plus at
   most one parsed chunk per admission permit -- 32 MiB plus `budget` chunks at
   the defaults -- independent of how many producers are connected (NFR-6).

## Alternatives considered

- **Keep the permit across the durable wait** (M10.8). Rejected by M10's F2: it
  caps concurrent producers, not load, and at a low rate that is a cap with
  nothing behind it.
- **An asynchronous append whose chunks the front door pipelines without
  waiting.** Rejected: a 256 MiB body would buffer whole before any of it was
  durable -- the memory M1.7b's chunking exists to prevent
  (`MemoryFlatUnderTenXBodySizeTest`).
- **Refuse, with a 429, past the ceiling.** Rejected: the append may be a later
  chunk of a request whose prefix is durable-bound. The first admission is where
  a refusal is cheap and safe, and it stays there.
- **Count per append, releasing on each waiter's completion.** Rejected: a
  `RecordSource` that throws part-way leaves records buffered with no waiter,
  so a per-append tally drifts; the accumulators' own byte counts cannot.
- **A configurable ceiling.** Deferred: 4 segments is twice the steady state
  (one buffering, one flushing), and a setting would be a knob with no measured
  reason to turn it.

## Consequences

- A producer waiting on a flush no longer holds an admission permit, so a pod at
  a low rate admits as many producers as connect.
- Behind a stalled store, appends wait at the ceiling instead of growing the
  heap; producers see latency, not an OutOfMemoryError. The ceiling is not a
  metric yet.
- The SPEC's "an append that returns once buffered with a handle the caller
  awaits" is realised as a callback run at buffered; ADR-0074 decision 6's
  permit lifetime is amended by decision 1 here.
