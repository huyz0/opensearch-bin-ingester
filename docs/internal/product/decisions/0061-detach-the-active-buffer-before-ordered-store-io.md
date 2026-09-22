# 0061. Detach the active buffer before ordered store I/O

Status: accepted
Date: 2026-09-22
Requirements: FR-4, NFR-1, NFR-7
Research: docs/internal/product/decisions/0026-a-lone-producers-throughput-scales-inversely-with-the-interval.md

## Context

`DefaultIngest` held its accumulator lock across segment PUT, commit PUT and
waiter completion. The lock made the segment and its commit agree, but it also
made every producer wait behind object-store latency. The M9.8 RustFS probe
showed the consequence: with one producer, five-second points accepted only
about 0.29 MiB and spent about 126 PUTs/MiB; with the repository's 24-producer
shape, 128-document batches still produced timer-sized segments and about
3.19 PUTs/MiB before the measurement was corrected.

The lock is needed to mutate the active accumulator and to assign the flush
sequence. It is not needed while a detached accumulator is being serialized,
stored and committed, provided that only one worker performs those operations.

## Decision

Keep one active accumulator under the existing lock. When a flush is due, move
that accumulator and its pending append waiters into one flush batch, install an
empty accumulator carrying forward the adaptive interval state, and enqueue the
batch. A single dedicated flush worker performs segment PUT followed by commit
in queue order, then completes the waiters and signals the timer. At most one
batch is queued or in flight; producers therefore get one replacement buffer,
not unbounded detached memory.

An append is acknowledged only after its ordered worker has observed the
durable commit. `flushNow()` waits for the current worker batch and any active
batch it detaches. Shutdown drains the same queue before releasing the
sequencer lease.

## Alternatives considered

- **Increase the production flush floor or use a fixed 250 ms window.**
  Rejected as the implementation fix. ADR-0016/0017 make the adaptive interval
  the cost dial, and the fixed-window model costs about $311/month at 250 ms
  versus $15.55/month at a 5 s ceiling. The M9 fixture may choose a longer
  measurement operating point when RustFS cannot reach the 8 MiB trigger, but
  production defaults do not move here.
- **Allow several flush workers.** Rejected: concurrent commits would need an
  additional ordering protocol and could make an earlier segment's offsets
  visible after a later segment. That would change the commit protocol for no
  request-cost benefit.
- **Piggyback commits or remove commit PUTs.** Deferred by the existing W3
  decision: it is a wire/protocol change, not a local scheduling optimisation.
- **Keep the lock and rely on 24 producers.** Rejected by the measurement:
  the producer count hides the serialized store wait but does not remove it;
  one slow commit still prevents the active buffer from being filled.

## Consequences

The write path keeps exactly one segment PUT and one commit PUT per flush; this
decision adds no object-store operation and does not move the `<0.30`
requests/MiB budget. It reduces producer blocking and makes the size-triggered
regime reachable under the RustFS fixture. The worker and replacement buffer
add one bounded segment-sized live allocation and make shutdown/error handling
depend on draining the flush queue, both covered by unit tests.
