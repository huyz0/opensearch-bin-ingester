# 0062. NFR-1 has an interval-bound low-rate regime

Status: superseded by 0072 (2026-09-26)
Date: 2026-09-22
Requirements: NFR-1
Research: docs/research/00-problem/02-cost-model.md; docs/internal/product/decisions/0017-every-pod-writes.md

## Context

NFR-1's original single target, `< 0.30 requests per MiB ingested`, is the
right invariant while the workload fills segments by size. It is not a useful
bound below that rate: a small workload can write the same number of requests
with very different bytes per request, so the ratio measures the workload
rather than the flush policy.

M9.8 measured the size-triggered regime on the assembled RustFS fleet. At 40,
80 and 160 MiB/s it recorded 35 PUTs at each point, with 0.2714, 0.2744 and
0.2722 PUTs/MiB respectively, and zero LISTs. Those counts confirm that the
original ratio is a size-regime assertion; they do not justify applying it to a
trickle workload.

The alternative of expressing the low-rate bound per second is not stable. At
a 250 ms ceiling, correct interval behaviour can produce eight write requests
per second (one data PUT and one commit PUT in each interval), while a 5 s
ceiling implementation flushing five times too often could still pass a
2-requests-per-second check. The same code would therefore be judged by two
different standards solely because an operator changed a dial.

## Decision

Amend NFR-1 to two regimes:

1. Above the rate at which flushes are size-triggered, write requests remain
   **strictly below 0.30 per MiB written**.
2. Below that rate, a pod issues **at most two write requests per interval
   ceiling**: one data PUT and one commit PUT. The bound is evaluated over
   elapsed interval-ceiling windows, never converted to a per-second target.

The bound is asserted at both a 250 ms ceiling and a 5 s ceiling by
`LowRateWriteBudgetIT`. Its smoke evidence is 24 PUTs in 3.07 s at 250 ms and
4 PUTs in 5.05 s at 5 s, with zero LISTs. The full M9 measurement records the
five-minute points separately; the smoke duration is not substituted for that
criterion.

The server configuration accepts `ingest.interval-ceiling` so the two test
operating points pin floor and ceiling to the same value. This is a testable
configuration seam, not a production-default change; the default remains the
`IngestConfig` five-second ceiling.

## Alternatives considered

- **Keep the unconditional requests-per-MiB bound.** Rejected: it falsely
  prices a trickle by payload size and cannot distinguish a healthy interval
  flush from an undersized segment.
- **Use a fixed requests-per-second bound.** Rejected: it changes meaning when
  the interval ceiling changes. The 250 ms and 5 s counterexamples above make
  the gate either reject correct behaviour or accept five-times-too-frequent
  flushing.
- **Raise the size-regime threshold or relax `< 0.30`.** Rejected: M9.8's
  measured counts already satisfy the existing budget. Moving that threshold
  would weaken a cost gate rather than describe the two operating regimes.
- **Count only data PUTs.** Rejected: the commit PUT is a durable write and is
  part of the object-store bill; omitting it would make the low-rate assertion
  undercount the design it claims to govern.

## Consequences

NFR-1 remains unchanged for the high-throughput regime and gains a falsifiable
low-throughput assertion that is independent of the chosen interval dial. Cost
and performance documents must state both forms. The measurement job must
report the regime and ceiling with every point; a single requests-per-MiB
number for a low-rate point is not evidence of this requirement.
