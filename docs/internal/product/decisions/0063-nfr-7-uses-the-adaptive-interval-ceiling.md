# 0063. NFR-7 Uses the Adaptive-Interval Ceiling

Status: accepted
Date: 2026-09-22
Requirements: NFR-7
Research: docs/research/40-implementation/03-benchmarking-plan.md

## Context

NFR-7 requires end-to-end p99 below three times the configured flush window.
The writer has an adaptive interval with both a floor and a ceiling, so
"configured flush window" is ambiguous: the floor is the fastest possible
flush, while the ceiling is the longest interval an operator permits. A
latency result divided by the floor could pass while the writer is operating
at its permitted five-second ceiling, and a result divided by the current
instantaneous interval would move its denominator during one run.

The latency endpoints are also part of the requirement. The start is the
producer's accepted 202 response, and the end is the same record becoming
available through the consumer library. This includes the durable commit and
the subscription delivery, but excludes a later OpenSearch search; the
searchable point has its own criterion.

## Decision

Use the configured adaptive interval **ceiling** as NFR-7's divisor. Every
latency point reports:

```
p99(producer 202 -> consumer record delivery) < 3 * intervalCeiling
```

The harness must run at the named ceilings of 250 ms, 1 s and 5 s, and must
record the ceiling beside each result. A result measured on RustFS is labelled
as a local-store lower bound; the S3 tail remains not-run until an AWS fixture
exists.

## Alternatives considered

- **Divide by the interval floor.** Rejected because a 5 s operating point
  would still be judged against 250 ms, mixing the fastest dial with the
  writer's permitted worst case and producing a false failure.
- **Divide by the instantaneous interval.** Rejected because adaptive changes
  during a point make the denominator workload-dependent; two identical p99s
  could pass or fail solely because one run shortened its interval before the
  sample was recorded.
- **Divide by the current interval at each record.** Rejected for the same
  reason, and because a percentile of ratios is not the stated latency
  percentile. It would make the acceptance calculation irreproducible from a
  single result distribution.

## Consequences

The threshold is conservative and stable: the 250 ms, 1 s and 5 s points are
judged against 750 ms, 3 s and 15 s respectively. The ceiling is a test
parameter, not a change to the adaptive algorithm. A future change to the
latency endpoint or divisor requires amending this ADR and NFR-7's evidence
rather than silently changing the curve's denominator.
