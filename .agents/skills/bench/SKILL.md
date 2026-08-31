---
name: bench
description: Measure before optimising, and prove an optimisation worked. Use when a change touches a hot path, when adding or changing a JMH benchmark, when a latency or throughput number is claimed, or when tempted to optimise anything.
---

# Benchmarking

Full methodology and the candidate benchmark list:
[`docs/research/40-implementation/03-benchmarking-plan.md`](../../../docs/research/40-implementation/03-benchmarking-plan.md).

## Profile before optimising

⚠️ **Do not write a micro-benchmark on a hunch.** Build the macro harness, find
the top three hot spots with JFR or async-profiler, and *then* write JMH
benchmarks for those. Micro-benchmarking on intuition is how a week goes into
making a 0.3% path 40% faster.

## JMH rules

- `@Fork(≥2)`, warmup ≥5 iterations.
- Consume every result with `Blackhole`. Never rely on a return value to defeat
  dead-code elimination.
- **`-prof gc` always.** Allocation rate (`gc.alloc.rate.norm`, B/op) is a
  first-class result, not a footnote: **a change that is 5% faster and allocates
  2× more is a regression here**, because GC pressure is the enemy at 100 MiB/s.
- Realistic inputs: real JSON documents with a realistic size distribution — the
  long tail matters, 200 B API logs behave nothing like 50 KB stack traces.
- Report distributions, not means. Object-store latency is fat-tailed and the
  p99 is what sets the flush interval.
- Pin the environment (JDK build, `-XX:+AlwaysPreTouch`, no co-tenancy) and
  record it with the result.

## The three tiers

| Tier | What | Gate |
|---|---|---|
| **Micro** (JMH) | framing, buffers, compression, checksum, hashing, filters, directory lookup | no regression beyond noise |
| **Macro** | end-to-end throughput and latency across store backends and flush intervals | the cost/latency curve |
| **Cost** | requests per MiB, idle request rate | [`cost-budget`](../cost-budget/SKILL.md) |

## Claiming a number

⚠️ **Never state a measured number you did not measure**, and never present a
modelled number as measured. Several figures in the research corpus are
explicitly modelled and say so; keep that distinction. If you model, label it,
and say what would falsify it.

## Anti-patterns

- Benchmarking only against `MemoryBinStore`, then being surprised by S3 tail latency.
- Optimising the record path while segment assembly copies everything twice.
- Reporting throughput without allocation rate.
- A "1M docs/sec" number on 100-byte documents with one partition.
