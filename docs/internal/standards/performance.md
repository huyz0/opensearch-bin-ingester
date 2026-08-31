# Performance

**Family:** Quality
**Read when:** Before optimizing, when adding or changing a benchmark, when a change touches a hot path, or when profiling.

Methodology:
[`docs/research/40-implementation/03-benchmarking-plan.md`](../../research/40-implementation/03-benchmarking-plan.md).

1. **Profile before optimising.** Build the macro harness, find the top three hot
   spots, then write micro-benchmarks for those.
2. **JMH: `@Fork(≥2)`, ≥5 warmup iterations, `Blackhole` on every result.**
3. **`-prof gc` always.** Allocation rate is a first-class result: a change 5%
   faster that allocates 2× more is a regression.
4. **Realistic inputs**, including the size long-tail.
5. **Report distributions, not means.** Object-store latency is fat-tailed.
6. **Record the environment** with the result.
7. **Never state a measured number you did not measure**, and never present a
   modelled number as measured. Label models and say what would falsify them.
8. **A hot path has a benchmark**, or an explicit note saying why not. Every hot
   path is profilable: JMH for the micro case, async-profiler/JFR wired into the
   e2e task for the macro case (L5).

## What may be a gate, and what may only be a trend

⚠️ **The distinction is determinism, not importance.**

> **An assertion has a bound and fails reproducibly — it may gate.
> A benchmark has a number and is noisy on a shared runner — it may only trend.**

**Gates** (deterministic, cheap, catastrophic if they regress):

| | |
|---|---|
| idle request count `== 0` | R3 — invisible to every functional test |
| requests per MiB `< 0.30` | NFR-1 |
| read requests scale with nodes, not shards | R5 |
| **allocation per record (`gc.alloc.rate.norm`, B/op)** | the leading indicator of GC-driven collapse |
| memory flat under 10× body size | NFR-6 |

**Trends** (report, chart, review — never fail a build): throughput MiB/s, p50/p99
latency, JMH ns/op.

9. ⚠️ **Gate on allocation, trend on time.** Wall-clock on a shared CI runner has
   double-digit variance and a threshold on it becomes noise that people learn to
   re-run until green — which is worse than no gate, because it teaches the team
   that a red build means nothing. **Allocation is stable across machines**, and a
   change that 2× allocation will 2× it everywhere.
10. **Gate benchmarks are a small named set** run with 1 fork and few iterations
    inside L1's 5-minute budget. The full suite, with proper fork counts, is L4 and
    manual.
11. **Optimisation claims come with before/after numbers in the commit body.**
