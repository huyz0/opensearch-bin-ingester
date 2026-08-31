# Cost

**Family:** Quality
**Read when:** A change touches how objects are written, read, listed or discovered; before claiming a change is cost-neutral; or when a request rate appears in a design.

The product converts a network bill into an API bill. These rules are the
product. Full derivation:
[`docs/research/00-problem/02-cost-model.md`](../../research/00-problem/02-cost-model.md).

⚠️ **The numbered rules below are the `R<n>` namespace**: rule 1 is R1, rule 18
is R18, and `R1b` is the adaptive-interval rule. Specs, ADRs and
[`cost-budget`](../../../.agents/skills/cost-budget/SKILL.md) cite those numbers,
so renumbering one silently rewrites every citation.

## The invariant

```
request rate may scale with:       segments, AZs, nodes
request rate may NEVER scale with: records, shards, partitions, indices, documents
```

1. **Bundle** all indices and partitions of a pod into one object per flush.
   Per-partition objects cost ~$52/partition/month at 250 ms rotation.
1b. **The flush interval is adaptive and is the primary dial**
   ([ADR-0017](../product/decisions/0017-every-pod-writes.md)). A flat 250 ms
   costs $311/mo against $15.55/mo at a 5 s ceiling — the interval is worth more
   than the writer count at every setting. ⚠️ Quoting a fixed 250 ms in a design
   is quoting the rejected operating point.
2. **Never `LIST` on a hot path.** A LIST costs the same as a PUT — 12.5 GETs.
   Recovery and GC only.
3. **Idle consumers issue zero requests.** At 120,000 shards a naive poll costs
   $1.2M/month as GETs, $15.5M as LISTs, doing nothing.
4. **Always coalesce adjacent reads.** Same-region bytes are free; requests are not.
5. **One fetch per object per node**, shared by every shard on it.
6. **Commit/metadata write rate is independent of index count.**
7. **One speculative range read, not footer-then-index.** The header length is in
   the object key precisely so the reader never guesses.
8. **Retention is a cost dial.** Default hours, not days. Past ~3 h, storage
   exceeds API cost.
9. **Every store operation is counted and attributable.** → `CountingBinStore`
10. **Read cost scales with node count** — above ~10 data nodes, serve reads from
    the ingester nodes rather than letting the plugin read the object store.
11. **Cache where fan-in is high** (an ingester node shared by ~100 nodes), not
    where it is low (a data node that will never re-read the segment).

## Enforcement

⚠️ **No script enforces these yet** — the counting harness lands with the store
SPI. Until it does, a cost claim is a claim, and the honest response is to say
the budget was not measured. The three assertions to write first:

```java
assertThat(stats.requestsPerMiBWritten()).isLessThan(0.30);
assertThat(stats.listRequests()).isZero();
assertThat(runIdle(minutes(5), /*shards=*/1600).totalRequests()).isZero();
```

The third is the most valuable test in the project: it is the one failure
invisible to every functional test and catastrophic in production.

## Runtime control, not only measurement

12. **The store is fronted by a cost governor** that *refuses*, not merely counts
    ([15-cost-governor.md](../../research/30-design-space/15-cost-governor.md)).
    Counting catches a regression in review; only refusing catches one in
    production, and the per-shard-polling regression costs **$21,600/hour as LIST**.
13. **Govern on ratio-to-expected, not a fixed rate.** `expected` is derived from
    throughput, segment size, writer count and interval, so it self-adjusts through
    a backfill. Alarm at 3×, hard-stop discretionary work at 10×.
14. ⚠️ **Never refuse a data write or a commit.** Apply backpressure upstream — a
    `429`, or a blocking in-process call. Refusing an acked segment PUT is data
    loss, and the bill is the lesser problem.
15. **LIST has its own hard ceiling (~1/s sustained)**, independent of the ratio.
    It costs the same as a PUT, has the fewest legitimate uses, and is the runaway
    with the worst blast radius.
16. **Count per index in memory; never export it as a metric label.** Attribution
    reaches an operator through top-K log events and `/admin/cost`, not through
    200,000 time series ([observability.md](observability.md) rules 1–2).
17. **`binstore_governor_refusals_total` must be zero in steady state.** A governor
    that fires routinely is mis-tuned, and one that fires routinely gets disabled.
18. **Never relax a budget to make a check pass.** Moving a budget is an ADR with
    the number, the rejected alternative, and what the new budget buys.
