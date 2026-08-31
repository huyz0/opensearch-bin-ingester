# The cost governor: measuring, attributing and *limiting* store requests

**Status:** proposal · **Confidence:** high (the arithmetic; the policy is a
judgement call) · **Last updated:** 2026-08-30

**Read this if:** you are implementing the store decorators, metrics, or anything
that could loop over the store.
**One-line takeaway:** counting is not controlling. A regression that reinstates
per-shard polling costs **$21,600/hour as LIST**, so the governor must **refuse**
requests, not merely record them — and the ceiling should be a **ratio to expected**,
not a fixed number, because expected moves with load.

---

## 1. What a runaway actually costs

| req/s | as GET | as PUT/LIST |
|---|---|---|
| 1,000 | $1/hr | $18/hr |
| 10,000 | $14/hr | $180/hr |
| 100,000 | $144/hr | $1,800/hr |
| **1,200,000** | **$1,728/hr** | **$21,600/hr** |

That last row is not hypothetical: it is 120,000 shards polling at 10 Hz — the
exact regression that removing the blocking `readNext` would cause
([poller-semantics](../20-opensearch/02-poller-semantics-and-cost.md)).

| Time to notice | Cost of the LIST version |
|---|---|
| 1 hour | $21,600 |
| overnight | $172,800 |
| **one day** | **$518,400** |

⚠️ **A PUT and a LIST cost the same, and 12.5× a GET.** So the enforcement must be
hardest on LIST, which is also the operation with the fewest legitimate uses.

## 2. Ratio to expected, not a fixed ceiling

A fixed requests/s ceiling is wrong at one end of the load curve or the other. The
design already maintains an invariant that *is* stable — **requests per MiB** — so
govern on that.

```
expectedPuts/s = max( bytes/s ÷ segmentSize , writers ÷ flushInterval )
                 + 1 ÷ commitInterval
```

| Load | Expected PUT/s | Alarm at 3× | Hard-stop at 10× |
|---|---|---|---|
| 1 MiB/s | 1.20 | 3.6 | 12 |
| 50 MiB/s (backfill) | 12.0 | 36 | 120 |
| 100 MiB/s | 24.0 | 72 | 240 |

The ratio self-adjusts through a backfill, where a fixed ceiling would either
throttle legitimate work or be useless at low load.

## 3. Not all requests may be refused

⚠️ **Refusing a segment PUT for already-acked data is data loss.** The governor is
priority-aware, and each class degrades differently:

| Class | On breach | Why |
|---|---|---|
| **data write** (segment) | **never dropped** — apply backpressure upstream: `429` to producers, block the in-process caller | The alternative is losing acked records |
| **commit** | never dropped; same backpressure | An uncommitted segment is invisible |
| **read** | queue, then degrade — serve from cache, or hand the client coordinates and let it fetch | Latency is recoverable; money is not |
| **GC, compaction, recovery sweep** | **defer immediately** and alarm | Discretionary by definition |
| **LIST** | **hard ceiling, ~1/s sustained**, regardless of ratio | Recovery and GC only (R2). More than a trickle means something is badly wrong |
| DELETE | unlimited | free |

## 4. Attribution — the alarm must name the culprit

With 10,000 indices and two trust domains, "requests are high" is not actionable.
Counters are kept **in memory** per `(op, purpose, trustDomain, index)` — a
`LongAdder` per index is ~10,000 objects and costs nothing.

⚠️ **They are not exported per index.** A `index` metric label costs **200,000
series**, and per-index telemetry at this scale emits 13× more datapoints than the
workload it observes ([observability.md](../../internal/standards/observability.md)).
Attribution reaches an operator by **top-K in a periodic log event** and by
**`GET /admin/cost?by=index&top=50`**, both of which have zero metric cardinality.
**Count everywhere, export almost nothing.**

Emit as metrics:

- `binstore_requests_total{op,purpose,domain}` — the raw counter. ⚠️ Note the
  absent `index`: labels are restricted to the allow-list in
  [observability.md](../../internal/standards/observability.md) rule 1
- `binstore_requests_per_mib{purpose}` — **the invariant**, and the one to chart
- `binstore_requests_ratio_to_expected{purpose}` — what the governor acts on
- `binstore_estimated_usd_per_hour{domain}` — the number an operator watches
- `binstore_governor_refusals_total{class}` — **must be zero in steady state**

⚠️ Prices live in a per-provider `CostTable`, not in code. The dollar metric is an
*estimate* and must be labelled so — request counts are exact, prices are a
lookup that goes stale.

## 5. The kill switch

Beyond the hard-stop ratio there is a **catastrophic** threshold (default 100×
expected, or any sustained LIST above the ceiling) which:

1. Halts **all** discretionary work — GC, compaction, recovery sweeps, prefetch.
2. Keeps the write path alive under backpressure, because stopping it loses data.
3. Pages, with the top three `(purpose, index)` pairs by request count.

⚠️ **It does not stop writes, and it does not silently continue.** Both of those
are worse than the bill.

## 6. Testing it

- A fault-injecting store that reports a fabricated rate, asserting each class
  degrades as §3 says — **especially that data writes are never dropped**.
- A test that removes the blocking `readNext` and asserts the governor refuses
  within seconds. **The regression this exists to catch should be in the suite.**
- Assert `binstore_governor_refusals_total == 0` across the normal-load run: a
  governor that fires in steady state is mis-tuned, and one that fires often will
  be turned off.
