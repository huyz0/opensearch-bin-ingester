# Observability

**Family:** Quality
**Read when:** Adding a metric, a log field, a span attribute, or a dashboard; or when you want per-index detail and are about to reach for a label.

## The cardinality budget

⚠️ **Labels are the only thing that makes observability expensive.** What a label
costs here:

| What you put in a label | Series (×20 metrics) | TSDB memory |
|---|---|---|
| **allow-list only** | ~1,000 | negligible |
| + `index` (10,000) | 200,000 | ~0.5 GiB |
| + `stream` (120,000) | 2,400,000 | ~5.7 GiB |
| + `tenant` (5,000,000) | 100,000,000 | ~238 GiB |

⚠️ **We are an ingestion system, so our telemetry is ingestion traffic.**
Per-index metrics at 10,000 indices emit **1.15 billion datapoints/day** — for a
workload of ~86 million records/day. **Our own observability would be 13× the
thing it observes.** That is the argument; the rest of this file follows from it.

## Rule 1 — the label allow-list is closed

| Allowed | Values |
|---|---|
| `op` | get, range_get, put, put_if_absent, put_if_match, list, delete, stat |
| `purpose` | data, commit, lease, registry, gc, recovery, prefetch |
| `domain` | trust domain (2) |
| `lane` | the active bucket set (≤8) |
| `result` | ok, retried, refused, error |
| `tier` | T0–T4, for test metrics only |

Worst-case product ≈ **1,080 combinations**. That is the budget.

⚠️ **Never a label:** `index` · `partition` · `stream` · `tenant` · routing value ·
object key · offset · `_id` · segment id. → `scripts/check-metric-cardinality.sh`

Adding one is not a judgement call; it is a standards violation with a number
attached.

## Rule 2 — attribution goes to events and endpoints, not to labels

You still need to know *which index* is burning requests. Three mechanisms, none
of which touch metric cardinality:

| Need | Mechanism | Cost |
|---|---|---|
| Continuous trend | **metrics**, allow-list labels only | ~1,000 series |
| "Who is doing this?" | **periodic structured log event** carrying top-K by request count, bytes and lag | one log line/minute |
| Deep dive, right now | **`GET /admin/cost?by=index&top=50`** — exact, from in-memory counters | zero stored |
| One slow request | **traces**, sampled, plus tail-sampling on error and on slow | bounded by sample rate |

⚠️ **In-memory per-index counters are fine.** A `LongAdder` per index is ~10,000
objects; a *time series* per index is 200,000 series forever. **Count everywhere,
export almost nothing.**

## Rule 3 — what to measure, on the three axes

**Performance** — latency histograms per stage (accumulate, segment PUT, commit,
push, fetch), bounded bucket counts; allocation rate `B/op` on the record path;
GC pause distribution; virtual-thread pinning events.

**Scalability** — the invariants, which are the leading indicators:
`requests_per_mib{purpose}` · `segment_fill_ratio` · `runs_per_segment` ·
`streams_active` (a gauge, not a label) · `writers_active` · queue depths ·
subscriber count · `consumer_lag_seconds` quantiles.

**Cost** — `binstore_requests_total{op,purpose,domain}` ·
`binstore_requests_ratio_to_expected{purpose}` ·
`binstore_cross_az_bytes_total{direction}` ·
`binstore_estimated_usd_per_hour{domain}` ·
`binstore_governor_refusals_total{class}` (**must be zero in steady state**).

⚠️ The dollar metric is an **estimate** from a per-provider `CostTable` and must be
labelled so. Request counts are exact; prices are a lookup that goes stale.

## Rule 4 — the four numbers on the front page

A dashboard nobody reads is not observability. Four tiles, in this order:

1. **`binstore_requests_per_mib`** — the invariant the whole design maintains.
2. **`consumer_lag_seconds` p99** — is the data arriving.
3. **`binstore_estimated_usd_per_hour`** — is it costing what we said.
4. **`binstore_governor_refusals_total`** — is anything being refused.

Everything else is for after one of those moves.

## Rule 5 — never log a payload

⚠️ No document body, credential, signed URL, routing value or `_id` in a log,
span attribute, metric label or error message ([security.md](security.md)). An
index *name* in a log line is acceptable; a tenant identifier is not.
