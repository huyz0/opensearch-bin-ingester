# The producer contract

**Status:** proposal · **Confidence:** high (follows from ADRs 0006, 0013–0016) ·
**Last updated:** 2026-08-30

**Read this if:** you are writing a producer, a consumer library, or the ingest
handler.
**One-line takeaway:** a producer sends **OpenSearch `_bulk` NDJSON, unchanged**,
to a different URL. No shard count, no hashing, no partition arithmetic, no
OpenSearch credentials. The `routing` field it already puts in the action line is
the only thing we need.

---

## 1. What the producer does

```http
POST /v1/bulk?lane=0                      HTTP/2
Content-Type: application/x-ndjson

{"index":{"_index":"logs","_id":"a1","routing":"t_4821993_2","version":7,"version_type":"external"}}
{"tenant":"4821993","msg":"…"}
{"delete":{"_index":"logs","_id":"a2","routing":"t_4821993_2"}}
```

→ `202 Accepted`

That is the whole contract. **This is byte-for-byte an OpenSearch bulk request**,
so Logstash, Fluent Bit, Data Prepper, the OpenSearch clients and OTel exporters
work by changing a URL.

### What the producer does **not** do

| | Why not |
|---|---|
| Look up shard counts | the ingester knows them (the plugin registers them, [ADR-0015](../../internal/product/decisions/0015-routing-registration-and-aliases.md)) |
| Compute `murmur3` or a partition | the ingester does it from the `routing` value |
| Talk to OpenSearch | ever, for any reason |
| Hold OpenSearch credentials or rotate certs | there are none to hold |
| Choose fast mode | it is an **index** setting, not a request field ([ADR-0013](../../internal/product/decisions/0013-fast-mode-wal-and-quorum.md)) |
| Resolve aliases | the ingester resolves to the current concrete index |
| Know about segments, offsets or partitions | none of it is in the contract |

⚠️ **The producer is first-party** (decided 2026-08-30) — a custom application,
not an off-the-shelf agent. That changes the emphasis, not the contract:

- `_bulk` stays the primary surface. It costs almost nothing to accept (§2), it is
  a contract nobody has to learn, and it keeps the door open for Logstash or an
  OTel exporter later.
- **The binary framing is a realistic near-term option, not a distant
  optimisation**, since the producer is ours to change. It skips the newline scan
  and the action-line parse, and it is the only way to set a *per-record* lane.
  Adopt it when a benchmark says the 5%-of-a-core matters — not before.
- The "existing agents work by changing a URL" claim is therefore **untested** and
  should be labelled as such until an agent is actually pointed at it.

⚠️ **The `routing` field is already there.** Anyone doing tenant routing on
OpenSearch already writes `"routing":"tenantId_k"` in the bulk action line —
that is how custom routing works. **The routing scheme is the producer's existing
application logic, not something this design imposes.**

## 2. Accepting `_bulk` costs almost nothing

Action lines are parsed; **document bodies never are** — the body is passed
through by length after a newline scan.

| Load | Newline scan | Action-line parse |
|---|---|---|
| 1,000 rec/s (1 MiB/s) | 0.01% of a core | 0.05% of a core |
| 50,000 rec/s (backfill) | 0.49% | 2.50% |
| 100,000 rec/s (100 MiB/s) | 0.98% | **5.00%** |

5% of one core at 100 MiB/s buys compatibility with every existing OpenSearch
client. Worth it.

A **length-prefixed binary framing** remains available for producers that care
([streaming-io §2](../40-implementation/02-streaming-io-and-memory.md)) — it skips
the scan and the parse — but it is an optimisation, not the contract.

### ⚠️ Backfill carries the source's version, never a replay-time one

A re-bootstrap replays each record with **the version it holds in the system of
record**, in the same version space as live traffic. That is what makes backfill
(lane `-1`) safe to run **concurrently with live writes for the same tenant**, with
no coordination: OpenSearch rejects the older value.

**Stamping a fresh version at replay time silently corrupts data** — the replay
would carry higher versions than live traffic, overwrite newer records, and be
accepted as correct. Nothing downstream can detect it. This is a producer-side
invariant and the most dangerous single line in this document.

### Supported bulk fields
`_index` · `_id` · `routing` · `version` + `version_type: external` ·
action `index` / `create` / `delete`.

**Rejected, loudly:** `pipeline`, `_source` manipulation, `if_seq_no` /
`if_primary_term`, and any action naming an unregistered index. Reject, never
ignore — a silently dropped field is a silently wrong index.

⚠️ **`version` + `version_type: external` is REQUIRED, not recommended**
([ADR-0020](../../internal/product/decisions/0020-record-envelope-and-mapper.md)).
It is what makes at-least-once replay safe on mutations — a redelivered stale
delete cannot resurrect a document — and what makes cross-lane mutations safe
([ADR-0014](../../internal/product/decisions/0014-priority-lanes.md)). A record
without one downgrades **both** guarantees, and the ingester should reject rather
than silently accept it on an index configured for mutations.

## 3. What 202 means — pick deliberately

| `ack` | Meaning | Latency | RPO |
|---|---|---|---|
| **`durable`** (default) | segment PUT **and** commit delta are durable | 42–708 ms | **0** |
| index in fast mode | WAL replicated to `quorum` AZs | **1.5–6 ms** | 0 while a quorum member survives |
| `ack=none` (opt-in) | accepted into the buffer | ~0 ms | **the producer must be able to replay** |

⚠️ **Do not reach for `ack=none` to get speed.** Fast mode gives 1.5–6 ms *with*
a durability guarantee; `ack=none` gives ~0 ms with none. `ack=none` is for
producers that already have a replayable source — a re-bootstrap job reading from
a system of record — and it should be an explicit, per-request choice.

**Backpressure is a `429` with `Retry-After`**, not a silent drop or an unbounded
queue. A producer must honour it; a consumer library must implement jittered retry.
At-least-once means a retry may duplicate — deduplicated by `_id` and external
version.

## 4. Lane is per request, not per record

```
POST /v1/bulk?lane=-1        # a re-bootstrap job marks everything it sends
```

Lane is a signed bucket (`-1` background, `0` standard, `+1` elevated). It is
per-**request** in the bulk API because that matches how it is used: a backfill
job sets it once for its whole workload. The binary framing allows per-record
lanes for producers that genuinely mix.

## 5. AZ affinity is a deployment concern, not a producer one

The producer uses one URL. Affinity comes from the platform:

- **In-cluster producers:** a per-zone `Service`, or `trafficDistribution: PreferClose`.
- **External producers:** ⚠️ **disable cross-zone load balancing.** The entry AZ
  then becomes the writing AZ and nothing crosses a boundary. Leaving it on sends
  ⅔ of traffic across AZs:

| Ingest | Wasted with cross-zone LB on |
|---|---|
| 1 MiB/s | $36/month |
| 50 MiB/s | $1,812/month |
| 100 MiB/s | **$3,624/month** |

This is the single most expensive misconfiguration available in the whole design,
it is one checkbox, and nothing in the data path will reveal it. Put it in the
deployment checklist and alert on cross-AZ bytes.

## 6. Errors a producer must handle

| Status | Meaning | Producer action |
|---|---|---|
| `202` | accepted per §3 | done |
| `429` + `Retry-After` | rate limit or buffer pressure | back off, retry |
| `503` | no writer available (lease failover) | retry, jittered |
| `400` | malformed frame, unsupported field, unknown index/alias | **fix; do not retry** |
| `413` | batch too large | split |

⚠️ `400` on an unknown index is deliberate ([ADR-0006](../../internal/product/decisions/0006-partition-assignment-and-the-routing-invariant.md)):
an unregistered index is buffered briefly in the pending pool and only then
rejected, so a `400` means it really is not coming.

## 7. Open questions

- Should the response body mirror OpenSearch's per-item bulk response? It would
  improve drop-in compatibility, and it is cheap in `durable` mode where per-item
  outcomes are known. Under `ack=none` there is nothing truthful to put in it.
- Is a `?refresh=wait_for` equivalent worth offering — hold the response until the
  record is *searchable*, not merely committed? It would need the plugin to report
  back, and it is the semantic some clients actually want.
