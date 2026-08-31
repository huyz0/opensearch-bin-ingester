# Fast mode: a quorum-replicated WAL for low-latency visibility

**Status:** proposal — see [ADR-0013](../../internal/product/decisions/0013-fast-mode-wal-and-quorum.md) ·
**Confidence:** medium (latency modelled; the cost figure is arithmetic and is the
load-bearing number) · **Last updated:** 2026-08-30

**Read this if:** you are implementing the fast tier, or deciding whether a
workload should use it.
**One-line takeaway:** fast mode reaches **~1.5–6 ms** ack-and-visible instead of
42–708 ms, and costs **$5,436/month at 100 MiB/s** in cross-AZ replication against
~$400/month for the entire normal-mode system. **It must be selective, and its
durability guarantee is weaker.**

---

## 1. The cost that shapes the design

Fast mode replicates every record across AZ boundaries. At $0.02/GB round trip:

| Traffic in fast mode (100 MiB/s total) | quorum 2/3 | quorum 3/3 |
|---|---|---|
| 100% | **$5,436/mo** | $10,872/mo |
| 10% | $544/mo | $1,087/mo |
| **1%** | **$54/mo** | $109/mo |

⚠️ **Fast mode on all traffic costs 13× the entire normal-mode design.** This is
not a reason to reject it — the brief accepts the trade — but it is the reason the
selector must be **per record**, not per deployment. A workload that puts
everything in the fast lane has not chosen a tier; it has chosen a different,
far more expensive product.

The corollary: **the ordinary path must stay the default**, and fast mode must be
something a producer opts into per message, so the bill is proportional to the
records that actually needed it.

## 2. What it changes

```
DURABLE (default)   buffer -> flush -> segment PUT -> commit delta PUT -> ack + visible
                    ack and visibility at 42-708 ms

FAST                buffer -> WAL append + fsync
                          -> sequencer assigns the offset
                          -> replicate (record + offset) to a quorum of AZs
                          -> ACK + VISIBLE                      ~1.5-6 ms
                          -> ...later: segment PUT + commit delta PUT (S3 catches up)
```

Two things move earlier: the **ack**, and the **offset assignment**. Both are
necessary — visibility requires an offset, and an offset requires the sequencer
([ADR-0001](../../internal/product/decisions/0001-segments-carry-no-absolute-offsets.md)).
Pre-allocating offsets before the write was rejected there and stays rejected: it
reintroduces holes.

| Stage | ms |
|---|---|
| producer → pod (same AZ) | 0.3 – 1.0 |
| WAL append + fsync (instance store) | 0.05 – 1.0 |
| pod → sequencer, offset assigned | 0.5 – 2.0 |
| replicate to quorum (1 remote AZ) | 0.5 – 2.0 |
| ack | 0.1 – 0.3 |
| **Total, acked and visible** | **~1.5 – 6.3 ms** |

## 3. The three index-level tuning settings

```
index.ingestion_source.param.flush_timer   duration   (default: 5s)   -- latency deadline
index.ingestion_source.param.wal           bool       (default: false)
index.ingestion_source.param.wal_quorum    1 | 2 | 3  (default: 2, only when wal=true)
```

Set in OpenSearch and pushed to the ingester by the plugin
([ADR-0015](../../internal/product/decisions/0015-routing-registration-and-aliases.md)).
`wal` replaces the earlier `ack_mode: durable|wal` — same meaning, plainer name.

These are the **tuning** dials. Three more index settings are *structural* rather
than tuning and are decided at creation: partitioning mode
(`explicit | routing_key | os_routing`, ADR-0006/0015), the active lane set
(ADR-0014), and OpenSearch's own `replication_type` + `all_active` (ADR-0009).

### The matrix, at 1 MiB/s

| Setting | Ack point | Latency | RPO | Cost/month |
|---|---|---|---|---|
| **`wal=false`** (default), timer 5 s | after segment PUT | 42–708 ms | **0** | **$15.55** (all PUT) |
| `wal=true, quorum=1`, timer 60 s | after local fsync | ~1–2 ms | ⚠️ **lost if that pod's disk goes** | **$1.62** |
| `wal=true, quorum=2`, timer 60 s | after 2 AZs | ~1.5–6 ms | 0 while a quorum member lives | $1.62 PUT + **$54.36 cross-AZ** = $55.98 |
| `wal=true, quorum=3`, timer 60 s | after 3 AZs | ~2–8 ms | survives 2 AZ losses | $1.62 + $108.72 = $110.34 |

⚠️ **`quorum=1` is AutoMQ's economics exactly** — a local-disk WAL, cheapest of all
four rows, and their single-AZ durability posture with it
([automq §10.2](../10-prior-art/02-automq.md)). It is only honest on a
PersistentVolume; on `emptyDir` a reschedule loses acked data.

### Why `wal=true` lets the timer relax — and when it does not

| Timer | Objects/s | PUT cost |
|---|---|---|
| 5 s | 1.20 | $15.55/mo |
| 30 s | 0.20 | $2.59/mo |
| 60 s | 0.12 | $1.62/mo |

- **`wal=false`:** the timer bounds **ack latency**, because the PUT *is* the ack.
  It cannot be long.
- **`wal=true`:** the ack comes from the quorum, so the timer only delays arrival
  in S3. **It can be long, and should be** — that is the AutoMQ insight
  ([§10.1](../10-prior-art/02-automq.md)), and it recovers ~26% of the quorum-2
  cross-AZ bill for free.

⚠️ **But the segment is shared, so the tightest timer in a pod wins.** An index
with `wal=true, timer=60s` sharing a pod with one at `wal=false, timer=5s` gets a
5 s cadence and none of its bundling benefit. **The timer is a per-index deadline,
not a per-index bundling guarantee**, and it is a cost externality: one tight index
sets the PUT rate for every index on that pod — the same shape as the lane-deadline
rule ([cost-model R16](../00-problem/02-cost-model.md)).

Do **not** fix this by giving each timer class its own segment stream: a second
stream adds a whole `pods ÷ interval` baseline and costs more than it saves. Make
timers uniform where the bundling benefit matters, cap how tight a timer may be,
and show operators the effective cadence rather than the requested one.

Set as OpenSearch index settings and pushed to the ingester by the plugin
([ADR-0015](../../internal/product/decisions/0015-routing-registration-and-aliases.md)).

⚠️ **A per-record selector does not work, and the reason is structural.** A
partition is one totally ordered log: a consumer must process offset *N* before
*N+1*. If R1 (durable) is 100 and R2 (fast) is 101, R2 is WAL-committed and
*would* be visible in 5 ms — but the consumer cannot reach it until R1 becomes
visible at ~300 ms. **A fast record behind a durable record is only as fast as the
record ahead of it.** Mixing modes in one partition yields head-of-line blocking,
or degenerates into "everything after this is fast".

> **The general rule** — it also explains why *lanes* work per record
> ([ADR-0014](../../internal/product/decisions/0014-priority-lanes.md)):
> **priority may be applied before the offset is assigned; anything applied after
> it is subject to the log's total order.** A lane reorders in the buffer, where
> reordering is free. An ack mode changes when an already-assigned offset becomes
> visible, which in-order consumption then defeats.

Three operational consequences follow, all pointing the same way: the **index
owner** owns the durability guarantee fast mode weakens; an operator can *see*
which indices are downgraded by reading index settings rather than sampling
traffic; and no producer can accidentally commit the cluster to $5,436/month.

| Setting | Values | Meaning |
|---|---|---|
| `ack_mode` | `durable` (default) · `wal` | when the producer is acknowledged |
| `quorum` | `1` · **`2`** · `3` | how many AZs must hold the WAL record before ack |

- `durable` — current behaviour. Ack after the S3 commit. RPO 0, unconditionally.
- `wal` + `quorum=1` — ack after local fsync. ~1 ms. **A single pod loss loses
  acked data.** Offered, but it is the only combination that can lose a record to
  an ordinary pod restart; treat it as a deliberate, documented choice.
- `wal` + `quorum=2` (default for fast mode) — survives the loss of one AZ.
- `wal` + `quorum=3` — survives two, at double the cross-AZ bill.

Keeping them separate matters: "write a WAL" and "how many copies before ack" are
independent questions, and collapsing them into one `fast: true` flag hides the
durability choice inside a latency choice.

## 4. The durability trade, stated plainly

⚠️ **Fast mode changes NFR-8.** In `durable` mode an acked record is in a
regional, multi-AZ bucket before the ack. In `wal` mode it is on the disks of
`quorum` pods until the segment upload completes.

Lose the quorum inside that window and an **acked record is gone** — and worse,
if a consumer already indexed it, **the OpenSearch index contains a record the log
can no longer reproduce.** Replay after a disaster would not rebuild that index
byte-for-byte.

The window is one upload interval (~250 ms – 1 s), so the exposure is "lose 2 of
3 AZs within a second of a write". Narrow, but real, and it is exactly the
exposure ADR-0011 declined to take for the *default* path.

**Requirement:** NFR-8 becomes conditional, and the index registration must record
which mode it uses so an operator can see it. A cluster that quietly turned on
fast mode everywhere has silently downgraded its durability.

## 5. S3 must be a superset of the WAL

> *"the committed data in S3 always have to be fully included the committed WAL"*

The invariant: **every WAL-committed record appears in a committed segment, at the
same offset.** Consequences:

- The WAL **is** the buffer. A flush uploads WAL contents as a segment; it is not
  a second copy written on a separate path. This keeps the two from diverging by
  construction rather than by reconciliation.
- WAL entries are released only after the segment is durable *and* its commit
  delta has landed.
- Recovery: if the writing pod dies, **any quorum member can upload the WAL** —
  the WAL entry carries the segment-assignment metadata needed to do so. That is
  what makes acking on quorum honest.
- **I6 (new invariant):** no offset is WAL-committed and then absent from a
  committed segment. Test it by killing the writer after ack and before flush, and
  asserting the record is still readable afterwards.

## 6. Catch-up for an AZ that missed the quorum

A record acked at quorum 2 has not reached the third AZ. That AZ's pods obtain it
by, in order:

1. **The segment from S3**, once uploaded — free in-region, and the common case
   because the upload lands within a second.
2. **A peer in another AZ**, only if the segment is not yet uploaded and a
   subscriber needs the bytes now. ⚠️ Subject to the **~19.5 KiB crossover**
   ([cost-model R12](../00-problem/02-cost-model.md)): a small WAL batch is
   cheaper to ship than to wait for; a large one is not.

There is no third path, and no repair protocol: the WAL is transient and S3 is the
convergence point.

## 7. Pods stay reschedulable

The WAL lives on **`emptyDir`** (instance store where available), **not** a
PersistentVolume. A pod that restarts with an empty WAL recovers from its quorum
peers. This preserves the property ADR-0011 protects — pods are cattle — while
accepting the disk the brief asks for.

⚠️ Fast mode still makes pods **stateful in effect** for the duration of the
upload window: a pod holding un-uploaded WAL entries cannot be terminated without
first flushing or handing off. `terminationGracePeriodSeconds` and the shutdown
ordering in [failure-domains §7](08-failure-domains-and-resilience.md) must cover
it. This is the real complexity cost of fast mode, more than the disk.

Footprint: 60 s of buffered 100 MiB/s traffic is **5.9 GiB per pod**; 300 s is
29 GiB. Size it from the upload interval, not from retention.

## 8. Relationship to AutoMQ, and to our own earlier decision

This is deliberately AutoMQ's model ([automq §2](../10-prior-art/02-automq.md)),
which we previously and correctly declined for the default path: they need a WAL
because they must ack a Kafka producer with Kafka's durability semantics, and we
did not.

⚠️ **This is a scoped reversal of [ADR-0011](../../internal/product/decisions/0011-no-consensus-cluster.md)'s
conclusion**, taken on exactly the trigger that ADR recorded: *"if a requirement
for sub-50 ms visibility ever appears, the answer is a WAL or an embedded Raft."*
It arrived. The reversal is scoped to an opt-in tier and does **not** move the
default, which remains object-store CAS with no WAL.

The difference from AutoMQ that survives: their metadata quorum is KRaft, a
consensus group. Ours is still a **single sequencer whose assignments are
replicated to a quorum of AZs** — replication for durability, not election. Leader
election remains the CAS lease.

## 9. Open questions

- Does the sequencer become a throughput bottleneck when every fast record passes
  through it synchronously? 60,000 assignments/s is a map lookup; the *replication
  fan-out* is the new load and is unmeasured.
- ✅ **Answered: per index only.** A per-record selector is unsound — see §3. It
  also means the cost is controlled by whoever owns the index, which is the right
  place for a $5,436/month dial.
- Is `quorum=1` worth offering at all, given it is the only setting that loses
  acked data to a routine pod restart?
- ✅ **Answered: two controls at different granularities.** Ack mode is per index
  because it cannot be per record; lane is per record because it must be. Do not
  merge them.
- ⚠️ **Open:** if OpenSearch ships `MODULO` plus a composite pointer, each lane
  could have its own offset space — which would make a **per-lane ack mode**
  sound, since today's head-of-line blocking is an artefact of sharing one ordered
  log. Revisit ADR-0013 and ADR-0014 together if that lands.
