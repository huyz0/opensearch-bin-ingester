# Failure domains and resilience

**Status:** proposal · **Confidence:** medium-high (topology and mechanisms are conventional; the
split-brain argument rests on [metadata-and-cas §7](03-metadata-and-cas.md), which is itself
unproven) · **Last updated:** 2026-08-30

**Read this if:** you are sizing the deployment, writing K8s manifests, implementing shutdown/failover,
or designing the chaos test suite.
**One-line takeaway:** "stateless" is necessary but not sufficient. The ingester has **three
singleton-ish roles** hiding inside a stateless process, and resilience means making each one
survivable. Safety never depends on failure *detection* — it rests on epoch fencing plus write-once
sequence claims, so an undetected zombie is safe, merely wasteful.

---

## 1. "Stateless" ≠ "no roles"

No pod holds durable state, but three things are not freely interchangeable:

| Role | What is at risk | Blast radius | Recovery |
|---|---|---|---|
| **Ingest buffer** | records accepted but not yet flushed (RAM only) | that pod's in-flight requests | producer retry — **we ack only after PUT + commit**, so nothing acked is ever lost |
| **Sequencer lease** (per slot) | offset assignment for its slots | visibility stall for those partitions | lease expiry or early challenge → new epoch → seal → continue |
| **Segment cache ownership** (per AZ) | which pod holds a segment in RAM | a few extra GETs | ring rebalance; pure optimisation, never correctness |

Only the second is a genuine singleton. The first is recoverable by retry; the third is a cache.

## 2. Minimum topology

| Component | Minimum | Why |
|---|---|---|
| AZs | 3 | 2 gives no quorum intuition and halves capacity on failure; 3 loses ⅓ |
| **Ingester nodes per AZ** | **≥ 2** | With 1/AZ, one pod failure is an AZ-wide serving outage. **This is the most common way to get this wrong.** |
| Total ingester nodes | 6 | HA-driven, not capacity-driven — see §3 |
| Object store | **regional / multi-AZ bucket** | The resilience anchor. **S3 Express One Zone is single-AZ and is disqualified for the primary path** — previously flagged on cost grounds ([cost-model §1](../00-problem/02-cost-model.md)), now a hard availability rule |
| OpenSearch | allocation awareness on `zone`, replicas across AZs | standard practice; interacts with §5 |

```yaml
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: topology.kubernetes.io/zone
    whenUnsatisfiable: DoNotSchedule
    labelSelector: { matchLabels: { app: bin-ingester } }
---
apiVersion: policy/v1
kind: PodDisruptionBudget
spec:
  maxUnavailable: 1                      # never drain two pods at once
  selector: { matchLabels: { app: bin-ingester } }
```

Also required: `cluster.routing.allocation.awareness.attributes: zone` (and *forced* awareness) on
the OpenSearch side, so replicas never co-locate with their primary in one AZ.

## 3. The cost of HA, and the adaptive flush interval

More pods means more time-triggered flushes:

```
PUT/s  =  pods x min( 1/flushInterval , perPodThroughput / segmentSize )
```

At 100 MiB/s: **3 pods** ⇒ 33 MiB/s each ⇒ size-triggered ⇒ ~12.5 PUT/s ($156/mo).
**6 pods** (the HA minimum) ⇒ 17 MiB/s each ⇒ only 4.2 MiB per 250 ms ⇒ **time**-triggered ⇒
24 PUT/s (**$312/mo**). HA doubles the write bill whenever the time trigger dominates.

**Fix: adaptive flush interval.** Target a segment size (default 8 MiB); if recent segments average
well below it, lengthen the interval (250 ms → 500 ms → 1 s, capped) until either the size target is
met or a latency ceiling is hit. This keeps PUT cost ≈ `totalThroughput / segmentSize` — independent
of pod count — and it degrades gracefully for low-traffic deployments, where pod count would
otherwise dominate the bill. Pair it with the per-stream trickle policy
([object-layout §7b](01-object-layout-and-format.md)), which solves the same problem one level down.

At this scale **pod count is driven by HA, not capacity** — 6 pods carry ~17 MiB/s in and
~17–50 MiB/s out each — which is precisely why the adaptive interval matters.

## 4. Safety does not depend on detecting failure

This is the property that makes the rest tractable. From
[metadata-and-cas §4, §7](03-metadata-and-cas.md):

- The epoch is **in the object path**, so a fenced leader's in-flight write lands where no reader looks.
- Every commit is a **write-once claim** on a sequence number (`If-None-Match: *`), so two processes
  that both believe they lead cannot both win a `seq`.
- The loser learns it is fenced from a 412 and stops.

**Consequence: the lease is a liveness and efficiency mechanism, not a safety mechanism.** We can
therefore challenge aggressively on evidence of death without waiting out a TTL, because an
over-eager takeover is merely wasteful, never incorrect.

**Lease policy:**
| Knob | Default | Rationale |
|---|---|---|
| TTL | 10 s | short enough to bound stalls, long enough to survive a GC pause |
| Renew | TTL/3 ≈ 3 s | two missed renewals before expiry |
| Voluntary release on `SIGTERM` | yes | turns a 10 s stall into sub-second on every rollout |
| Early challenge | on K8s endpoint removal + failed health probe | skips the TTL when death is evident |
| **Self-fencing** | release the lease voluntarily when commit latency exceeds SLO | see §6 gray failure |

## 5. Shard copies multiply consumers — pick the replication mode deliberately

`IndexMetadata.java:978-982`: *"In this mode, replicas will directly consume from the streaming
source... In the default document replication mode, this setting must be enabled. This mode is
currently not supported with segment replication."*

| Mode | `all_active` | Who consumes | Failover | Load on us |
|---|---|---|---|---|
| **Document replication** | **must be `true`** | **every shard copy independently** | instant — the replica is already current | serving bandwidth × copy count; **GETs unchanged** (the ingester caches) |
| **Segment replication** | must be `false` | primary only | replica promotes, then re-ingests a small tail from its recovered `batch_start` | 1× |

Two consequences worth internalising:

1. **`all_active` makes our offset stability an OpenSearch *consistency* requirement**, not just a
   replay convenience. Two copies independently consuming the same partition converge only because
   record *R* is at offset *N* on both. This is invariant I2
   ([metadata-and-cas §7](03-metadata-and-cas.md)) — now load-bearing for correctness of the search
   index itself, not merely for our own recovery.
2. **Another argument for proxying reads.** With direct plugin→S3 reads, `all_active` would multiply
   the GET count by the copy count. Served from the ingester nodes, GETs stay at 2 per segment per AZ
   and only serving *bandwidth* scales — 100 MiB/s becomes 200–300 MiB/s, which is nothing.
   ([discovery-and-tailing §2a](04-discovery-and-tailing.md).)

✅ **Decided: `SEGMENT` + `all_active=false`** —
[ADR-0009](../../internal/product/decisions/0009-replication-mode-segment-replication-primary-only-ingest.md).

The validator in `IndexMetadata.INGESTION_SOURCE_ALL_ACTIVE_INGESTION_SETTING` enforces a strict
XOR — `SEGMENT`+`true` and `DOCUMENT`+`false` are both rejected — the setting **defaults to
`false`**, and it is `Property.Final`, so the choice is made once at index creation.

An earlier version of this section leaned the other way. Primary-only ingest wins because it keeps
**our** serving bandwidth at 1× rather than copy-count× , and it is the OpenSearch default. The
cost is a slower failover: a promoted replica re-ingests the tail from its recovered `batch_start`,
which is safe because replay is at-least-once and offsets are stable (ADR-0001).

⚠️ It simplifies but does not remove the GC-watermark rule: with one consumer the `min()` across
copies is trivial, but the consumer's **identity changes on promotion** and the new primary may be
behind — so "silence freezes the watermark" is now the load-bearing half of
[ADR-0005](../../internal/product/decisions/0005-no-consumer-offset-store.md).

## 6. The failure matrix

| # | Failure | Detection | Effect | Recovery | Data impact |
|---|---|---|---|---|---|
| F1 | Ingester node, **graceful** (rollout) | K8s | none if drained correctly (§7) | seconds | none |
| F2 | Ingester node, **crash** | health probe / endpoint removal | in-flight unacked requests fail; subscribers on it disconnect | producers retry; subscribers reconnect in-AZ (ladder tier 1); new pod GETs segments it lacks | **none acked**; unflushed buffer replayed by producer ⇒ possible duplicates, deduped by `_id` |
| F3 | Pod crashes **after PUT, before commit** | — | segment orphaned | GC sweeps it after the grace period; producer retries ⇒ records land in a new segment | duplicates possible (at-least-once) |
| F4 | **Sequencer** pod lost | lease renewal stops | visibility stall for its slots | early challenge (§4) or TTL; new epoch → seal → continue | none — offsets already committed are immutable |
| F5 | **Whole AZ lost** | K8s / endpoints | ⅓ of pods, ⅓ of sequencer leases, ⅓ of OpenSearch nodes | see §6.1 | none |
| F6 | OpenSearch **data node** lost | OpenSearch | its shards reallocate | new node resumes from recovered `batch_start`; catch-up read burst | none |
| F7 | **AZ↔AZ partition**, all pods alive | may be undetected | commit RPCs fail cross-AZ | pods fall back to the **S3 commit-intent inbox** ([metadata-and-cas §10](03-metadata-and-cas.md)); possible dual leaders, resolved by seal | none — safety is detection-independent (§4) |
| F8 | **Gray failure**: pod alive but slow | latency SLO breach | worst case a sequencer that renews its lease but commits slowly ⇒ silent visibility lag | **self-fencing**: release the lease on SLO breach. Subscribers report staleness as a second signal | none, but lag |
| F9 | Object store degraded (regional) | error rates | writes fail; consumers idle harmlessly | producers retry; rate-limited backoff | none |
| F10 | Region loss | — | total | **out of scope** — cross-region replication is a non-goal |

### 6.1 Whole-AZ loss, step by step
1. **Producers in the lost AZ** are gone with it. Producers elsewhere are unaffected. If producers
   are external, the load balancer fails them over cross-AZ — **accept the cross-AZ transfer charge
   during a failure; availability outranks cost here.** Say so explicitly in the runbook so nobody
   "optimises" it away.
2. **~⅓ of sequencer leases** expire together. Guard against a takeover storm: jittered challenge
   delays, and a cap on concurrent takeovers per challenger.
3. **Serving in surviving AZs** is unaffected; each AZ has its own pods and cache.
4. **~⅓ of OpenSearch nodes** are gone. With `all_active` + cross-AZ replicas, the surviving copies
   are already current and promote immediately. With segment replication, the promoted replica
   re-ingests a short tail.
5. **Catch-up burst** as relocated shards resubscribe from older offsets. This is the moment
   [Q22](../50-open-questions.md)'s read priority classes earn their keep — historical reads must not
   starve live tail.
6. **Cost during the outage actually falls** (2 AZs ⇒ 1 non-writing AZ ⇒ 1 GET/segment) while
   cross-AZ producer traffic rises. Neither matters versus staying up.

## 7. Graceful shutdown ordering (gets more of the value than any failover code)

```
SIGTERM
  1. fail readiness  -> LB stops sending new requests (keep liveness passing)
  2. tell subscribers to reconnect elsewhere, and wait briefly   <-- do not just close the socket
  3. finish in-flight ingest requests
  4. flush buffers, PUT segments, commit
  5. release held leases (voluntary putIfMatch, expires immediately)
  6. hand off cached segments? no - just exit; the ring rebalances
  7. exit
```

Step 2 is easy to omit and expensive to omit: if the pod simply closes, every subscriber waits out a
timeout and then reconnects *simultaneously*. Step 5 turns a 10 s visibility stall into sub-second
on every deploy. **Most real-world "AZ resilience" incidents are actually rollouts.**

`terminationGracePeriodSeconds` must exceed the worst-case of steps 2–5 (flush + PUT + commit);
budget ~30 s and measure it.

## 8. RTO / RPO

| Property | Target | Basis |
|---|---|---|
| **RPO for acked writes** | **0** | We ack only after the segment is durable in a regional (multi-AZ) bucket **and** committed |
| RTO, ingest availability | seconds | LB failover to another pod in-AZ, or cross-AZ |
| RTO, visibility (offset assignment) | < 5 s | early lease challenge; ~10 s worst case on TTL expiry |
| RTO, AZ loss | minutes | dominated by OpenSearch shard promotion/reallocation, not by us |
| Consumer outage tolerance | = retention (default 6 h) | [compaction-and-retention §3](06-compaction-and-retention.md) — retention *is* the outage budget |

That last row is worth stating to operators as a single sentence: **retention is how long OpenSearch
can be down before you lose data.** Six hours is a deliberate default, not an arbitrary one.

## 9. Chaos test matrix

Each row asserts specific invariants; run these in CI against the local-FS and MinIO backends.

| Injection | Assert |
|---|---|
| Kill a pod mid-flush | no acked record lost; duplicates only |
| Kill after PUT, before commit | orphan GC'd; records re-ingested; no gap in offsets |
| Kill the sequencer mid-commit | I1–I4 hold; seal succeeds; offsets never reassigned |
| **`SIGSTOP` a sequencer** (gray failure) | self-fencing or challenge fires; no unbounded lag |
| Partition one AZ from the others (S3 still reachable) | inbox path engages; dual leaders resolved by seal; no divergence |
| Partition a pod from S3 only | pod fails readiness; stops accepting; does not silently buffer forever |
| Rolling restart of all pods | zero visibility gap > 1 s; no subscriber thundering herd |
| Kill an OpenSearch node mid-backlog | resumes from `batch_start`; catch-up does not starve live tail |
| Clock skew ±5 min on one pod | safety unaffected (epoch is a counter); only liveness degrades |

The `SIGSTOP` and clock-skew rows are the ones that find real bugs — crash-stop failures are the easy
case and the one everyone already tests.

## 10. Open questions

- Exact lease TTL and challenge policy under realistic GC pauses — measure, don't guess.
- Should the ingest path buffer to local disk (emptyDir) so an ungraceful crash does not force
  producer replay? It would reduce duplicates but reintroduces the WAL we deliberately avoided
  ([automq §2](../10-prior-art/02-automq.md)). **Leaning no.**
- `all_active = true` vs segment replication: confirm the support matrix and measure the serving
  bandwidth multiplier on the target OpenSearch version (§5).
- Does a takeover storm after AZ loss need explicit coordination, or is jitter enough at ~20 slots?
