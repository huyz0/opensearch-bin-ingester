# Consumer position: what Kafka tracks, what we must not rebuild, and the one watermark we do need

**Status:** proposal · **Confidence:** high for the OpenSearch mechanics (read from source), medium
for the GC policy constants · **Last updated:** 2026-08-30

**Read this if:** you are implementing GC/retention, the subscription protocol, or wondering whether
we need an offset-commit API.
**One-line takeaway:** **do not build a Kafka-style offset store.** OpenSearch already persists the
consumer position *inside the same Lucene commit as the documents* — atomically, which Kafka cannot
do. We need a position feed for exactly one purpose: **a conservative GC watermark**, and it must be
allowed only to *extend* retention, never to shorten it.

---

## 1. What Kafka does, and why it does not apply

Kafka stores committed offsets server-side in `__consumer_offsets` because its consumers are
**anonymous, interchangeable group members** that rebalance: when a partition moves to a different
consumer, the new owner must discover where the old one stopped, and the broker is the only shared
place to put that.

None of that holds here:

| Kafka assumption | Our reality |
|---|---|
| Anonymous group members, dynamic assignment | The consumer is a **specific shard copy**; OpenSearch's allocator decides ownership |
| Consumer has no durable store | The consumer **is a database** with its own commit protocol |
| Offset commit is decoupled from processing (`auto.commit`) | The pointer is committed **atomically with the indexed documents** |
| Many independent consumer groups per partition | Exactly one logical consumer (plus replicas, §4) |

That third row is the important one, and it is a place where OpenSearch's design is **strictly
better than Kafka's**, not merely different.

## 2. OpenSearch already tracks it — atomically

`IngestionEngine.java` (~line 404), inside the same `writer.commit()` that persists the documents:

```java
if (batchStartPointer != null) {
    commitData.put(StreamPoller.BATCH_START, batchStartPointer.asString());
}
...
writer.commit();
lastCommittedBatchStartPointer = batchStartPointer;
```

On open, the engine recovers the start pointer from commit data, falling back to
`pointer.init.reset`. So:

- **The marker and the data it describes are committed or lost together.** There is no window in
  which the position claims progress the index does not have. Kafka's `enable.auto.commit` is the
  classic source of exactly that bug; we inherit the fix for free.
- Crash between commits ⇒ replay from the last commit ⇒ **at-least-once**, deduplicated by `_id` +
  external versioning.

**Therefore: we must not build a second offset store.** An ingester-side marker would be a weaker,
asynchronous copy that can disagree with what is actually indexed — and when the two disagree, the
one in the Lucene commit is right and ours causes data loss.

## 3. The subscription is client-driven, and stays that way

The plugin sends `fromOffset` per partition on subscribe
([discovery-and-tailing §2, §2d](04-discovery-and-tailing.md)). The ingester holds **no cursor** —
it is `seek()`, not a server-side consumer group.

Keep this property. It is what lets an ingester node die mid-stream with no consequence beyond a
reconnect, and it is a large part of why the ingester is genuinely stateless with respect to
consumers.

## 4. The one thing we *do* need: a GC watermark

Three different consumers of "where is the reader?", with three different consistency requirements:

| Purpose | Who needs it | Requirement | Source |
|---|---|---|---|
| **Resume after restart** | the shard itself | must be exactly consistent with indexed data | Lucene commit data — **already exists, do not touch** |
| **Retention / GC safety** | the ingester | a *conservative lower bound* is sufficient; being stale means keeping data longer, which is safe | a reported feed (§5) |
| **Lag monitoring** | operators | approximate | `PollingIngestStats.pointerBasedLag` / `lagInMillis`, and our pushed tail offset |

Only the middle row needs anything new, and it needs far weaker guarantees than an offset store.

Without it, retention is a data-loss mechanism: a purely time-based policy deletes data a lagging
or restarting consumer has not read ([compaction-and-retention §3](06-compaction-and-retention.md)).

## 5. How to collect it

**Piggyback a `progress` frame on the existing subscription.** It is already an open, same-AZ,
metadata-only channel:

```
-> event: progress
   { "index":"<uuid>", "shardCopy":"<allocationId>", "partition":3, "consumedUpTo": 41822 }
```

Send it every few seconds, or on change, batched across all partitions on the node. Cost:
negligible bytes, zero object-store requests.

**Alternative considered and rejected:** having the ingester poll OpenSearch's
`GetIngestionStateAction` (`ShardIngestionState` exposes `batchStartPointer`, `isPrimary`,
`nodeName` per shard). It would require the ingester to hold OpenSearch credentials and know the
cluster topology — a coupling we have otherwise avoided — and it returns the *same in-memory value*
anyway (§6.1). Useful for operators and debugging; not the mechanism.

## 6. Five traps, four of which silently lose data

### 6.1 The reported pointer is **not** the committed pointer
`IngestionEngine.getIngestionState()` reports `streamPoller.getBatchStartPointer()` — the
**in-memory** pointer. The engine separately tracks `lastCommittedBatchStartPointer`, and **only the
latter survives a crash.** OpenSearch does not expose the committed one.

So every watermark we receive is **optimistic**: it can be ahead of what a restart would resume
from, by up to one Lucene commit interval (which is driven by translog flush thresholds and can be
minutes).

> **Rule: a consumer watermark may only *extend* retention, never shorten it below the time floor.**
> Time-based `minRetention` is the safety net; watermarks are a brake, never an accelerator.

### 6.2 Take the **minimum across all shard copies**
With `all_active = true` — required for document replication — **every shard copy independently
consumes the partition** ([failure-domains §5](08-failure-domains-and-resilience.md)). A lagging
replica in another AZ must not have its data deleted. Key the feed by allocation id, and take
`min()` over all known copies.

### 6.3 Silence is not progress
A copy that stops reporting (node down, network blip, pod restart) must **freeze** the watermark at
its last known value — not be dropped from the `min()`. Dropping a silent copy is how a node that is
down for ten minutes comes back to deleted data. Only an explicit deregistration, or expiry well
past `minRetention`, removes a copy from consideration.

### 6.4 Distinguish "moved" from "temporarily silent"
Shard relocation produces a new allocation id reporting for the same partition while the old one
goes quiet. Retire an allocation id only on explicit deregistration or a long timeout — never merely
because a new one appeared, since both may legitimately exist mid-relocation.

### 6.5 A paused shard pins retention forever
OpenSearch exposes pause/resume APIs; a paused shard reports a frozen pointer indefinitely, and
storage grows without bound. Hence the hard ceiling below — and it must **alarm**, because crossing
it means a consumer *will* lose data. Silently deleting is worse than a loud alert.

## 7. The retention rule, stated precisely

```
delete(segment)  iff
      age(segment) > minRetention                      # default 6h, the floor
  AND min(watermark over all known copies) > segment.endOffset + safetyMargin
  AND every known copy reported within reportTimeout    # else the min() is stale
  OR  age(segment) > maxRetention                       # hard ceiling -- ALARM, do not do this quietly
```

- `minRetention` (6 h) is the **outage budget**: how long OpenSearch can be down before data is lost.
- `safetyMargin` covers trap 6.1 — size it above the observed Lucene commit interval, not below.
- `maxRetention` bounds cost and is an incident, not a routine event.

## 8. Where the watermark lives

Add `consumerWatermark` per stream to the **existing checkpoint object**
([metadata-and-cas §9](03-metadata-and-cas.md)) — no new object, no new CAS, and it already carries
`nextOffset` and `oldestRetainedOffset` per stream.

On sequencer failover, watermarks are simply relearned from the next round of reports. GC is a
periodic background job and can require *fresh* reports before deleting anything, so a failover
means "GC pauses briefly", which is the correct failure direction.

## 9. What operators should see

- Per partition: tail offset, min watermark, lag in offsets and in milliseconds, oldest retained
  segment age.
- **Alarms:** any copy silent beyond `reportTimeout`; any segment approaching `maxRetention`; any
  paused shard older than `minRetention`.
- The single number worth a dashboard tile: **`minRetention − oldest unread data age`**, i.e. how
  much outage budget is left.

## 10. Open questions

- Can the plugin observe the *committed* pointer rather than the in-memory one — a commit listener,
  or reading commit data — to shrink `safetyMargin`? Worth a look; it would make GC materially
  tighter and therefore storage cheaper.
- Should `safetyMargin` be adaptive, derived from the observed interval between watermark jumps
  (a proxy for the commit cadence) rather than configured?
- Is `reportTimeout` per copy or per node? Per node is fewer moving parts; per copy is more precise
  during relocation.
