# The poll loop, and the $200k/month idle bill

**Status:** stable · **Confidence:** high (read from `DefaultStreamPoller.java`, OpenSearch 3.8.0) · **Last updated:** 2026-08-29

**Read this if:** you are implementing `IngestionShardConsumer`, or you are about to assume that
"just poll the object store" is fine.
**One-line takeaway:** OpenSearch runs **one poller thread per shard** calling `readNext` roughly
**ten times a second**, and we cannot change that from a plugin. The only viable design blocks
inside `readNext` on a **node-level** push subscription so that an idle cluster issues **zero**
object-store requests.

---

## 1. What the poller actually does

`server/src/main/java/org/opensearch/indices/pollingingest/DefaultStreamPoller.java`:

```java
private static final int DEFAULT_POLLER_SLEEP_PERIOD_MS = 100;
...
// per iteration:
updatePointerBasedLagIfNeeded();                 // calls consumer.getPointerBasedLag(...)
if (paused || isWriteBlockEnabled) { sleep(100); continue; }
results = (forcedShardPointer != null)
        ? consumer.readNext(forcedShardPointer, true, maxPollSize, pollTimeout)
        : consumer.readNext(maxPollSize, pollTimeout);
if (results.isEmpty()) { sleep(100); continue; }  // <-- the whole story is here
forcedShardPointer = processRecords(results);
```

Defaults (`DefaultStreamPoller.Builder`, `IndexMetadata`):

| Knob | Default | Setting |
|---|---|---|
| `maxPollSize` | 1000 messages | `index.ingestion_source.poll.max_batch_size` |
| `pollTimeout` | 1000 ms | `index.ingestion_source.poll.timeout` |
| empty-result sleep | **100 ms, hard-coded** | — |
| lag update interval | 10 s | `index.ingestion_source.pointer_based_lag_update_interval` |
| warmup lag threshold | 100 | `index.ingestion_source.warmup.lag_threshold` |

One poller instance exists per `IngestionEngine`, i.e. **per shard**. There is no node-level
throttle, and the 100 ms sleep is not configurable.

## 2. The naive-implementation cost, computed

Scenario A from [cost-model](../00-problem/02-cost-model.md): 1,600 shards across 9 data nodes.
If `readNext` returns promptly with no data, each poller loops at ~10 Hz ⇒ **~16,000 calls/s**.

| Naive `readNext` implementation | Requests/s | Cost/month | |
|---|---|---|---|
| `LIST` the prefix to look for new objects | 16,000 LIST/s | **$207,000** | catastrophic |
| `GET` a manifest / marker object | 16,000 GET/s | **$16,600** | still absurd |
| `GET`, but block the full `pollTimeout` (1 s) first | ~1,450 GET/s | **$1,500** | still 5x the entire ingest cost |
| **Block on a node-level push subscription** | **0** | **$0** | correct |

**This is the cost of an *idle* cluster.** It is paid whether or not anyone is writing data. It is
larger than the entire write path. Any design review that does not check this number is incomplete.

## 3. The lever: `readNext(maxMessages, timeoutMillis)` may block

The contract passes us a timeout and declares `throws TimeoutException`. Nothing requires us to
return immediately. So:

```
readNext(maxMessages, timeoutMillis):
    if localQueue has records:  drain up to maxMessages, return
    else:                       block on localQueue.poll(timeoutMillis)   // no I/O at all
```

An idle shard costs one parked virtual/platform thread and **zero** object-store requests. When
data arrives, the shared subscription pushes it into every interested shard's queue and all the
parked pollers wake at once.

The 100 ms sleep after an empty result is then harmless: it only adds ≤100 ms of latency after a
genuinely empty 1 s wait, and costs nothing.

> **Tuning note:** raise `index.ingestion_source.poll.timeout` (e.g. to 5–30 s) to lengthen the
> block. It costs nothing extra and reduces wakeup churn. Do **not** lower it.

## 4. The hard structural requirement: node-level sharing

The factory is called **once per shard**, so a naive implementation creates 178 subscriptions,
178 HTTP connections and 178 independent caches on one node. Everything expensive must instead
live in a **node-level singleton**, created in `Plugin.createComponents()` (or a static registry
keyed by the store config) and handed to every consumer:

```
BinStoreIngestionPlugin (node singleton)
├── TailSubscriber          one HTTP/2 connection per node to a SAME-AZ ingester node
│                           subscribes to the union of (index, partition) this node hosts
├── ObjectBlockCache        chunk cache keyed by (objectKey, chunkIndex);
│                           concurrent requests for the same chunk are DEDUPLICATED into one GET
├── FetchCoalescer          merges per-shard byte-range needs into whole-object / coalesced GETs
└── BinStorePartitionConsumer  (one per shard) -- owns only a queue + a pointer
```

Two properties this buys, both directly from the cost model:
- **R5**: one GET per object per node, not per shard. Without it, cost scales with shard count.
- **R3**: zero idle requests.

The block cache is the same idea as WarpStream's per-AZ "distributed mmap"
([warpstream.md](../10-prior-art/01-warpstream.md) §3), scoped down to a single node because we
have exactly one consumer per partition. A per-AZ distributed variant is a v2 optimisation.

## 5. Other calls that must not touch the object store

| Call | Frequency | Serve from |
|---|---|---|
| `getPointerBasedLag(p)` | every 10 s **per shard**, even while paused | last known tail offset pushed by the subscription |
| `latestPointer()` | warmup + lag paths | same cached tail offset |
| `earliestPointer()` | init | cached from the subscription's snapshot |
| `pointerFromTimestampMillis(t)` | reset only (rare) | one checkpoint read; acceptable |
| `pointerFromOffset(s)` / `parsePointerFromString` | pure parse | no I/O |

1,600 shards × one request every 10 s = 160 req/s of pure metadata chatter = **$165/month if it is
a GET, $2,070/month if it is a LIST**, for a number nobody reads. Serve it from memory.

## 6. Failure and backpressure behaviour to respect

- **Any exception thrown from `readNext` pauses ingestion for that shard** ("Pausing ingestion.
  Fatal error occurred in polling the shard…") and requires operator intervention to resume.
  Therefore: **retry transient object-store and service errors inside the consumer**; only throw
  for genuinely unrecoverable states. A 503 from S3 must never surface.
- On a processing failure the poller sets `forcedShardPointer` and re-calls
  `readNext(pointer, includeStart=true, …)`. Our consumer must support **cheap re-reads from an
  arbitrary recent pointer** — keep a small ring buffer of recently delivered records per partition
  so a retry does not become an object-store fetch.
- `PartitionedBlockingQueueContainer` + `internal_queue_size` already provide backpressure into
  the engine. Our own queue should be small (hundreds of records), with the real buffering left to
  the object store — do not build a second deep queue.

## 7. Checklist for the consumer implementation

- [ ] `readNext` blocks on an in-memory queue; **never** performs I/O when there is nothing new
- [ ] Zero object-store requests when idle (assert this in an integration test)
- [ ] One subscription + one cache per **node**, not per shard
- [ ] Concurrent chunk fetches deduplicated
- [ ] `getPointerBasedLag` / `latestPointer` served from memory
- [ ] Transient errors retried internally; never propagate to the poller
- [ ] Recent-record ring buffer so `forcedShardPointer` retries are free
- [ ] Bounded memory: queue depth and cache size configured, not unbounded

---

**Next:** [03-plugin-packaging.md](03-plugin-packaging.md)
