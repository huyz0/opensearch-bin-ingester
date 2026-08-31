# Prior art: AutoMQ / S3Stream

**Status:** stable · **Confidence:** high (grounded in cloned source, see §7) · **Last updated:** 2026-08-29

**Read this if:** you are designing the on-object binary format, the reader's range-read strategy,
or the pluggable store SPI.
**One-line takeaway:** AutoMQ is open source and written in Java — it is our best *code* reference
for object format, the speculative tail read, and a clean `ObjectStorage` abstraction. Its WAL
layer is largely irrelevant to us because OpenSearch is our durability consumer, not a broker.

---

## 1. Shape

A stateless fork of Apache Kafka that offloads storage to S3. Three layers: **ElasticLog**
(Kafka log API) → **S3Stream** (the interesting part: a stream abstraction over object storage)
→ KRaft metadata control plane.

Two object kinds:
- **Stream Set Object (SSO)** — one object containing data for *many* streams (partitions). This is
  the write-path object, equivalent to our bundled segment. Cap: `maxStreamNumPerStreamSetObject = 20000`.
- **Stream Object** — one object for a single stream, produced by compaction to make catch-up reads
  cheap.

## 2. WAL — and why we mostly skip it

AutoMQ writes to a WAL first, then uploads to S3. WAL media options: EBS/Regional EBS
(sub-ms), NFS (ms), and **S3 WAL** (hundreds of ms, the open-source default). Crucially, the WAL
"mixes data from all partitions of a node into a single WAL file or object", which is the same
bundling insight as WarpStream.

**We do not need a WAL.** AutoMQ needs one because it must ack a Kafka producer with Kafka's
durability semantics before the S3 upload completes. Our latency budget is seconds, so we can ack
*after* the object PUT and skip the entire WAL layer, its recovery logic, and its EBS cost. This
is a major simplification — do not accidentally re-introduce it.

## 3. Convergent evidence on flush thresholds

`ObjectWALConfig` defaults (`s3stream/.../wal/impl/object/ObjectWALConfig.java`):

```java
private long batchInterval   = 250;              // 250 ms
private long maxBytesInBatch = 8 * 1024 * 1024L; // 8 MiB
private long maxUnflushedBytes = 1024*1024*1024L;// 1 GiB backpressure ceiling
private int  maxInflightUploadCount = 50;
```

WarpStream independently landed on **250 ms / 4–8 MiB**. Two unrelated teams converging on the
same constants is the strongest signal in this corpus: **start at 250 ms / 8 MiB and tune from
there.** Also note `maxUnflushedBytes` — an explicit backpressure ceiling, which we need too
(see [streaming-io](../40-implementation/02-streaming-io-and-memory.md)).

Other useful defaults (`Config.java`): `objectBlockSize = 1 MiB`, `objectPartSize = 16 MiB`
(multipart part size), `streamSplitSize = 16 MiB`, `blockCacheSize = 100 MiB`,
`objectRetentionTimeInSecond = 600`.

## 4. Object format — copy this

From `ObjectWriter.java` / `ObjectReader.java` / `DataBlockIndex.java`:

```
+----------------------------------------------+
| DataBlock 0   [magic 0x5A][u32 recordCount]  |  ~1 MiB each (objectBlockSize)
|               [u32 dataLength][records...]   |
| DataBlock 1   ...                            |
| ...                                          |
+----------------------------------------------+
| IndexBlock: N x 36-byte entries              |
|   u64 streamId | u64 startOffset             |
|   u32 endOffsetDelta | u32 recordCount       |
|   u64 blockPosition | u32 blockSize          |
+----------------------------------------------+
| Footer (48 bytes):                           |
|   u64 indexStartPosition | u32 indexLength   |
|   ... | u64 MAGIC 0x88e241b785f4cff7         |
+----------------------------------------------+
```

Reader lookup is a **binary search over the fixed-width index** (`IndexBlockOrderedBytes`) —
fixed-size entries mean no parsing is needed to seek. We should keep that property.

### The speculative tail read (the single best trick in this codebase)
`ObjectReader.asyncGetBasicObjectInfo()`:

```java
int guessIndexBlockSize = 8192 + (int)(metadata.objectSize() / (1024*1024) * 36);
asyncGetBasicObjectInfo0(Math.max(0, metadata.objectSize() - guessIndexBlockSize), true);
```

It **guesses** how large the index is (36 bytes per ~1 MiB block, plus 8 KiB slack) and issues a
*single* range GET of the object's tail that usually captures footer **and** index together. If
the guess was short, the footer it did read tells it the true `indexBlockPosition` and it retries
once (`ObjectParseException` carries the position).

This turns the textbook "GET footer -> GET index -> GET data" three-request dance into
**one request in the common case**. At $4e-7 per GET this is worth real money at scale, and it
is a pattern to reuse verbatim. See rule R7 in [cost-model](../00-problem/02-cost-model.md).

> **Our variant:** because *we* mint the object key, we can do better still and embed the exact
> header length in the key (`...-h4096.seg`), making it one request with **zero** guessing.
> See [object-layout](../30-design-space/01-object-layout-and-format.md) §5.

## 5. The `ObjectStorage` SPI — a good shape to imitate

`s3stream/.../operator/ObjectStorage.java` is deliberately small:

```java
Writer writer(WriteOptions options, String objectPath);
CompletableFuture<ByteBuf> rangeRead(ReadOptions options, String objectPath, long start, long end);
CompletableFuture<WriteResult> write(WriteOptions options, String objectPath, ByteBuf buf);
CompletableFuture<String> createMultipartUpload(...);   // + uploadPart / uploadPartCopy / complete
CompletableFuture<List<ObjectInfo>> list(String prefix);
CompletableFuture<Void> delete(List<ObjectPath> objectPaths);
```

Implementations shipped: `AwsObjectStorage`, `LocalFileObjectStorage`, `MemoryObjectStorage`,
plus `MultiPartWriter`, `ProxyWriter`, `RetryStrategy`, `TrafficRateLimiter`, `S3LatencyCalculator`.
`BucketURI` gives a single-string bucket config (`s3://bucket?region=...&endpoint=...`).

**Take:** the narrowness, the local-FS + in-memory implementations for tests, the URI-style config,
the built-in rate limiter and latency tracker.
**Add:** conditional-write options (`ifNoneMatch`, `ifMatch`) — AutoMQ's SPI has no CAS because it
gets ordering from KRaft. Ours is CAS-centric. See
[pluggable-store-abstraction](../30-design-space/07-pluggable-store-abstraction.md).

## 6. Compaction

Two levers, both time/size triggered: SSO compaction (`streamSetObjectCompactionInterval = 5` min,
`maxObjectNum = 500`, `forceSplitPeriod = 120` min) merges many SSOs and *splits out* large
single-stream regions into Stream Objects; stream-object compaction runs hourly up to 10 GiB.

**Applicability:** the *split* idea is the interesting one — once a partition has accumulated
enough contiguous data, promote it to its own object so catch-up reads are one sequential scan.
See [compaction-and-retention](../30-design-space/06-compaction-and-retention.md).

## 8. How AutoMQ avoids excessive GET and LIST — and how it notifies remote readers

This section answers the question directly, from the code in `.tmp/automq`.

### 8.1 LIST: they simply do not use the bucket for discovery

`list()` appears in **exactly two places** in `s3stream` outside the store implementations:

```
wal/impl/object/DefaultReader.java:259   objectStorage.list(nodePrefix)   // WAL recovery scan
wal/impl/object/DefaultWriter.java:153   objectStorage.list(nodePrefix)   // writer startup
```

Both are **node-startup / WAL-recovery only**, and both are scoped to a single `nodePrefix`. There
is **no LIST anywhere on the steady-state read or write path.** Object discovery comes from the
**KRaft controller**: object metadata is a replicated Kafka metadata record, pushed to every broker
(`metadata/.../NodeS3StreamSetObjectMetadataImage`, `SortedStreamSetObjectsList`).

**Transfers to us as:** the commit log plus the push channel is our substitute for KRaft's metadata
propagation. Their LIST usage is precisely our tier-4 fallback
([discovery-and-tailing](../30-design-space/04-discovery-and-tailing.md) §3) — a recovery path, never
a hot path. Same conclusion, reached independently.

### 8.2 GET for tail data: served from the write buffer, never from S3

`S3Storage.read()` (line ~565) checks the in-memory `LogCache` **first**:

```java
List<StreamRecordBatch> logCacheRecords = firstCache.get(context, streamId, startOffset, endOffset, maxBytes);
if (!logCacheRecords.isEmpty() && logCacheRecords.get(0).getBaseOffset() <= startOffset) {
    return CompletableFuture.completedFuture(new ReadDataBlock(logCacheRecords, CacheAccessType.DELTA_WAL_CACHE_HIT));
}
// ... only now fall through to blockCache.read(...) which issues S3 range reads
```

The decisive detail is the **eviction policy**. After a block is uploaded to S3, `LogCache.markFree()`
only *marks* it:

```java
public CompletableFuture<Void> markFree(LogCacheBlock block) {
    block.free = true; updateEvictableSize(); ... tryRealFree();
}
private void tryRealFree() {
    if (currSize <= capacity * 0.9 && blockCount.get() <= MAX_BLOCKS_COUNT) return;  // keep it
    ...
}
```

Memory is reclaimed only above **90% of `walCacheSize` (200 MiB default)**. So freshly written data
lingers in RAM and **a tailing consumer is served from the write buffer, at zero object-store cost
and zero S3 latency**. The upload to S3 buys durability; it is not what serves the read.

**This is the single most transferable idea in the codebase for our read-cost problem.** See §8.5.

### 8.3 GET for catch-up reads: caches plus aggressive readahead

| Mechanism | Where | Default |
|---|---|---|
| `LogCache` (delta WAL cache) | tail reads | `walCacheSize = 200 MiB` |
| `DataBlockCache` / `blockCache` | catch-up reads | `blockCacheSize = 100 MiB` |
| `ObjectReaderLRUCache` | caches *parsed object indexes* so the directory is not re-read/re-parsed | — |
| `StreamReader.Readahead` | grows 512 KiB → **32 MiB** (`AUTOMQ_MAX_READAHEAD_SIZE`) | amortises one GET over a large sequential span |

Readahead is the catch-up equivalent of WarpStream's 4 MiB chunk paging: pay one request, get a lot
of sequential data.

### 8.4 Notifying a *remote* node: the `zerozone` snapshot-read subscription

AutoMQ normally needs no notification protocol at all, because **it has partition leadership** — the
broker that accepts writes for partition *P* is the broker that serves reads for *P*, so tail data is
in local RAM by construction. (This is exactly why AutoMQ needs no distributed cache and WarpStream,
being leaderless, does.)

Where AutoMQ *does* need a remote node to tail another node's data — its **Zero Zone** feature, in
`core/src/main/java/kafka/automq/zerozone/` — it uses a dedicated subscription RPC,
**`AutomqGetPartitionSnapshot`, API key 516**:

```
Request : SessionId | SessionEpoch | RequestCommit | Version
Response: SessionId | SessionEpoch
          Topics[] -> Partitions[] -> Operation (0=ADD, 1=PATCH, 2=REMOVE)
                                       LogMetadata { FirstUnstableOffset, LogEndOffset, segments[], streamMap }
                                       StreamMetadata { StreamId, EndOffset, LastTimestampOffset }
                                       ConfirmWalEndOffset
                                       ConfirmWalConfig
                                       ConfirmWalDeltaData   <-- the actual bytes, optional
```

Three properties worth copying:

1. **Session-based incremental delta**, `SessionId` + `SessionEpoch`, with per-partition
   `ADD | PATCH | REMOVE` operations — the same shape as Kafka's incremental fetch sessions
   (KIP-227). Responses carry *changes*, not full state. At our 120,000-stream scale a full
   subscription snapshot on every reconnect would be prohibitive; this is how to avoid it.
2. **`ConfirmWalDeltaData` inlines the new bytes in the notification.** The schema says it plainly:
   *"The confirm WAL delta data between two end offsets. It's an optional field. **If not present,
   the client should read the delta from WAL**."* When the delta is small, the subscriber needs
   **zero** object-store requests and zero extra round trips — the notification *is* the data. When
   it is large, the subscriber falls back to reading the object.
3. **Continuous request loop:** `SnapshotReadPartitionsManager.REQUEST_INTERVAL_MS = 1` — a 1 ms
   re-request, i.e. effectively a long-poll/streaming session rather than a periodic poll. Note that
   because it is an RPC to a *broker*, not to S3, a tight loop costs nothing.

The receiving side (`SubscriberReplayer.onNewWalEndOffset(walConfig, endOffset, walDeltaData)`)
replays the delta into a local `SnapshotReadCache`, which is itself a `LogCache` sized at
`walCacheSize / 3 * 2` — so remotely-tailed data lands in the same in-memory structure that local
writes do, and is served the same way.

### 8.5 What this means for our design

| AutoMQ mechanism | Our equivalent | Status |
|---|---|---|
| KRaft metadata push | commit log + push channel | already designed |
| No LIST on hot paths | LIST is tier-4 recovery only | already designed |
| `LogCache` serves tail reads from the write buffer | **serve tail ranges from the ingester node's own buffer** | **new — see [discovery-and-tailing](../30-design-space/04-discovery-and-tailing.md) §2b** |
| `markFree` without eviction until 90% | keep flushed segments resident; reclaim under pressure only | **new** |
| `ConfirmWalDeltaData` inlined in the notification | **inline small record batches in the push event** | **new** |
| Session + epoch, ADD/PATCH/REMOVE | incremental subscription protocol | **new** |
| `ObjectReaderLRUCache` | cache parsed segment headers by key | **new** |
| Readahead 512 KiB → 32 MiB | catch-up readahead in the plugin | **new** |
| Partition leadership makes tail reads local | we are leaderless; the per-AZ cache substitutes | inherent difference |


## 7. Ground truth on disk

```
.tmp/automq/                                   # shallow clone, blob-filtered
  s3stream/src/main/java/com/automq/stream/s3/
    ObjectWriter.java  ObjectReader.java  DataBlockIndex.java    # <- object format
    Config.java                                                  # <- tunables
    operator/ObjectStorage.java  operator/LocalFileObjectStorage.java  # <- store SPI
    operator/MultiPartWriter.java  operator/AbstractObjectStorage.java
    wal/impl/object/ObjectWALConfig.java                         # <- 250ms / 8MiB
    cache/LogCache.java                                          # <- tail reads from write buffer
    cache/S3Storage.java (read path ~line 565)                   # <- LogCache checked before S3
    cache/blockcache/StreamReader.java                           # <- readahead 512KiB -> 32MiB
../core/src/main/java/kafka/autobalancer/                        # <- rebalancing, 60 s anomaly loop
    config/AutoBalancerControllerConfig.java  goals/NetworkIn|OutUsageDistributionGoal.java
../core/src/main/java/kafka/automq/zerozone/                     # <- remote tail subscription
    SubscriberRequester.java  SubscriberReplayer.java  SnapshotReadPartitionsManager.java
../clients/src/main/resources/common/message/
    AutomqGetPartitionSnapshot{Request,Response}.json            # <- the notify protocol (apiKey 516)
```

`.tmp/` is git-ignored. Re-clone with:
`git clone --depth 1 --filter=blob:none https://github.com/AutoMQ/automq.git .tmp/automq`

## 9. Why AutoMQ needs KRaft and we do not

⚠️ Read this before proposing a consensus layer — the question recurs. The
decision is [ADR-0011](../../internal/product/decisions/0011-no-consensus-cluster.md);
this section is the evidence for why prior art differs.

### The empirical finding

```
grep -rn "IfNoneMatch|ifMatch|ifGenerationMatch" s3stream/src/main/   ->  no matches
```

**AutoMQ uses zero object-store compare-and-swap.** Every coordination decision
goes through KRaft controller RPCs. That is not an oversight — it is what the
next three points explain.

### 9.1 The historical reason: the primitive did not exist

| | |
|---|---|
| WarpStream and AutoMQ designed | 2023 |
| S3 `If-None-Match` (create-if-absent) GA | **20 August 2024** |
| S3 `If-Match` (compare-and-swap) GA | **25–26 November 2024** |

**Neither system could have used object-store CAS, because it did not exist when
they were built.** WarpStream reached for DynamoDB, AutoMQ inherited KRaft. This
is the single largest reason the architectures differ, and it means their choice
is not evidence against ours — we are designing with a primitive they did not
have.

### 9.2 They need a controller for things we do not have

The RPCs AutoMQ added to Kafka
(`clients/src/main/resources/common/message/`) show what the controller is
actually for:

| RPC | Purpose | Do we need it? |
|---|---|---|
| **`PrepareS3Object`** — `NodeId`, `PreparedCount`, `TimeToLiveInMs` | a **global object-ID allocator**: brokers lease a batch of monotonic object IDs before writing | **No.** We mint keys locally from pod id + timestamp + ULID. A global counter needs an arbiter; a locally-unique name does not |
| `OpenStreams` / `CloseStreams` — `NodeId`, `NodeEpoch`, `StreamId`, `StreamEpoch` | per-stream leadership with two-level epoch fencing | **No.** Writes are leaderless (ADR-0001); no stream is "opened" by a node |
| `CommitStreamSetObject` / `CommitStreamObject` | commit object metadata, assign order | Yes — but this is the *one* thing, and it is what our write-once chain does |
| `AutomqRegisterNode` / `AutomqGetNodes` | a consistent node roster (object ownership, compaction assignment) | **No.** K8s endpoints, with no consistency requirement |
| `AutomqPreparePartitionHandoff` | partition handoff | **No.** OpenSearch's allocator owns shard placement |
| `AutomqUpdateGroup` | consumer groups | **No.** Non-goal |
| `CreateStreams` / `DeleteStreams` / `DescribeStreams` | Kafka topic lifecycle and admin | **No.** Index lifecycle is OpenSearch's |

Only **one** row is shared. Everything else is Kafka control-plane surface —
partition leadership, consumer groups, transactions, ACLs, topic metadata — that
is consensus-shaped by nature and that we simply do not have. **The controller
is not there to order the log; it is there to be Kafka.**

`PrepareS3Object` deserves emphasis: a global monotonic ID allocator *requires* an
arbiter, full stop. Choosing content-addressed or locally-unique object names
instead removes an entire class of coordination, and it is the cheapest
architectural decision available to anyone building this.

### 9.3 The clock-vs-counter tell

```java
// core/src/main/java/kafka/automq/AutoMQConfig.java:352
private final long nodeEpoch = System.currentTimeMillis();
```

**AutoMQ's node epoch is a wall-clock timestamp.** SlateDB's — and ours — is a
counter incremented under CAS, precisely to avoid clock assumptions
([diskless-kafka §2](03-diskless-kafka-and-cas-systems.md)).

AutoMQ can afford a clock because the **controller is the arbiter**: it rejects a
stale epoch, so the clock only needs to be roughly monotonic. We have no arbiter,
so our epoch must be a counter whose uniqueness the store itself guarantees.

**This is the trade in one line: an arbiter buys you cheap, fast decisions and
lets you be sloppy about time; no arbiter forces every decision to be a single
atomic store operation, and forbids clock-based reasoning entirely.**

### 9.4 Latency

AutoMQ targets Kafka-compatible latency, and its EBS WAL is sub-millisecond. A
KRaft commit is ~1–2 ms; an S3 conditional PUT is ~30–60 ms. Object-store CAS
would be **30× their WAL latency and would dominate their write path**. Our budget
is seconds, so the same 30–60 ms disappears into it.

⚠️ **This is the honest reason to reverse our decision if the requirement
changes.** If sub-50 ms visibility is ever needed, AutoMQ's answer — a fast WAL
plus a consensus-backed controller — is the right one, and it is a v2
architecture rather than a tweak.

### 9.5 What their choice costs them

- **Stateful controller nodes.** AutoMQ's *brokers* are stateless with respect to
  data; the KRaft quorum is not. "Stateless" there is a claim about data, not
  metadata.
- **A quorum that must survive AZ loss**, with its own membership, split-brain and
  rescaling concerns.
- ⚠️ **Committed metadata is only as durable as the quorum until it is
  checkpointed.** With CAS, a committed offset is in a regional bucket at the
  instant of commit — which is why ADR-0011 concludes Raft would make *our*
  durability worse, not better.


## 10. How AutoMQ bundles and scales — and why our answer differs

⚠️ Read this before proposing that we copy their elasticity model. The mechanisms
are good; two of the three do not transfer, for reasons that are structural.

### 10.1 They do **not** adapt bundling. They do not need to.

`ObjectWALConfig` and `Config` are fixed: `batchInterval = 250 ms`,
`maxBytesInBatch = 8 MiB`, `objectBlockSize = 1 MiB`, `walUploadThreshold = 100 MB`.
Nothing in the codebase varies them with load.

The main-storage upload is triggered by **memory pressure, not time**:

```java
long walUploadThreshold = Math.min(deltaWALCacheSize / 3, config.walUploadThreshold());
long forceUploadThreshold = cacheCapacity * 4 / 5;      // upload when the cache is 80% full
private final FutureTicker forceUploadTicker = new FutureTicker(100, MILLISECONDS, …);  // debounce, not a timer
```

They can wait for memory because **the WAL already provides durability**. We
cannot: in default mode our segment PUT *is* the durability event, so the trigger
must be time-bounded or a crash loses everything since the last flush. That single
difference is why we needed an adaptive interval and they did not.

### 10.2 The real reason EBS WAL is recommended: it is a **PUT-cost** mechanism

The WAL is usually described as a latency choice. The cost table says otherwise:

| Ingest | S3 WAL (250 ms × 3 brokers) | EBS WAL (only the 100 MB main upload) |
|---|---|---|
| 1 MiB/s | 12.0 PUT/s → **$156/mo** | 0.010 PUT/s → **$0.14/mo** |
| 50 MiB/s | 12.0 PUT/s → $156/mo | 0.52 PUT/s → $6.79/mo |
| 100 MiB/s | 12.0 PUT/s → $156/mo | 1.05 PUT/s → $13.59/mo |

**Putting the frequent small writes on a disk and only the rare large batches on
S3 is the whole trick.** With an S3 WAL they pay a flat ~$156/month floor
regardless of throughput — the same `writers ÷ interval` floor we hit in
[ADR-0016](../../internal/product/decisions/0016-designated-writer-per-az.md).

### 10.3 ⚠️ Our fast mode cannot borrow this — the arithmetic forbids it

Fast mode gives us a WAL, so we *could* defer segment uploads to 100 MB the way
they do. It never pays:

| Ingest | Cross-AZ WAL replication (quorum 2) | PUT saving from deferring | Ratio |
|---|---|---|---|
| 1 MiB/s | $54/mo | $7.64/mo | **7.1×** |
| 50 MiB/s | $2,718/mo | $74/mo | **36.6×** |
| 100 MiB/s | $5,436/mo | $148/mo | **36.6×** |

Both terms are linear in bytes, so **the ratio never flips at any throughput**.

> **Fast mode is a latency feature only. It never pays for itself in PUTs.**
> Record that plainly, because "we already have a WAL, so let's also bundle
> lazily" is the obvious next thought and it is wrong.

The difference is that AutoMQ's WAL is a **local disk relying on EBS's own
durability** (single-AZ, 5–9 nines), not a cross-AZ replicated log. That is
cheaper — and it is why their brokers are stateful with attached volumes, which
[ADR-0011](../../internal/product/decisions/0011-no-consensus-cluster.md) and
[ADR-0013](../../internal/product/decisions/0013-fast-mode-wal-and-quorum.md)
declined. **A `quorum=1` local-disk WAL would reproduce their economics exactly,
and their durability posture with it.**

### 10.4 Scaling: an external autoscaler, plus a 60-second rebalancer

`kafka/autobalancer/` runs in the **controller**:

| Knob | Default |
|---|---|
| `anomaly.detect.interval.ms` | **60,000** |
| `metrics.delay.ms` | 60,000 |
| goals | `NetworkInUsageDistributionGoal`, `NetworkOutUsageDistributionGoal` |
| network-in detect threshold | 1 MiB/s, avg deviation 0.1 |
| metrics transport | a Kafka topic, 30 min retention |

So: an **external** autoscaler adds a broker; the AutoBalancer notices within
~1–2 minutes and **moves partitions onto it**. Moving a partition is
`CloseStreams` + `OpenStreams` with epoch fencing and **no data copy** — that is
the whole basis of "scale in seconds".

### 10.5 What transfers, and what we get for free

| Their mechanism | Ours |
|---|---|
| Rebalance partitions across brokers | ⚠️ **We have nothing to rebalance.** Writes are leaderless (ADR-0001), so the load balancer distributes at connection time and no partition is bound to a pod. The AutoBalancer's whole job does not exist for us |
| 60 s anomaly detection on network distribution | `fillRatio` at every flush — 0.12–2.4 s under load. ⚠️ **Not a fair comparison**: they detect *imbalance* (slow-moving), we detect *fill* (fast-moving). Different jobs |
| Fixed bundling thresholds | Adaptive interval + `fillRatio` control loop, because our durability trigger is the PUT itself |
| Metrics via a Kafka topic to a controller | Piggybacked on the forward-ack (ADR-0012) — no transport, no controller |
| Stateful brokers with EBS volumes | Stateless pods; the cost is that we cannot defer uploads in default mode |

**The honest summary:** AutoMQ scales *compute* elastically and keeps bundling
constant, because a durable WAL lets them upload lazily. We keep compute
elastic *and* adapt bundling, because our uploads are the durability event. Their
design is better if you accept stateful brokers; ours is better if you do not, and
neither is free.


---

**Sources:** cloned source (above) ·
[S3Stream overview](https://github.com/AutoMQ/automq/wiki/S3stream-shared-streaming-storage:-Overview) ·
[WAL storage docs](https://docs.automq.com/automq/architecture/s3stream-shared-streaming-storage/wal-storage) ·
[WarpStream vs AutoMQ](https://www.automq.com/blog/warpstream-vs-automq-object-storage-backed-kafka) ·
[S3 conditional writes GA, Aug 2024](https://aws.amazon.com/about-aws/whats-new/2024/08/amazon-s3-conditional-writes) ·
[S3 If-Match, Nov 2024](https://aws.amazon.com/about-aws/whats-new/2024/11/amazon-s3-functionality-conditional-writes/)
