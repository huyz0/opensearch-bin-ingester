# OpenSearch pull-based ingestion: the SPI we must implement

**Status:** stable · **Confidence:** high (read directly from OpenSearch 3.8.0 source) · **Last updated:** 2026-08-29

**Read this if:** you are writing the plugin, or designing anything that the plugin must expose
(pointers, offsets, partition mapping).
**One-line takeaway:** the contract is small — five interfaces — and it constrains our offset design
in exactly one important way: **the pointer must be a single monotonically increasing, range-queryable
value.** Use a `long`.

**Ground truth:** `/home/tuong/work/OpenSearch` @ 3.8.0 (`buildSrc/version.properties: opensearch = 3.8.0`).
All paths below are relative to that checkout.

---

## 1. The five types

```
server/src/main/java/org/opensearch/
  plugins/IngestionConsumerPlugin.java     <- what our Plugin class implements
  index/IngestionConsumerFactory.java      <- creates a consumer per shard
  index/IngestionShardConsumer.java        <- the poll interface
  index/IngestionShardPointer.java         <- the offset type
  index/Message.java                       <- the record payload
```

### `IngestionConsumerPlugin`
```java
public interface IngestionConsumerPlugin {
    Map<String, IngestionConsumerFactory> getIngestionConsumerFactories();
    String getType();
}
```
Factories are collected in `node/Node.java:1019`. Our plugin also extends `Plugin`, so it can
implement `getSettings()` / `createComponents()` — **this is where node-level shared state
(the tail subscription and the block cache) must live**, because the factory is called once per
shard. See [poller-semantics](02-poller-semantics-and-cost.md) §4.

### `IngestionConsumerFactory<T extends IngestionShardConsumer, P extends IngestionShardPointer>`
```java
P parsePointerFromString(String pointer);
default T createShardConsumer(String clientId, int shardId, IndexMetadata indexMetadata);
```
(The older `initialize(IngestionSource)` + `createShardConsumer(clientId, shardId)` two-step is
deprecated `forRemoval` — implement the `IndexMetadata` overload only.)

`indexMetadata.getIngestionSource().params()` is the `Map<String,Object>` from
`index.ingestion_source.param.*`. **`indexMetadata` also gives us the index name, UUID and shard
count** — which we need to derive the stream identity. See §4.

### `IngestionShardPointer` — the constrained one
```java
String OFFSET_FIELD = "_offset";
byte[] serialize();
String asString();
Field  asPointField(String fieldName);
Query  newRangeQueryGreaterThan(String fieldName);
int    compareTo(IngestionShardPointer o);   // extends Comparable
```
`MessageProcessorRunnable.java:268-270` indexes it into every document:
```java
document.add(pointer.asPointField(IngestionShardPointer.OFFSET_FIELD));
document.add(new StoredField(IngestionShardPointer.OFFSET_FIELD, pointer.asString()));
```

> **Design consequence (important).** The pointer is a Lucene point field with a range query. The
> reference implementation `FileOffset` uses `LongPoint`. This effectively rules out composite
> pointers like `(objectId, byteOffset)`: they are awkward to range-query and they are not stable
> across compaction (the object holding a record changes; the record's position in the log must not).
> **Our pointer is a single `long`: the sequencer-assigned logical offset within the partition.**
> The mapping `offset -> (object, byteRange)` lives in the commit log, not in the pointer.
> See [object-layout](../30-design-space/01-object-layout-and-format.md) §2.

### `IngestionShardConsumer<T extends IngestionShardPointer, M extends Message>`
```java
List<ReadResult<T,M>> readNext(T pointer, boolean includeStart, long maxMessages, int timeoutMillis)
        throws TimeoutException;
List<ReadResult<T,M>> readNext(long maxMessages, int timeoutMillis) throws TimeoutException;
IngestionShardPointer earliestPointer();
IngestionShardPointer latestPointer();
IngestionShardPointer pointerFromTimestampMillis(long timestampMillis);
IngestionShardPointer pointerFromOffset(String offset);
int  getShardId();
long getPointerBasedLag(IngestionShardPointer expectedStartPointer);
```

Notes that matter:
- **`timeoutMillis` is ours to use.** Nothing forbids blocking inside `readNext` for up to that
  long. This is the hook that makes zero-cost idling possible — see
  [poller-semantics](02-poller-semantics-and-cost.md) §3. **This is the single most important
  observation in the OpenSearch section.**
- `pointerFromTimestampMillis` must map wall-clock to offset. The reference `FilePartitionConsumer`
  cheats and returns `earliestPointer()`. We can do better cheaply because our commit log records
  a first-timestamp per (stream, object) — a binary search over checkpoints, no data reads.
- `getPointerBasedLag` is called periodically (`index.ingestion_source.pointer_based_lag_update_interval`)
  even while paused. **It must not cost an object-store request** — serve it from the cached tail
  offset that the subscription already pushes to us.

## 2. Message format the engine expects

`indices/pollingingest/mappers/` offers three mappers, selected by
`index.ingestion_source.mapper_type` with settings under `index.ingestion_source.mapper_settings.*`:

| Mapper | Payload shape |
|---|---|
| `DefaultIngestionMessageMapper` | JSON envelope: `{"_id":…, "_op_type":"index\|create\|delete", "_version":…, "_source":{…}}` |
| `RawPayloadIngestionMessageMapper` | the payload **is** the `_source`; `_id` generated; op type always `index` |
| `FieldMappingIngestionMessageMapper` | plain document; `id_field` / `version_field` / `op_type_field` name the source fields to lift out (with `delete_value` / `create_value`) |

⚠️ **`MapperType` is a closed enum** (`DEFAULT`, `RAW_PAYLOAD`, `FIELD_MAPPING`) —
a plugin cannot register its own. Whatever we store must already match one of the
three shapes when handed to `Message.getPayload()`. We use `DEFAULT` and assemble
its JSON envelope **in the consumer**, from fields carried natively in the segment
([ADR-0020](../../internal/product/decisions/0020-record-envelope-and-mapper.md)).
That costs a serialise-then-parse round trip per record; a plugin-extensible mapper
upstream would remove it.

**Consequence for the ingester:** the record payload is an **opaque byte array** as far as we are
concerned — `Message<byte[]>`. The ingester should *not* parse user documents (see
[streaming-io](../40-implementation/02-streaming-io-and-memory.md) §3 for why parsing is the most
expensive thing we could do). Routing metadata (index, partition) should arrive **out of band** in
the wire framing, not be extracted from the document body.

## 3. Index configuration surface

From `cluster/metadata/IndexMetadata.java`:

```
index.ingestion_source.type                              <- our plugin's getType(), e.g. "BINSTORE"
index.ingestion_source.param.*                           <- free-form Map<String,Object> -> our config
index.ingestion_source.pointer.init.reset                <- earliest|latest|reset_by_offset|reset_by_timestamp|none
index.ingestion_source.pointer.init.reset.value
index.ingestion_source.poll.max_batch_size               <- maxPollSize, default 1000
index.ingestion_source.poll.timeout                      <- pollTimeout ms, default 1000
index.ingestion_source.num_processor_threads
index.ingestion_source.internal_queue_size
index.ingestion_source.error_strategy                    <- DROP | BLOCK
index.ingestion_source.all_active
index.ingestion_source.pointer_based_lag_update_interval
index.ingestion_source.mapper_type
index.ingestion_source.mapper_settings.*
index.ingestion_source.source_partition_strategy         <- SIMPLE | MODULO
index.ingestion_source.warmup.lag_threshold
index.ingestion_source.warmup.timeout
```

Management APIs exist for pause/resume/get-state/update-state under
`action/admin/indices/streamingingestion/`.

**Credentials must not go in `param.*`** (index settings are cluster state, readable by anyone with
cluster:monitor). Put bucket credentials in node settings / the OpenSearch keystore via
`Plugin.getSettings()`, and keep only bucket/prefix/endpoint in `param.*`. See
[plugin-packaging](03-plugin-packaging.md) §3.

## 4. Partition ↔ shard mapping — a live constraint

`indices/pollingingest/SourcePartitionAssignment.java` defines two strategies:

- `SIMPLE`: shard *N* consumes source partition *N*. Requires `numSourcePartitions >= numShards`
  (a shard with `shardId >= numSourcePartitions` fails to initialise).
- `MODULO`: shard *N* consumes every partition *p* where `p % numShards == shardId`.

**But: `assignSourcePartitions` has no callers in 3.8.0.** `grep -rn assignSourcePartitions` returns
only its own definition, and `MetadataCreateIndexService.java:2016` carries a TODO about surfacing
warnings for the mismatch cases. In practice today: **one consumer per shard, shard id == partition
id.**

Consequences for our design:
1. **Number of partitions per index is fixed at index creation and equals the shard count.** The
   service must learn it (config in `param.*`, or the producer states it). Repartitioning is not
   supported by OpenSearch, so it isn't supported by us either — record this as a hard limitation.
2. Design our stream identity as `(indexUUID, partitionId)` where `partitionId ∈ [0, numShards)`,
   and keep the multi-partition-per-shard door open for when `MODULO` gets wired up — i.e. the
   consumer should be able to merge several partition streams behind one pointer space. **Do not**
   assume 1:1 in the internal API even though 1:1 is what runs today.
3. Producers must be able to compute the partition. Either they send an explicit partition, or they
   send a routing key and the ingester applies `Math.floorMod(murmur3(routingKey), numPartitions)` —
   which must match OpenSearch's own routing if documents are to land on the shard that owns them.
   ✅ **Answered — [ADR-0006](../../internal/product/decisions/0006-partition-assignment-and-the-routing-invariant.md).**
   The partition function is **ours to choose**: OpenSearch's own ingestion path
   already breaks the routing invariant, because `RawPayloadIngestionMessageMapper`
   sets `_id = shardId + "-" + pointer` and `MessageProcessorRunnable` performs no
   routing at all. Search fans out and finds everything; by-id `GET`/`update`/
   `delete` do not, with or without us. Three modes: `explicit` (default),
   `routing_key`, `os_routing`.

   ⚠️ **The natural alternative — write routing-agnostic and filter at read time —
   is worse in both available forms**, and the numbers are in
   [ADR-0015](../../internal/product/decisions/0015-routing-registration-and-aliases.md):
   plugin-side filtering costs `S`× serving bandwidth (2,000× for a mega index),
   and bucket-keyed runs inflate the directory and commit log 22–85× while
   removing no dependency, since the bucket count *is* `routingNumShards` and is
   fixed at index creation. Resolution stays at write time; the unknown-count
   window is covered by **buffer-and-reroute**, which is legal precisely because a
   buffered record has no offset yet (ADR-0001).

## 4b. High-cardinality routing (millions of tenants): what it does and does not touch

The common multi-tenant scheme is `routing = tenantId + "_" + (n mod k)` — co-locate
a tenant's data on a few shards, spreading a large tenant over `k` of them. With
millions of tenants that is millions of distinct routing values.

### It does not touch the bundling keys

| | |
|---|---|
| Routing values | ~5,000,000 |
| **Streams `(indexUUID, partition)`** | **~100,000 — unchanged** |
| Routing cardinality in the stream key? | **No** |

The routing value is consumed **once, at admission**, to compute the partition, and
then discarded. It never reaches the segment directory, the commit log, the key
filter or the trickle policy — all of which are keyed by partition, which is
bounded by shard count. **Tenant cardinality is invisible to the bundling
strategy.**

Wire overhead is the only cost: ~20 bytes per record, which is 10% on a 200 B
document and 0.2% on a 10 KB one. It is not stored, so there is no storage cost.

A tenant's records within one partition land in the **same run**, so they are
contiguous in the segment — which is what a consumer wants anyway.

### ⚠️ `_routing` is never set on the document, and cannot be

`MessageProcessorRunnable` reads only `_id`, `_op_type`, `_source` and `_version`
from the mapper payload. There is no `_routing` key. And `IngestPipelineExecutor`
**actively forbids** it:

```java
// IngestPipelineExecutor.java:155-157
if (Objects.equals(originalRouting, indexRequest.routing()) == false) {
    "Ingest pipeline attempted to change _routing. _routing mutations are not allowed in pull-based ingestion."
```

Consequences for a routing-based tenancy scheme:

- **Routed search works** — `?routing=tenant_123_0` hashes to shard *N*, and the
  document is on shard *N* because we placed it there. ✅
- **It works only because our placement reproduces OpenSearch's hash.** The
  document carries no `_routing` of its own, so nothing corrects a mistake.
- ⚠️ **A placement error is a silent wrong answer, not an error.** Put a document
  on the wrong shard and routed queries simply miss it — no exception, no log, no
  gate. For `os_routing` indices, reproducing `OperationRouting.generateShardId`
  exactly is therefore a **correctness requirement**, including
  `routingNumShards` / `routingFactor`, and it must be tested against OpenSearch's
  own implementation rather than reimplemented from memory.
- Anything that reads `_routing` back — reindex preserving routing, update-by-query
  with routing — will not find it. **Store the tenant id as an ordinary `_source`
  field** if you need it after the fact.

### ⚠️ `index.routing_partition_size` is incompatible with routing-only placement

If the index uses OpenSearch's built-in tenant partitioning, the shard depends on
**both** the routing value and the document `_id`:

```java
partitionOffset = floorMod(murmur3(_id), routingPartitionSize);   // OperationRouting:587
```

The ingester cannot compute the partition from the routing value alone. Either the
producer must also send `_id`, or the index must not use
`routing_partition_size`. **The manual `tenantId + (n mod k)` scheme avoids this
entirely and is the recommended form** — it puts the spread in the routing value,
where the ingester can see it.

Validate at registration: reject `os_routing` when `routing_partition_size > 1`
unless the producer contract also carries `_id`.

### The real risk is skew, not cardinality

Millions of tenants hash uniformly, but **traffic** does not. One dominant tenant
lands in one partition:

| Top tenant's share of the index | Its partition receives (16 partitions) |
|---|---|
| 5% | 10.9% (1.8× even) |
| 20% | 25.0% (4.0× even) |
| 40% | 43.8% (**7.0×** even) |

Spreading that tenant over `k` routing values is the mitigation, and it works:

| `k` | Hot partition share (40% tenant) |
|---|---|
| 1 | 43.8% (7.0×) |
| 4 | 13.8% (2.2×) |
| **16** | **6.2% (1.0×)** |

⚠️ **This means ADR-0010's isolation caps are at the wrong granularity for this
workload.** They are per *index*; with millions of tenants inside one index, the
unit that goes hot is a **stream** `(index, partition)`. Add a **per-stream share
cap on segment bytes** alongside the per-index one, or one hot tenant dominates
every segment and inflates read amplification for every other tenant in the index.


## 5. Restart / recovery semantics

`index/engine/IngestionEngine.java` persists the poller position into the Lucene commit data under
`StreamPoller.BATCH_START` (`"batch_start"`), and on open recovers the start pointer from commit
data (falling back to `pointer.init.reset`). Between the last commit and a crash, messages are
replayed — **at-least-once**, deduplicated by `_id` + external versioning.

**Consequence:** our offsets must be **stable across replay and across sequencer failover**. A
record that was visible at offset *N* must still be at offset *N* forever. This is why the commit
protocol must make a record visible only *after* its offset is durably recorded — see
[metadata-and-cas](../30-design-space/03-metadata-and-cas.md) §4.

## 6. Reference implementations to read

| Plugin | Path | Why read it |
|---|---|---|
| `ingestion-fs` | `plugins/ingestion-fs/` | The minimal end-to-end example (6 small classes). Our skeleton. |
| `ingestion-kafka` | `plugins/ingestion-kafka/` | How a real remote source handles lag, latest pointer, config, and `build.gradle` dependency declaration |
| `ingestion-kinesis` | `plugins/ingestion-kinesis/` | Sequence-number pointers — closest analogue to an opaque cloud offset |
| `repository-s3` | `plugins/repository-s3/` | How OpenSearch packages AWS SDK access, incl. `plugin-metadata/plugin-security.policy` |

---

**Next:** [02-poller-semantics-and-cost.md](02-poller-semantics-and-cost.md) — the poll loop, and
why a naive implementation costs $16k/month doing nothing.
