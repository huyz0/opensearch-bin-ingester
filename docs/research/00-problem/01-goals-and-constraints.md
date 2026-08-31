# Goals, Constraints and Non-Goals

> **Read this first.** Every other document assumes these constraints. If a design idea
> violates one of these, it is out of scope regardless of how elegant it is.

**Status:** stable · **Confidence:** high (derived from the project brief) · **Last updated:** 2026-08-29

---

## 1. What we are building

Two artifacts, one protocol between them:

| # | Artifact | Runtime | Role |
|---|----------|---------|------|
| A | **Ingester** ("the ingester") | Standalone Java, Helidon SE 4, JDK 25, deployed as K8s pods across ≥2 AZs | Accepts writes over HTTP, bundles records from **many indices and many partitions** into a small number of object-store objects, assigns durable per-partition offsets, exposes a tail/notification API |
| B | **OpenSearch plugin** ("the plugin") | Inside the OpenSearch data node JVM (OpenSearch 3.8.0) | Implements `IngestionConsumerPlugin` so that pull-based ingestion reads from the object store instead of Kafka; downloads only the byte ranges for the partitions assigned to *this* node's shards |
| — | **Object store** ("bin store") | S3 / GCS / Azure Blob / local FS | The only durable state. Also the replication layer, and (via CAS) the coordination layer |

The ingester is **multi-tenant across indices**. It is not one deployment per index.

## 2. The prime directive: cost

> "The key goal of the project is to save cost where we can trade with ingestion latency."

Ranked objectives — when two conflict, the higher one wins:

1. **Minimise object-store API request count** (PUT/LIST are the expensive ones — see [cost model](02-cost-model.md)).
2. **Eliminate cross-AZ data transfer.** Bulk record bytes must never cross an AZ boundary. Only *metadata* may cross, and only when small.
3. **Avoid any non-object-store dependency.** No DynamoDB, no Postgres, no etcd, no ZooKeeper. Coordination must ride on object-store compare-and-swap primitives. (This is the main departure from WarpStream, which uses a hosted metadata store — see [warpstream.md](../10-prior-art/01-warpstream.md).)
4. **Bounded, predictable memory.** Strict streaming end to end; never materialise a whole request body or a whole object in heap.
5. **Then** minimise ingestion latency. Seconds are acceptable. Sub-second is a bonus, not a requirement.

### The latency escape hatch
Latency is traded away by *batching*, but the brief also asks for the best-effort improvement:

> "if we can have the client (plugin to poll or receive push notification) from the ingester for latest tailed data then it is better."

So: batching sets the floor on *durability* latency, but **discovery** latency must not add to it. A push/long-poll tail channel makes discovery ~free in both cost and time. See [discovery-and-tailing](../30-design-space/04-discovery-and-tailing.md). This matters more than it sounds: a naive polling design costs *more* in API calls when idle than the write path costs under load.

## 3. Hard constraints

| ID | Constraint | Source | Consequence |
|----|-----------|--------|-------------|
| C1 | Pluggable object store: S3, GCS, Azure Blob, local FS for tests | brief | Narrow SPI; no S3-only semantics on the hot path. CAS is expressed three different ways per provider — see [metadata-and-cas](../30-design-space/03-metadata-and-cas.md) §2 |
| C2 | Service nodes are **stateless**; any node may accept any write | brief | No sticky routing from producers. Ordering cannot come from write placement. See [metadata-and-cas](../30-design-space/03-metadata-and-cas.md) §3 |
| C3 | Nodes may gossip **transient state and metadata** to each other | brief | A sequencer/owner role is allowed, as long as only metadata crosses the wire |
| C4 | One object carries data for **many partitions**, and a header declares which | brief | Object needs a stream directory; see [object-layout](../30-design-space/01-object-layout-and-format.md) |
| C5 | The **object key** encodes a Bloom filter of the partitions inside | brief | Enables service-free discovery via LIST; see [partition-bloom-in-key](../30-design-space/02-partition-bloom-in-key.md). Note the analysis there: an exact bitmap beats a Bloom filter in one common case |
| C6 | Upload, download and list must be **API-call efficient** | brief | Every design must be scored in *requests per MB*, not just MB/s |
| C7 | Java + Helidon SE + virtual threads | brief | JDK 25 LTS. See [java-runtime](../40-implementation/01-java-runtime-helidon-vthreads.md) |
| C8 | Strict streaming request handling, no full-body buffering | brief | Framing protocol must be length-prefixed or line-delimited and consumed incrementally |
| C9 | Microbenchmark every hot path | brief | JMH harness is a first-class deliverable, including a *cost* benchmark. See [benchmarking-plan](../40-implementation/03-benchmarking-plan.md) |

## 4. Correctness requirements inherited from OpenSearch

These are not negotiable — they come from the `IngestionShardConsumer` contract (OpenSearch 3.8.0):

- **Monotonic, comparable, serialisable pointer per partition.** The pointer is indexed into Lucene as `_offset` and must support a "greater than" range query. A single `long` is the right choice. See [pull-based-ingestion-spi](../20-opensearch/01-pull-based-ingestion-spi.md).
- **Stable offsets.** Once a record is assigned offset *N* and made visible, replay must produce the same record at *N*. Offsets may not be reassigned after a sequencer failover. This forces "durable before visible" ordering.
- **At-least-once delivery.** OpenSearch deduplicates using `_id` + external versioning; we must not attempt exactly-once.
- **Resumable from an arbitrary pointer**, plus `earliestPointer()`, `latestPointer()`, `pointerFromTimestampMillis()`, and a lag metric.

## 5. Non-goals

- **Not a Kafka protocol implementation.** No consumer groups, no rebalancing, no Kafka wire compatibility. The only consumer is our plugin. This is a large simplification versus WarpStream/AutoMQ and should be exploited, not accidentally re-implemented.
- **Not general-purpose streaming.** Single logical consumer per partition (the shard that owns it). No fan-out to N independent consumer groups. This removes the hardest part of the read-side cost problem.
- **No exactly-once semantics.**
- **No transactions across partitions.**
- **No sub-100ms end-to-end target.** Anything in the 0.5s–10s band is a tunable, not a failure.
- **Not a replacement for OpenSearch remote store / segment replication.** We feed the ingestion path only.

## 6. Success criteria (make these measurable)

Concrete, falsifiable targets to design against and later assert in the benchmark harness:

| Metric | Target | Why |
|--------|--------|-----|
| Object-store **write** requests per MiB ingested | < 0.3 req/MiB at 30 MiB/s/AZ | See [cost model](02-cost-model.md) §5 worked example |
| Object-store **read** requests per MiB consumed | < 0.5 req/MiB per AZ | Bundling + range reads + node-level cache |
| Idle cost (no traffic) | **0** object-store requests/s from consumers | Long-poll tail channel, not polling. This is the single most commonly botched property |
| Cross-AZ bytes | < 0.1% of ingested bytes | Metadata only |
| Heap per ingester node | Bounded, independent of request size | Strict streaming + pooled buffers |
| End-to-end p99 (ack → searchable) | < 3× the configured batch window | No hidden discovery delay |

## 7. Open questions parked here

Tracked in full in [50-open-questions.md](../50-open-questions.md). The two that most affect architecture:

1. **Where does the partition assignment come from?** OpenSearch 3.8.0 currently maps shard *N* ⟷ source partition *N* (`SIMPLE`); `MODULO` exists in `SourcePartitionAssignment` but is **not yet wired up**. Does the ingester need to know the index's shard count, or does the producer supply the partition?
2. **Who owns retention/compaction?** The ingester (background lease-holder) or an out-of-band job?

---

**Next:** [02-cost-model.md](02-cost-model.md) — the arithmetic that justifies all of the above.
