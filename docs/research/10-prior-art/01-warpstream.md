# Prior art: WarpStream

**Status:** stable · **Confidence:** high (vendor docs + engineering blog; closed source, so no code grounding) · **Last updated:** 2026-08-29

**Read this if:** you are designing the write batching, the read-side cache, or the AZ topology.
**One-line takeaway:** WarpStream is the closest architectural relative to this project; copy its
write batching and per-AZ read cache, but replace its hosted metadata store with object-store CAS.

---

## 1. Shape

Kafka-protocol-compatible streaming built directly on object storage. Three separations:

- **Storage ↔ compute:** "Agents" are stateless, diskless binaries in the customer VPC. No agent
  owns any partition, so scaling out requires no rebalancing.
- **Data ↔ metadata:** record bytes live in the customer's S3 bucket; ordering metadata lives in
  WarpStream's hosted **Cloud Metadata Store** (DynamoDB / Spanner).
- **Data plane ↔ control plane:** agents serve produce/fetch; the cloud control plane handles
  ordering, compaction and retention.

## 2. Write path — the part to copy

- Agents buffer `Produce()` from **many producers and many topic-partitions** and flush **one
  object per agent** every **250 ms or 4 MiB**, whichever comes first (the blog says 4 MiB, the
  docs say 8 MiB — treat as "a few MiB, tunable").
- Records inside the object are **sorted by topic, then partition**.
- After the object is durable, the agent commits the file's metadata to the metadata store.
  **Nothing is acknowledged until both the object PUT and the metadata commit succeed.**
- **Ordering is assigned at commit time, not at flush time.** This is the crucial trick: it lets
  every agent write to S3 in parallel with no coordination, while a single logical sequencer
  decides the order afterwards.
- Result: **~4 files/second per agent regardless of partition count.** A 1-partition topic and a
  1024-partition topic cost the same in PUTs.
- p99 write latency ≈ **400 ms**, with no local disks anywhere.

**What we take:** all of it, except the hosted metadata store. See
[metadata-and-cas](../30-design-space/03-metadata-and-cas.md) for the CAS-on-object-store
replacement.

## 3. Read path — the "distributed mmap"

The read-side problem: with N partitions × M consumers, naive fetches explode GET counts
(their example: 1024 partitions, one consumer ⇒ 4,096 GET/s ⇒ **>$4,200/month**).

Their solution, a per-AZ distributed file cache:

- A **consistent hashing ring over agents within one AZ** maps `fileID → owning agent`.
- The cache pages in **fixed 4 MiB aligned chunks**, regardless of the size of the incoming
  fetch. The cache is deliberately "agnostic to the size of the IO requests it receives".
- A fetch for partition *P* is turned into a sub-fetch routed to the agent that owns the chunk.
  Concurrent fetches for different partitions that land in the same 4 MiB chunk are **deduplicated
  into a single GET**.
- **Scan sharing:** live consumers all read near the head of the log, and the head is exactly
  where all partitions are interleaved in the same recent objects — so one 4 MiB GET serves many
  partitions, approaching zero waste.
- Consequence: "a topic with 1024 partitions can be processed with the same number of GET requests
  as a topic with 1 partition."
- Each chunk is downloaded **once per AZ** (3× for 3 AZs). They accept that 3× GET multiplier
  explicitly because it is >10× cheaper than paying inter-zone bandwidth.

**What we take:** the chunked, request-deduplicating cache and the "once per AZ" principle. What
we *simplify*: we have exactly **one consumer per partition** (the shard that owns it), no
consumer groups, so scan sharing is between *shards on the same node* rather than between
arbitrary consumers. That makes a **node-local** cache sufficient for v1, with a per-AZ
distributed cache as a v2 optimisation. See [az-topology](../30-design-space/05-az-topology-and-data-flow.md).

## 4. Lagging consumers and compaction

A consumer reading historical data gets no scan-sharing benefit and can double GET counts.
Fix: agents compact many small ingest-time objects into larger ones. Compaction costs
**one extra GET per input file**, is streaming (low memory), and massively reduces read
amplification and improves compression ratios.

**Applicability to us:** weaker. Our single consumer normally sits at the live edge; catch-up
reads only happen after a node restart or a long outage. Treat compaction as **optional and
deferred** — see [compaction-and-retention](../30-design-space/06-compaction-and-retention.md).

## 5. Cross-AZ elimination

- Agents write to S3 and let S3 do the cross-AZ replication: "leverages the free networking
  between EC2 and S3, which just so happens to durably replicate your data along the way."
- Clients are routed to same-AZ agents via a zone hint (`warpstream_az=...` encoded in the Kafka
  client ID) rather than Kafka's rack-awareness — they explicitly warn that standard Kafka
  rack-aware consumer strategy causes excessive rebalances with WarpStream.

**What we take:** the principle, and the warning that AZ routing belongs in *our* discovery
layer, not bolted onto a client feature designed for something else. For us this means: the
plugin resolves a **same-AZ service endpoint** for the metadata/tail channel, and fetches bulk
bytes from the object store directly (free, in-region). See
[az-topology](../30-design-space/05-az-topology-and-data-flow.md).

## 6. What we deliberately do NOT copy

| WarpStream feature | Why not |
|---|---|
| Hosted metadata store (DynamoDB/Spanner) | Constraint C3: object store must be the only dependency. Replaced by CAS-based commit chain |
| Kafka protocol compatibility | Non-goal; huge complexity for zero benefit here |
| Consumer groups / rebalancing | Non-goal; OpenSearch shard assignment already decides ownership |
| Agent-side fetch serving (client → agent → S3) | Our consumer *is* a JVM inside OpenSearch; it can GET S3 directly and skip a hop, saving bandwidth and an ingester tier |

That last row is a genuine advantage we have over WarpStream: **the plugin reads the object store
directly**, so bulk bytes never traverse our service at all.

---

**Sources:**
[Architecture](https://docs.warpstream.com/warpstream/overview/architecture) ·
[Write path](https://docs.warpstream.com/warpstream/overview/architecture/write-path) ·
[Read path](https://docs.warpstream.com/warpstream/overview/architecture/read-path) ·
[Minimizing S3 API Costs with Distributed mmap](https://www.warpstream.com/blog/minimizing-s3-api-costs-with-distributed-mmap) ·
[Configure clients to eliminate AZ networking costs](https://docs.warpstream.com/warpstream/kafka/configure-kafka-client/configure-clients-to-eliminate-az-networking-costs) ·
[S3 Express One Zone benchmark & TCO](https://www.warpstream.com/blog/warpstream-s3-express-one-zone-benchmark-and-total-cost-of-ownership)
