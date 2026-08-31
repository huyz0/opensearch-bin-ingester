# Prior art: side-by-side comparison

**Status:** stable · **Confidence:** high · **Last updated:** 2026-08-29

**Read this if:** you want the one-page orientation before diving into a specific system, or you
are justifying a divergence from prior art in a design review.

---

## 1. The matrix

| Dimension | WarpStream | AutoMQ | KIP-1150 | Quickwit | **This project** |
|---|---|---|---|---|---|
| Consumer protocol | Kafka | Kafka | Kafka | HTTP search | **OpenSearch pull ingestion SPI** |
| Write bundling | many partitions/object | many streams/SSO | many partitions/SLSO | per-index splits | **many indices + partitions/object** |
| Flush trigger | 250 ms / 4–8 MiB | 250 ms / 8 MiB | broker-tuned | time-based | **250 ms / 8 MiB (adopt)** |
| Local disk | none | WAL (EBS/S3/NFS) | none | local WAL shards | **none** |
| Ordering authority | hosted metadata store (DynamoDB/Spanner) | KRaft controller | Batch Coordinator | PostgreSQL metastore | **object-store CAS + leased sequencer** |
| External dependency | yes (vendor cloud) | yes (KRaft) | yes (Kafka cluster) | yes (Postgres at scale) | **none — object store only** |
| Read cache | per-AZ distributed mmap, 4 MiB chunks | block cache + readahead | TBD | OS page cache + split footer cache | **node-local block cache (v1), per-AZ (v2)** |
| Consumers per partition | many (consumer groups) | many | many | n/a | **exactly one (the shard)** |
| Compaction | required | required | KIP-1165 | merge pipeline | **optional / deferred** |
| Retention | days | days | days | months | **hours** |
| Open source | no | **yes (Java)** | yes | yes (Rust) | — |

## 2. Where we are genuinely simpler

Four constraints we get for free that every system above had to solve the hard way. Exploit them;
do not accidentally rebuild what we don't need.

1. **Exactly one consumer per partition.** OpenSearch shard assignment already decides who reads
   what. No consumer groups, no rebalancing, no offset-commit protocol, no fan-out read
   amplification. This is why our read path can be a node-local cache instead of a distributed one.
2. ~~**The consumer is a JVM we control, inside the data node.** It can GET the object store
   directly.~~ ⚠️ **Corrected 2026-08-30.** True at ~9 data nodes; **false at the ~300-node target
   scale**, where direct reads cost either $3,732/month and 300× bandwidth amplification, or an
   absurd request count. We converge on WarpStream's agent-serves-fetches model for the same reason
   they chose it — see [discovery-and-tailing §2a](../30-design-space/04-discovery-and-tailing.md).
   The advantage we *do* keep: our serving tier is the same process that buffered the write, so the
   writing AZ needs **no GET at all**.
3. **Seconds of latency are acceptable.** Removes the entire WAL layer that AutoMQ needs.
4. **Data is transient.** The plugin indexes it within seconds; retention exists only to survive
   consumer downtime. Hours, not days — which shrinks storage cost and makes compaction optional.

## 3. Where we are genuinely harder

1. **No external metadata store.** WarpStream has DynamoDB; AutoMQ has KRaft; Quickwit ends up at
   Postgres. We have S3 conditional PUTs and a lease. This is the highest-risk part of the design
   and the one that needs the most careful review — see
   [metadata-and-cas](../30-design-space/03-metadata-and-cas.md).
2. **Multi-tenancy across indices in one deployment.** The commit rate must not scale with the
   number of indices (cost rule R6). Nobody above solves exactly this; they scale per-cluster.
3. **The consumer's poll loop is not ours.** `DefaultStreamPoller` calls `readNext` roughly every
   100 ms *per shard* and we cannot change it from a plugin. Turning that into zero idle requests
   is a hard requirement with a narrow solution — see
   [poller-semantics](../20-opensearch/02-poller-semantics-and-cost.md).

## 4. Reading order for a newcomer

1. [warpstream.md](01-warpstream.md) — the architecture we are closest to.
2. [automq.md](02-automq.md) — the code we can actually read (in `.tmp/automq`).
3. [diskless-kafka-and-cas-systems.md](03-diskless-kafka-and-cas-systems.md) — the coordination problem.
