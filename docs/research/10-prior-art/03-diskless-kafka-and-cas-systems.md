# Prior art: KIP-1150 Diskless Kafka, SlateDB, Quickwit

**Status:** stable · **Confidence:** medium-high (secondary sources for KIP-1150; primary RFC for SlateDB) · **Last updated:** 2026-08-29

**Read this if:** you are designing the sequencer / commit protocol, or the object-store-as-database
coordination layer.
**One-line takeaway:** KIP-1150 validates "leaderless writes + a separate sequencer" as the correct
decomposition; SlateDB shows how to do the sequencer's *fencing* with pure object-store CAS;
Quickwit is the cautionary tale about file-based metastores under concurrent writers.

---

## 1. KIP-1150 — Diskless Topics in Apache Kafka

The Apache Kafka project is standardising exactly this architecture, which is strong validation
that the decomposition is right (and a warning that this is where the hard problems live —
they split it into KIP-1163 Core, **KIP-1164 Batch Coordinator**, KIP-1165 Object Compaction).

Design in one paragraph: brokers accumulate record batches and upload **Shared Log Segment
Objects (SLSOs)** containing batches for many partitions from many brokers. Writes are
**leaderless** — any broker accepts any partition. A separate **Batch Coordinator** receives the
"batch coordinates" (which byte ranges of which object hold which partition's records), assigns
offsets, and thereby defines the global per-partition order.

Their framing: *"Diskless is to No-Disks as Serverless is to No-Servers"* — the disks don't
disappear, they stop being the durability layer for active data.

**What this tells us:**
- The **write placement / offset assignment split is the canonical design**, independently
  arrived at by WarpStream, AutoMQ (via KRaft), and now Apache Kafka. Our design must have this
  split too. See [metadata-and-cas](../30-design-space/03-metadata-and-cas.md) §3.
- The Batch Coordinator is the hardest component and the one everybody implements differently.
  It is the natural place for our CAS-on-object-store innovation.
- Compaction is worth a KIP of its own — evidence that it is *separable* and can be deferred
  (which our single-consumer model lets us do).

## 2. SlateDB — how to do CAS coordination with only an object store

An embedded LSM database whose *entire* durable state is object storage. Directly relevant
because it solves our problem: safe coordination with no external lock service.

Mechanics, from [RFC-0001 Manifest](https://slatedb.io/rfcs/0001-manifest/):

- All state lives in a **manifest** object; **every** update is a **compare-and-swap**.
- **Writer fencing by epoch:** on open, a writer reads the epoch from the manifest, increments it,
  and immediately writes it back with CAS. If two processes open for write concurrently, only one
  CAS succeeds. The epoch is **a counter, not a timestamp** — no clock assumptions.
- A zombie writer that wakes up after a pause finds its epoch stale and is fenced out. CAS also
  guarantees each SST is written exactly once.
- Single writer, many readers.

**What we take:**
- **Epoch fencing via CAS**, verbatim, for the sequencer role. See
  [metadata-and-cas](../30-design-space/03-metadata-and-cas.md) §4.
- The epoch belongs **in the object key path**, so that a fenced-out leader's writes are not merely
  rejected — they land in a path readers ignore, which makes the protocol safe even if the fenced
  writer's PUT was already in flight.
- The insight that a manifest *pointer* is the only mutable object; everything else is write-once.

**What we must add:** SlateDB's CAS rate is low (a database open, occasional manifest updates).
Ours is 4-12 commits/second sustained. See §4 below and the contention analysis in
[metadata-and-cas](../30-design-space/03-metadata-and-cas.md) §5.

## 3. Quickwit — the cautionary tale, plus a useful cost datapoint

A distributed search engine on object storage (the closest system in *purpose* to ours — it is
"OpenSearch-shaped" but Rust). Indexers chop the document stream into **splits** (mini indexes),
upload them to S3, and a **merge pipeline** consolidates them. Ingest V2 distributes documents
into WAL **shards** that a control plane assigns across indexers, with progress tracked in a
metastore shards table.

Two lessons:

1. **The warning, verbatim from their docs:** a metastore backed by a JSON file on object storage
   *"does not handle concurrent writers well and you should move to PostgreSQL before scaling
   indexing out."* This is precisely the failure mode our design must avoid — and the reason our
   commit protocol must use **write-once (`If-None-Match: *`) appends** rather than
   read-modify-write CAS on a single shared manifest object. See
   [metadata-and-cas](../30-design-space/03-metadata-and-cas.md) §5.
2. **A useful cost datapoint:** they measure that polling a metastore every 30 seconds costs
   ~$0.04/month per index. Sounds trivial — until you multiply by 100 indices × 9 nodes and
   shorten the interval to 100 ms, which is what a naive OpenSearch poller would do
   (see [poller-semantics](../20-opensearch/02-poller-semantics-and-cost.md)). Their published
   figures: 27 MB/s indexing throughput, ~$2 per ingested TB, $8.4/TB/month storage.

## 4. Synthesis: the shape of our commit protocol

Combining the three:

| From | Idea | Our use |
|------|------|---------|
| KIP-1150 | Leaderless data write + separate coordinator assigns offsets | Any pod writes any partition's bytes; a group sequencer assigns offsets |
| SlateDB | Epoch fencing via CAS; epoch in the path; write-once everything else | Sequencer leases, fenced by epoch embedded in the commit-log prefix |
| Quickwit | Do **not** read-modify-write a shared manifest under concurrency | Commit log = monotonically numbered, write-once delta objects (`If-None-Match: *`), periodically checkpointed |
| WarpStream | Ordering assigned at commit, not at flush | Same |
| AutoMQ | Batch everything; 250 ms / 8 MiB | Same, and batch the *commits* too so their rate is independent of index count |

---

**Sources:**
[KIP-1150](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1150:+Diskless+Topics) ·
[Aiven: Hitchhiker's guide to Diskless Kafka](https://aiven.io/blog/guide-diskless-apache-kafka-kip-1150) ·
[Jack Vanlightly: A Fork in the Road](https://jack-vanlightly.com/blog/2025/10/22/a-fork-in-the-road-deciding-kafkas-diskless-future) ·
[SlateDB RFC-0001 Manifest](https://slatedb.io/rfcs/0001-manifest/) ·
[Quickwit 101](https://quickwit.io/blog/quickwit-101) ·
[Quickwit metastore config](https://quickwit.io/docs/configuration/metastore-config) ·
[Quickwit ingest-v2 internals](https://github.com/quickwit-oss/quickwit/blob/main/docs/internals/ingest-v2.md)
