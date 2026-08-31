# Research index — object-store ingestion for OpenSearch

**Project:** a lightweight, serverless-style ingester that bundles writes from many indices and
partitions into a small number of object-store objects, plus an OpenSearch plugin that consumes them
through the pull-based ingestion SPI. Replaces Kafka in the OpenSearch ingest path at roughly
**1/40th of the marginal cost**, trading seconds of latency for it.

**Audience:** the next AI session or engineer picking this up. Read tier by tier; do not read
everything.

**Last updated:** 2026-08-29 · **Corpus status:** research complete, no code written yet.
**Target scale:** 10,000 indices — ~10 mega indices at up to 2,000 shards, the rest at 6–15 shards
⇒ **~120,000 streams / shards over ~300 data nodes**. Several recommendations were revised on
2026-08-29 when this scale was confirmed; see the ⚠️ markers below.

---

## Start here: the five things that matter

1. **Bundling is the architecture, not an optimisation.** One object per pod per flush carrying many
   `(index, partition)` streams. Per-partition objects cost **$52/partition/month** at a 250 ms
   rotation — 1,000 partitions would cost more than the Kafka cluster we are replacing.
2. **A LIST costs the same as a PUT — 12.5× a GET.** Never LIST on a hot path.
3. **Idle cost is the hidden killer.** OpenSearch runs one poller per shard at ~10 Hz. A naive
   `readNext` that touches the object store costs **$16,600–$207,000/month doing nothing**. The fix
   is to *block* inside `readNext` on a node-level push subscription: **zero** requests when idle.
4. **Byte-range GETs are billed per request, not per byte, and same-region bytes are free.** So a
   "precise" reader that fetches exactly its partitions is **175× more expensive** than one that
   reads the whole object. Always coalesce.
5. **Write cost is indifferent to scale; read cost is not.** Going from 1,600 to 120,000 streams adds
   **zero** write requests — but read cost scales with **node count**: $112/month at 9 data nodes
   becomes **$3,732/month at 300**. Fix with a longer flush interval (free) plus a per-AZ shared
   cache. ⚠️ This reverses an earlier recommendation.
6. **The ingester serves reads; the plugin does not read the object store on the hot path.** At ~300
   data nodes, direct reads cost either **$3,732/month with 300× bandwidth amplification**
   (whole-object) or **$1.49M/month** (precise ranges); proxying through same-AZ ingester nodes costs
   **$25/month at 0.33 MiB/s per node**. Cache where fan-in is high. This also buys back the latency
   budget — read cost no longer depends on the flush interval, so keep 250 ms.
7. **The write buffer is the read cache.** AutoMQ never evicts a flushed block until memory pressure
   forces it, so tail reads are served from RAM and never touch S3. Applied to our topology:
   **0 GETs in the writing AZ, 2 GETs per segment total** — independent of data-node count. Small
   deltas can be **inlined in the push event**, so a trickle index costs zero fetches of any kind.
8. **Safety never depends on detecting failure.** The epoch lives in the object path and every commit
   is a write-once claim on a sequence number, so an undetected zombie leader is *wasteful, not
   incorrect*. That lets us fail over aggressively — and it is why a partitioned AZ is survivable.
9. **Ordering is assigned at commit, not at write.** Segments contain no absolute offsets. That is
   what lets any pod write any partition with zero coordination — and it is the same decomposition
   WarpStream, AutoMQ and Apache Kafka's KIP-1150 all arrived at independently.

---

## Progressive disclosure: which tier do you need?

| Tier | Read when | Files | Time |
|---|---|---|---|
| **0** | You just arrived | this README | 5 min |
| **1 — Problem** | Before proposing *any* design change | [`00-problem/`](00-problem/) | 20 min |
| **2 — Prior art** | You want to know if this is solved already | [`10-prior-art/`](10-prior-art/) | 30 min |
| **3 — OpenSearch** | You are touching the plugin or the consumer contract | [`20-opensearch/`](20-opensearch/) | 30 min |
| **4 — Design space** | You are implementing an ingester component | [`30-design-space/`](30-design-space/) | 60 min |
| **5 — Implementation** | You are writing Java | [`40-implementation/`](40-implementation/) | 30 min |
| **∞** | Always, before deciding anything contentious | [`50-open-questions.md`](50-open-questions.md) | 10 min |

**Minimum viable path:** README → [goals](00-problem/01-goals-and-constraints.md) →
[cost model](00-problem/02-cost-model.md) → the one design doc for your component.

---

## Task-based routing

| I need to… | Read |
|---|---|
| See the whole path once, concretely | [30-design-space/13](30-design-space/13-worked-example-multi-tenant.md) |
| Write a producer or consumer library | [30-design-space/14](30-design-space/14-producer-contract.md) |
| Understand why this project exists | [00-problem/02-cost-model.md](00-problem/02-cost-model.md) §2 |
| Justify or challenge a design decision | [00-problem/02-cost-model.md](00-problem/02-cost-model.md) §6 (rules R1–R9) |
| Write the OpenSearch plugin | [20-opensearch/01](20-opensearch/01-pull-based-ingestion-spi.md), [02](20-opensearch/02-poller-semantics-and-cost.md), [03](20-opensearch/03-plugin-packaging.md) |
| Design the on-disk object format | [30-design-space/01](30-design-space/01-object-layout-and-format.md) + [10-prior-art/02](10-prior-art/02-automq.md) §4 |
| Implement offset assignment / leader election | [30-design-space/03](30-design-space/03-metadata-and-cas.md) ← **highest-risk component** |
| Implement key generation / the membership filter | [30-design-space/02](30-design-space/02-partition-bloom-in-key.md) |
| Build the tail/push channel | [30-design-space/04](30-design-space/04-discovery-and-tailing.md) |
| Write Kubernetes manifests | [30-design-space/05](30-design-space/05-az-topology-and-data-flow.md) §3 + [08](30-design-space/08-failure-domains-and-resilience.md) §2 |
| Handle AZ / node failure, shutdown, chaos tests | [30-design-space/08](30-design-space/08-failure-domains-and-resilience.md) |
| Define the store SPI or add a backend | [30-design-space/07](30-design-space/07-pluggable-store-abstraction.md) |
| Decide what *not* to build in v1 | [30-design-space/06](30-design-space/06-compaction-and-retention.md) §1 |
| Implement retention / GC / consumer position | [30-design-space/09](30-design-space/09-consumer-position-and-watermarks.md) |
| Build the consumer library / plugin fetch path | [30-design-space/10](30-design-space/10-client-library-and-fetch-modes.md) |
| Set up the ingester skeleton (Helidon/JDK) | [40-implementation/01](40-implementation/01-java-runtime-helidon-vthreads.md) |
| Implement the ingest handler | [40-implementation/02](40-implementation/02-streaming-io-and-memory.md) |
| Optimise anything | [40-implementation/03](40-implementation/03-benchmarking-plan.md) §0 first |

---

## Full map

### `00-problem/` — what and why
| File | Takeaway | Status |
|---|---|---|
| [01-goals-and-constraints.md](00-problem/01-goals-and-constraints.md) | Constraints C1–C9, ranked objectives, non-goals, measurable success criteria | stable |
| [02-cost-model.md](00-problem/02-cost-model.md) | **The load-bearing document.** Unit prices, Scenario A (small) and **§5b Scenario B (the real target scale)**, design rules R1–R10 | stable |

### `10-prior-art/` — who solved this before
| File | Takeaway | Status |
|---|---|---|
| [01-warpstream.md](10-prior-art/01-warpstream.md) | Closest relative. Copy the 250 ms/8 MiB batching and the per-AZ chunk cache; replace the hosted metadata store | stable |
| [02-automq.md](10-prior-art/02-automq.md) | Open-source **Java** reference. Object format, the speculative tail read, the `ObjectStorage` SPI. **§8: how they avoid GET/LIST entirely, and the `AutomqGetPartitionSnapshot` (apiKey 516) remote-tail subscription that inlines delta bytes** | stable |
| [03-diskless-kafka-and-cas-systems.md](10-prior-art/03-diskless-kafka-and-cas-systems.md) | KIP-1150 validates the write/sequencer split; SlateDB gives epoch fencing; Quickwit warns against manifest CAS | stable |
| [04-comparison-matrix.md](10-prior-art/04-comparison-matrix.md) | One-page comparison; where we are simpler and where we are harder | stable |

### `20-opensearch/` — the consumer contract
| File | Takeaway | Status |
|---|---|---|
| [01-pull-based-ingestion-spi.md](20-opensearch/01-pull-based-ingestion-spi.md) | Five interfaces; the pointer must be a single `long`; shard N ⟷ partition N today | stable |
| [02-poller-semantics-and-cost.md](20-opensearch/02-poller-semantics-and-cost.md) | **Block inside `readNext`.** One subscription + one cache per *node*, not per shard | stable |
| [03-plugin-packaging.md](20-opensearch/03-plugin-packaging.md) | Copy `ingestion-fs` for shape, `repository-s3` for SDK packaging; credentials in the keystore | draft |

### `30-design-space/` — our design
| File | Takeaway | Status |
|---|---|---|
| [01-object-layout-and-format.md](30-design-space/01-object-layout-and-format.md) | Segment format; **no absolute offsets in segments**; header length in the key ⇒ 1-request reads; §7b **per-stream trickle flush policy**, required at 10,000 indices | proposal |
| [02-partition-bloom-in-key.md](30-design-space/02-partition-bloom-in-key.md) | ⚠️ **Revised for scale.** The prefix sets the filter's universe and the universe decides the encoding: exact bitmap for one mega-index (334 chars), index-level Bloom for the cluster, stream-level filtering impossible (19,984 chars). **Two-tier: Bloom in the key, exact directory in the header** | proposal |
| [03-metadata-and-cas.md](30-design-space/03-metadata-and-cas.md) | Leased sequencers, epoch fencing, **write-once commit log**, seal-by-racing-for-the-next-seq. **Highest risk** | proposal |
| [04-discovery-and-tailing.md](30-design-space/04-discovery-and-tailing.md) | Push over HTTP/2; §2b **serve tail bytes from the write buffer**, §2c **inline small deltas**, §2d **session-based incremental subscription**; a 4-tier fallback ladder down to LIST | proposal |
| [05-az-topology-and-data-flow.md](30-design-space/05-az-topology-and-data-flow.md) | The object store *is* the cross-AZ network; K8s shape; only metadata crosses zones. ⚠️ **Revised:** at ~300 nodes a per-AZ shared cache is now essential, probably hosted in the ingester nodes | proposal |
| [06-compaction-and-retention.md](30-design-space/06-compaction-and-retention.md) | **Defer compaction**; retention (default 6 h) is the real cost dial; GC is a v1 correctness requirement | proposal |
| [07-pluggable-store-abstraction.md](30-design-space/07-pluggable-store-abstraction.md) | ~10-method SPI, streaming in/out, conditional writes, decorators for retry/limit/count | proposal |
| [15-cost-governor.md](30-design-space/15-cost-governor.md) | **Counting is not controlling.** Ratio-to-expected limiting, a hard LIST ceiling, attribution, and a kill switch — because the per-shard-polling regression costs **$21,600/hour as LIST** | proposal |
| [14-producer-contract.md](30-design-space/14-producer-contract.md) | **OpenSearch `_bulk` NDJSON, unchanged, to a different URL.** No shard count, no hashing, no OpenSearch credentials. Ack semantics, lane, and the one deployment checkbox worth $3,624/month | proposal |
| [13-worked-example-multi-tenant.md](30-design-space/13-worked-example-multi-tenant.md) | **Start here to see the whole path once.** 5M tenants end to end: what the pod does, the object key, key-vs-header-vs-commit-log, and **16.5 PUT/s + 25 GET/s regardless of tenant count** | worked example |
| [12-fast-mode-wal-and-quorum.md](30-design-space/12-fast-mode-wal-and-quorum.md) | Opt-in WAL + AZ quorum: **~1.5–6 ms** ack-and-visible, but **$5,436/mo at 100% of 100 MiB/s** vs ~$400 for the whole normal system. Must be per-record. Weakens NFR-8 | proposal |
| [11-multi-tenancy-and-security.md](30-design-space/11-multi-tenancy-and-security.md) | **The bundling universe is a trust domain.** Isolation by rate limit, fair-share buffers and a segment share cap — not by per-index streams, which cost $312/mo below ~192 MiB/s. SSE-KMS needs Bucket Keys | decided |
| [10-client-library-and-fetch-modes.md](30-design-space/10-client-library-and-fetch-modes.md) | Three fetch modes — `inline` / `proxy` / `direct` — **the ingester chooses** by cache state and fan-out. Proxy penalty is ~0.5 ms *if* the pod streams through. Signed URLs keep credentials out of the OpenSearch JVM | proposal |
| [09-consumer-position-and-watermarks.md](30-design-space/09-consumer-position-and-watermarks.md) | **Do not build a Kafka-style offset store** — OpenSearch commits the pointer atomically with the documents. We need only a conservative GC watermark, and it may only *extend* retention | proposal |
| [08-failure-domains-and-resilience.md](30-design-space/08-failure-domains-and-resilience.md) | **≥2 ingester nodes per AZ**; the 3 singleton-ish roles inside a "stateless" service; safety is detection-independent; failure matrix, shutdown ordering, RTO/RPO, chaos matrix. **`all_active` replicas each consume the stream** | proposal |

### `40-implementation/` — how to build it
| File | Takeaway | Status |
|---|---|---|
| [01-java-runtime-helidon-vthreads.md](40-implementation/01-java-runtime-helidon-vthreads.md) | **JDK 25 LTS, not 21** (JEP 491). Helidon SE 4. CPU work off virtual threads. No `ThreadLocal` buffers | proposal |
| [02-streaming-io-and-memory.md](40-implementation/02-streaming-io-and-memory.md) | Length-prefixed framing; **never parse the payload**; pooled buffers; backpressure by blocking | proposal |
| [03-benchmarking-plan.md](40-implementation/03-benchmarking-plan.md) | Profile before optimising; JMH with `-prof gc`; **a cost benchmark that fails CI** | proposal |

### Cross-cutting
| File | Purpose |
|---|---|
| [50-open-questions.md](50-open-questions.md) | ✅ **All answered (2026-08-30).** Ten became ADRs; the rest are decided in place with evidence. What remains is six constants that need running code to settle — listed as *deferred to measurement*, not as open design |

---

## Ground truth on disk

Claims in this corpus are grounded in code where possible. Verify rather than trust:

| Path | What | How to refresh |
|---|---|---|
| `/home/tuong/work/OpenSearch` | OpenSearch **3.8.0** source: the ingestion SPI, `DefaultStreamPoller`, the `ingestion-fs`/`ingestion-kafka`/`ingestion-kinesis` plugins, `repository-s3` | already present |
| `.tmp/automq` | AutoMQ source: `ObjectWriter`/`ObjectReader`/`DataBlockIndex`, `ObjectStorage` SPI, `ObjectWALConfig` | `git clone --depth 1 --filter=blob:none https://github.com/AutoMQ/automq.git .tmp/automq` |

`.tmp/` is git-ignored. WarpStream is closed source — its documents are cited by URL only.

---

## Conventions

Every document opens with:
```
**Status:** stable | draft | proposal   **Confidence:** high | medium | low   **Last updated:** …
**Read this if:** …
**One-line takeaway:** …
```

- **stable** = verified against source or vendor docs, unlikely to change
- **draft** = correct in outline, details unverified
- **proposal** = our design, not yet validated by implementation

**When you learn something:** update the document *and* its one-line takeaway in this index, move
any resolved item into "Answered" in [50-open-questions.md](50-open-questions.md), and re-check the
arithmetic in [02-cost-model.md](00-problem/02-cost-model.md) if the change touches request rates.
Keeping the cost model true is what keeps the rest of this corpus honest.
