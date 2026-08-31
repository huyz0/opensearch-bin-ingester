# Requirements

IDs are stable and cited by specs, tasks, and ADRs. Status is `agreed`,
`proposed`, or `blocked` (on a 🔴 open question in the research corpus).

## Functional

| ID | Requirement | Status |
|---|---|---|
| FR-1 | Accept record writes over HTTP for many indices and partitions on one connection, streamed, without materialising the body | agreed |
| FR-2 | Bundle records from many indices and partitions into one segment per pod per flush | agreed |
| FR-3 | Assign a stable, monotonic `long` offset per `(index, partition)`, immutable once visible | agreed |
| FR-4 | Acknowledge a write only after the segment is durable **and** the offset is committed | agreed |
| FR-5 | Push tail notifications to subscribers over a persistent same-AZ channel, with session-based incremental subscription | agreed |
| FR-6 | Serve record bytes in three modes — `inline`, `proxy`, `direct` (signed URL) — chosen by the ingester | agreed |
| FR-7 | An OpenSearch plugin implementing `IngestionConsumerPlugin` for OpenSearch 3.8.0+ | agreed |
| FR-8 | A pluggable store SPI with S3, GCS, Azure Blob, local-FS and in-memory backends | agreed |
| FR-9 | Retention and GC driven by time **and** a conservative consumer watermark | agreed |
| FR-10 | Consumers keep working without the ingester, via a documented fallback ladder down to LIST recovery | agreed |
| FR-11 | Sequencer leadership by object-store CAS lease with epoch fencing and a write-once commit log | agreed |
| FR-12 | Multi-AZ Kubernetes deployment, ≥2 ingester nodes per AZ | agreed |
| FR-13 | Producers supply the partition (`explicit` \| `routing_key` \| `os_routing`); the ingester validates against a registered partition count and rejects mismatches | agreed (ADR-0006) |
| FR-14 | Compaction of small segments into larger or per-stream objects | proposed |

## Non-functional

| ID | Requirement | Target | Status |
|---|---|---|---|
| NFR-1 | Write request rate | < 0.30 requests per MiB ingested | agreed |
| NFR-2 | Idle cost | **zero** object-store requests from consumers | agreed |
| NFR-3 | LIST on hot paths | zero, and a hard runtime ceiling of ~1/s sustained | agreed |
| NFR-16 | Governor refusals in steady state | zero | agreed |
| NFR-4 | Read request rate | scales with segments, AZs, nodes — never with shards, partitions or indices | agreed |
| NFR-5 | Cross-AZ bytes | < 0.1% of ingested bytes | agreed |
| NFR-6 | Service memory | bounded and independent of request size | agreed |
| NFR-7 | End-to-end latency | p99 < 3× the configured flush window | agreed |
| NFR-8 | RPO for acked writes, `ack_mode=durable` | 0 | agreed |
| NFR-14 | RPO for acked writes, `ack_mode=wal` | 0 while ≥1 quorum member survives the upload window (~250 ms–1 s). ⚠️ **A weaker guarantee, opt-in per index/record** | agreed (ADR-0013) |
| NFR-15 | Fast-mode ack-and-visible latency | p99 < 10 ms | agreed (ADR-0013) |
| NFR-9 | RTO for offset visibility after sequencer loss | < 5 s | agreed |
| NFR-10 | Survive the loss of one AZ | no data loss; ⅓ capacity loss | agreed |
| NFR-11 | Offset stability across sequencer failover | absolute | agreed |
| NFR-12 | Object key length | ≤ 1024 bytes for every input, including pathological | agreed |
| NFR-13 | Consumer outage tolerance | = retention (default 6 h) | agreed |

| FR-15 | Per-index admission rate limiting, fair-share buffering, and a per-index segment share cap | agreed (ADR-0010) |
| FR-16 | Index registration, pushed by the plugin over its existing subscription: shard counts, aliases, replication mode, lanes, ack mode | agreed (ADR-0015) |
| FR-17 | **Fast mode**: opt-in WAL replicated to a quorum of AZs, ack and visibility in ~1.5–6 ms. Three per-index tuning settings — `flush_timer`, `wal`, `wal_quorum` — set in OpenSearch and pushed by the plugin | agreed (ADR-0013, ADR-0017) |
| FR-18 | **Priority lanes**: signed `i8` bucket in the frame (0 = standard, negative = background, positive = elevated), ≤8 active, expressed as scheduling priority through admission, buffering, flush, commit and push, with weighted fair share and a per-bucket latency ceiling | agreed (ADR-0014) |
| FR-21 | **Cost governor**: ratio-to-expected limiting per purpose class, a hard LIST ceiling, attribution by `(op, purpose, domain, index)`, and a kill switch that halts discretionary work but never the write path | agreed (doc 15) |
| FR-20 | `CredentialSource` SPI: a presented credential resolves to a `Principal` carrying its trust domain; file-backed with hot reload, sidecar-backed later | agreed (ADR-0021) |
| FR-19 | Producers send a **routing value**; the ingester computes the partition. Aliases resolved ingester-side to the current concrete index | agreed (ADR-0015) |

⚠️ **A requirement marked `blocked` may not be implemented.** Nothing is blocked
as of 2026-08-30 — every open question is decided; see
[`docs/research/50-open-questions.md`](../../research/50-open-questions.md).
