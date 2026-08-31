# Glossary

**Family:** Process
**Read when:** Writing any document, skill, ADR, commit message, log line, metric name, or identifier. These terms are binding.

⚠️ **One name per concept, and no synonyms.** A system described with drifting
vocabulary cannot be reasoned about, and this corpus already drifted once —
"service", "service pod", "service node" and "ingester pod" all meant the same
thing. → `scripts/check-terminology.sh`

## The six roles

| Term | Definition |
|---|---|
| **producer** | Whatever supplies records — either a remote client POSTing `_bulk` and receiving `202`, or producer logic inside a gateway calling the in-process API. Knows nothing of partitions, shard counts or object storage. |
| **gateway** | A **deployment shape**, not a separate tier: a host process that embeds the ingest library in-process alongside producer logic, rather than reaching it over HTTP. ~2 per AZ. An ingester node running in gateway shape is still an ingester node. |
| **ingest library** | The ingest core as an embeddable Java library — accumulators, writer, reader, sequencer — with **no HTTP dependency**. The HTTP endpoint is a thin adapter over it ([ADR-0019](../product/decisions/0019-two-delivery-surfaces.md)). |
| **ingester** | The **service** as a whole: many nodes running as Kubernetes pods, spanning three AZs, horizontally scalable. May have EBS or equivalent attached storage — **only when `wal=true`**; with `wal=false` it is diskless. |
| **ingester node** | One pod of the ingester. Contains a writer, a reader and possibly the sequencer lease. |
| **writer** | The component **inside an ingester node** that writes to object storage, or to the WAL when `wal=true`. Every ingester node runs one ([ADR-0017](../product/decisions/0017-every-pod-writes.md)). |
| **reader** | The component **inside an ingester node** that reads from object storage, from the WAL, or from a peer ingester node in the same AZ. |
| **consumer** | The **client library** that talks to a reader to obtain records. Owns subscription, fetch-mode dispatch, coalescing and decode. |
| **plugin** | The OpenSearch plugin that **bundles the consumer library** and runs in-process inside an OpenSearch data node. |

```
producer ──bulk/202──▶ ingester  (K8s pods, 3 AZ, scalable)
                        ├── writer ──▶ object store / WAL
                        └── reader ◀── object store / WAL / peer ingester node
                                ▲
                                │
                          consumer  (library)
                                │ bundled into
                          plugin  (in the OpenSearch node process)
```

## Deprecated — do not use

| ❌ Never write | ✅ Write |
|---|---|
| "the service", "service pod", "service node" | **ingester**, **ingester node** |
| "the client library" | **consumer**, or **consumer library** |
| "ingester pod" | **ingester node** |
| "agent", "broker", "collector" — *for our own components* | one of the six above |
| "service-side" | **ingester-side** |

⚠️ Kubernetes' own `Service` resource keeps its name — it is a different thing,
and is always written capitalised and in code font.

⚠️ **Other systems keep their own vocabulary.** `docs/research/10-prior-art/`
describes Kafka, WarpStream and AutoMQ, where "broker" is the correct term for
*their* component; "agent" names this repository's harness. The ban is on using
those words for **our** six roles, which is why `check-terminology.sh` enforces
the unambiguous rows and not those two.

## Data-plane terms

| Term | Definition |
|---|---|
| **record** | One document payload, opaque bytes to the ingester. |
| **stream** | The ordered record sequence for one `(indexUUID, partitionId)`. |
| **run** | The contiguous chunk of one stream's records inside one segment. |
| **segment** | One object written by one writer at one flush; contains many runs. |
| **offset** | A record's `long` position within its stream, assigned by the sequencer at commit. |
| **partition** | A source partition; shard *N* consumes partition *N*. |
| **lane** | A signed `i8` scheduling bucket: `0` standard, negative background, positive elevated. |
| **sequencer** | The leased role that assigns offsets and writes the commit log. Runs on one ingester node. |
| **commit log** | The write-once chain of deltas and checkpoints that defines ordering. |
| **trust domain** | The bundling universe — one OpenSearch cluster. Segments are never bundled across two. |

## Naming carries into code

Package names, class names, metric names and log fields use these terms:
`IngesterNode`, `SegmentWriter`, `SegmentReader`, `ConsumerClient`,
`ingester_segments_written_total`. ⚠️ **A class called `ServiceNode` is a
standards violation**, not a style preference.
