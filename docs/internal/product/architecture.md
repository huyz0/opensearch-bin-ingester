# Architecture

Derived from [`docs/research/`](../../research/README.md). This file is the
component map; the research documents carry the reasoning and the numbers.

## Vocabulary

Defined and enforced in [glossary.md](../standards/glossary.md): **producer** ·
**ingester** (many **ingester nodes**) · **writer** and **reader** (components
inside a node) · **consumer** (library) · **plugin** (bundles the consumer,
runs in the OpenSearch process).

## Components

```
producers ──HTTP/streamed──▶ ingester nodes (≥2 per AZ, stateless)
                               │  buffer by (index, partition)
                               │  flush: 8 MiB, or the adaptive interval
                               ▼
                          object store (regional, multi-AZ)
                            segments · commit log · leases · checkpoints
                               ▲                    │
        prefetch on commit ────┘                    │ push events (metadata)
                                                    ▼
                              OpenSearch data nodes ── plugin ── consumer library
```

## Modules

| Module | Contains | Depends on |
|---|---|---|
| `binstore-spi` | `BinStore`, `Version`, `Capabilities`, decorators (retry, rate-limit, **counting**) | nothing |
| `binstore-backends` | S3, GCS, Azure, local-FS, in-memory | `binstore-spi` |
| `format` | segment layout, key grammar, membership filters, commit-log records | nothing (pure) |
| `sequencer` | leases, epoch fencing, write-once commit log, checkpoints | `binstore-spi`, `format` |
| `ingest` | **the library**: accumulators, flush, writer, reader, subscription fan-out, GC. **No HTTP dependency** | `binstore-spi`, `format`, `sequencer` |
| `http` | thin Helidon adapter: parse `_bulk`, map errors, delegate to `ingest` | `ingest` |
| `client` | the **consumer** (glossary.md's role name): subscription, fetch-mode dispatch, coalescing, decode. ⚠️ The module is `client` for its Gradle path; every type inside it is named for the role — `ConsumerClient`, not `ClientClient` | `format` |
| `plugin` | `IngestionConsumerPlugin` implementation | `client` |

⚠️ **The flush interval is adaptive, and it is the cost dial**
([ADR-0017](decisions/0017-every-pod-writes.md)). Each pod lengthens or shortens
its own interval from its own observed rate — no shared signal, no lease. A flat
250 ms costs **$311/mo** where a 5 s ceiling costs **$15.55/mo** for the same
traffic, a 20× difference on the one number this project exists to control. The
ceiling is derived from `maxStreamLatency`, not configured separately.

## The peer mesh

Pods talk directly — commit forwarding, directed push fan-out, and intra-AZ
segment fetch over a consistent-hash ring. **There is no gossip protocol**:
membership is a Kubernetes `EndpointSlice` watch and placement is computed, not
discovered ([ADR-0012](decisions/0012-peer-mesh-without-gossip.md)).
⚠️ **Peer fetch is intra-AZ only** — a cross-AZ peer fetch costs 419× the
object-store GET it would replace.

## No consensus layer

There is no master node, no Raft, no KRaft, no external quorum — see
[ADR-0011](decisions/0011-no-consensus-cluster.md). Coordination is
compare-and-swap on the object store, which is a consensus primitive in its own
right. Pods are cattle; membership disagreement costs an extra GET, never
correctness.

## Seams (the only places I/O is allowed)

`BinStore` · `Clock` · `Sequencer` · `SubscriptionTransport`.
A new seam is an ADR. Every seam has a fake, kept in step in the same commit.

## Dependency rules

1. `format` depends on nothing and is pure — it is where T0 tests live.
2. `plugin` depends on `client`, never on `binstore-backends`. The plugin's
   dependency surface is deliberately minimal: no cloud SDK in the OpenSearch JVM.
3. The ingester and the plugin share **formats and the SPI**, never runtime choices.
4. Nothing depends on `http`.
5. ⚠️ **No module below `http` may reference HTTP.** `format`, `binstore-spi`,
   `sequencer` and `ingest` compile without Helidon on the classpath — asserted by
   a gate once the build exists. Two front doors, one implementation
   ([ADR-0019](decisions/0019-two-delivery-surfaces.md)).

## The invariants

| # | Invariant |
|---|---|
| I1 | No commit-log sequence number is ever written twice |
| I2 | A committed offset is never reassigned |
| I3 | A reader applies only deltas in a sealed prefix or a later epoch's chain |
| I4 | Uncommitted records may be reordered or dropped; committed ones may not |
| I5 | No acknowledged commit exists beyond a `SEAL` in its own chain — a commit is acked only after every lower-numbered write is confirmed (ADR-0011) |

I2 is load-bearing for OpenSearch consistency, not just our replay: with
`all_active` replicas, two copies converge only because record *R* is at offset
*N* on both.
