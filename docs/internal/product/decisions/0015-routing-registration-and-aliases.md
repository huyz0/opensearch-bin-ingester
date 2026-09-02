# 0015. The producer sends a routing value; the plugin registers the shard count

Status: accepted
Date: 2026-08-30
Requirements: FR-13, FR-16, FR-19
Research: docs/research/20-opensearch/01-pull-based-ingestion-spi.md §4
Supersedes the `os_routing` mode sketched in ADR-0006 §Decision (kept, but the producer no longer computes it)

## Context

Producers want to send mutations with a **custom routing value** — OpenSearch's
`_routing` — and have it decide the partition. Computing a partition from a
routing value needs the index's shard count, which lives in OpenSearch.

Two mechanisms were proposed: an unauthenticated endpoint exposed by the plugin,
or the producer talking to OpenSearch directly with a password file or a rotating
client certificate.

⚠️ **Both solve a problem the producer should not have.**

## Decision

### 1. The producer sends the routing value; the ingester computes the partition

```
frame: [u8 type][i8 lane][u16 routingLen][routing bytes][u32 payloadLen][payload]
```

The producer never learns the shard count, never contacts OpenSearch, needs no
credentials, and has no certificate to rotate. The ingester already knows
`numPartitions` (FR-16), and hashing a short routing string is ~50 ns — it is not
document parsing, which stays forbidden.

`partition = floorMod(murmur3_32(routing), routingNumShards) / routingFactor`,
matching `OperationRouting.calculateScaledShardId`. For a non-split index this
reduces to `floorMod(murmur3_32(routing), numShards)`.

⚠️ **For `os_routing` indices this is a correctness requirement, not an
optimisation.** OpenSearch never sets `_routing` on an ingested document —
`MessageProcessorRunnable` has no such key and `IngestPipelineExecutor` forbids
mutating it — so routed search finds a document *only* because our placement
reproduces OpenSearch's hash. A placement error is a **silent wrong answer**: the
query simply misses the document, with no exception and no gate. The
implementation must be tested against `OperationRouting`, not reimplemented from
memory, and must reject indices where the simple form does not hold
(`routing_partition_size > 1` without a producer-supplied `_id`). See
[SPI §4b](../../../research/20-opensearch/01-pull-based-ingestion-spi.md).

### 2. The plugin registers index metadata over the connection it already has

The plugin runs **inside** OpenSearch, receives `IndexMetadata` in
`createShardConsumer`, can hold a `ClusterStateListener`, and already maintains an
authenticated subscription to the ingester. So it pushes:

```
{ indexUUID, indexName, aliases[], numShards, routingNumShards, routingFactor,
  replicationMode, allActive, lanes[], ackMode, quorum }
```

on connect and on every relevant cluster-state change.

**No new endpoint. No new credential. No unauthenticated surface.** The trust
relationship that already exists for subscriptions carries the registration.

### 3. Buffer-and-reroute covers the window where the shard count is unknown

⚠️ **A buffered record has no offset yet, so it can still be moved between
partitions.** That is a direct consequence of
[ADR-0001](0001-segments-carry-no-absolute-offsets.md) — ordering is assigned at
commit, not at write — and it dissolves the bootstrap problem without deferring
anything to read time.

When a record arrives for an index whose shard count the ingester does not yet
know (never registered, or a rollover the plugin has not yet pushed):

1. Accept it into a bounded **pending pool**, FIFO per index. No keying by routing
   value is needed — the record simply has not been assigned a partition yet.
2. On registration arriving — normally within milliseconds — assign partitions and
   fold the records into the ordinary buffers.
3. Only if registration does not arrive within `pendingTimeout` (default 5 s) is
   the write **rejected**, per ADR-0006's "reject, never fold".

This makes the alias-rollover staleness window (§4) shrink from "records land in
the previous index" to "records wait a few milliseconds and land in the right
one", and it means a producer that starts before the plugin connects is not
punished for a race it cannot see.

⚠️ The pending pool is bounded and counted against the slab pool like any other
buffer; an unregistered index must not be able to consume memory indefinitely.

### 4. Aliases and shard-count changes are supported

Stream identity is `(indexUUID, partition)` — **per concrete index**, never per
alias. So:

- A producer writes to alias `logs`; the ingester resolves it to the current
  concrete index and uses **that index's** shard count.
- On rollover, `logs` points at `logs-000002` with a different shard count. New
  records partition by the new count; records already in the log keep their
  partitions, in the previous index's streams. Nothing is repartitioned, because
  nothing needs to be.

⚠️ **Bounded staleness at rollover:** between the rollover and the plugin pushing
the new mapping, records land in the *previous* index. For time-series rollover
this is normal and harmless, but it must be documented rather than discovered.

**What is not supported: changing the shard count of an index that is already
ingesting.** Resize creates a new index and requires the source to be read-only,
so it is incompatible with an ingesting index regardless of us. **Roll over
instead** — which is what aliases are for, and is the pattern this design serves
natively.

## Alternatives considered

- **An unauthenticated endpoint on the plugin exposing shard counts.** Rejected:
  a plugin cannot unilaterally bypass the security plugin, making it open is a
  cluster-admin decision we should not force, and it leaks topology to anyone who
  can reach the node. It also solves only half the problem — the producer would
  still need alias resolution.
- **Producer authenticates to OpenSearch** (password in a mounted secret, or a
  rotating client certificate polled from the filesystem). Rejected: it puts
  OpenSearch credentials in every producer, requires certificate-rotation logic in
  a consumer library, and couples producer deployment to cluster topology. Retained
  only as a documented fallback for a deployment with no plugin.
- **Producer computes the partition itself** (ADR-0006's original framing).
  Retained as the `explicit` mode for producers that know their partition, but no
  longer the answer for routing-based placement.
- **Defer routing to read time: write records routing-agnostic and let the
  *plugin* filter** — each shard reads everything and keeps the `1/S` that hashes
  to it. ⚠️ **Rejected on serving amplification**, which is `S`×:

  | | shards | ingest | serving |
  |---|---|---|---|
  | small index | 10 | 0.01 MiB/s | 0.1 MiB/s (10×) |
  | typical index | 16 | 1 MiB/s | 16 MiB/s (16×) |
  | **mega index** | **2,000** | **50 MiB/s** | **100 GiB/s (2,000×)** |

  [ADR-0004](0004-the-service-serves-reads.md) exists precisely to hold serving at
  ~1× ingest. This is the same mistake in a different place.

- **Defer to read time, but filter in the *service*: key segment runs by a fixed
  bucket space** (`bucket = floorMod(murmur3(routing), B)`), sort runs by bucket,
  and have the ingester serve shard *N* the contiguous bucket range that maps to it.
  This is genuinely elegant — it is exactly how OpenSearch's own
  `routingNumShards` / `routingFactor` supports splitting, the bucket range per
  shard is contiguous so serving stays at 1× with no amplification, and the
  write path needs no shard count.

  ⚠️ **Rejected on metadata inflation, twice over.**

  | Run key | Streams at target scale |
  |---|---|
  | partition (today) | **119,900** |
  | bucket, B=256 | 2,577,920 (**22×**) |
  | bucket, B=1024 | 10,250,240 (**85×**) |

  The directory, the commit-log index entries and the trickle policy all inflate
  by the same factor, and the per-stream run sizes collapse — reintroducing the
  single-record-run pathology that
  [object-layout §7b](../../../research/30-design-space/01-object-layout-and-format.md)
  exists to prevent. **And it removes no dependency:** `B` must equal the index's
  `routingNumShards`, which is fixed at index creation and known exactly when the
  shard count is. It also needs `MODULO`/`RANGE` partition assignment and a
  composite pointer, neither of which exists in OpenSearch 3.8.0.

- **The ingester reads OpenSearch cluster state.** Rejected in ADR-0005 and
  ADR-0006 and still rejected: it puts OpenSearch credentials in the ingester. The
  plugin pushing *out* is the same information travelling the safe direction.

## Consequences

- The producer's contract shrinks to: index or alias name, optional routing value,
  optional lane, payload. No topology, no credentials, no rotation.
- The plugin gains a registration responsibility and a `ClusterStateListener`.
- The chicken-and-egg between "producer writes" and "plugin registers" is handled
  by **buffer-and-reroute** (§3), not by rejection: writes are held in a bounded
  pending pool for up to `pendingTimeout` and only then rejected. A manual
  registration API exists for bootstrap and for deployments with no plugin.
- **The write-time shard-count dependency is real but cheap**, and the two
  alternatives that remove it are both worse — plugin-side filtering costs `S`×
  serving, and bucket-keyed runs cost 22–85× in metadata while removing no
  dependency at all. Keeping the resolution at write time, with a buffer to cover
  the window, is the cheapest correct arrangement.
- Alias resolution is an ingester-side map lookup on the hot path; it must be a map
  lookup, not a network call.
- The frame now carries a variable-length routing field, so this is a
  [`wire-format-change`](../../../../.agents/skills/wire-format-change/SKILL.md) and
  travels with golden-file tests.
