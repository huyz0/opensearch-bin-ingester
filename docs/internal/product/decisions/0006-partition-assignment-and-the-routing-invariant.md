# 0006. Partition assignment, and why OpenSearch routing does not constrain it

Status: accepted
Date: 2026-08-30
Requirements: FR-13
Research: docs/research/20-opensearch/01-pull-based-ingestion-spi.md §4 (Q1)

## Context

A producer must place each record in a partition, and shard *N* consumes
partition *N*. The worry was that the partition function must reproduce
OpenSearch's own document routing, or `GET /index/_doc/<id>` would query the
wrong shard and miss the document.

Evidence from OpenSearch 3.8.0 source settles it:

- `OperationRouting.generateShardId` routes a by-id lookup to
  `floorMod(murmur3(effectiveRouting), routingNumShards) / routingFactor`, where
  `effectiveRouting` is `_routing` or, absent that, `_id`.
- `MessageProcessorRunnable` performs **no routing at all** on the ingestion
  path — it indexes into the shard that consumed the partition.
- The ingestion mappers expose `_id`, `_op_type`, `_version` and `_source`, and
  **no `_routing`**.
- ⚠️ **OpenSearch's own `RawPayloadIngestionMessageMapper` sets
  `_id = shardId + "-" + pointer`** — which does not hash back to `shardId`
  except by coincidence.

So OpenSearch's pull-based ingestion **already breaks the routing invariant by
design**. Search fans out to all shards and finds everything; by-id `GET`,
`update` and `delete` do not.

## Decision

**The partition function is ours to choose.** Two modes, per index:

| Mode | Partition | Use |
|---|---|---|
| **`explicit`** (default) | the producer sends `partitionId` in the frame | balanced placement; `_doc` by-id APIs not used |
| `routing_key` | `floorMod(murmur3_32(key), numPartitions)` over a producer-supplied key | co-locate related records |
| `os_routing` | `floorMod(murmur3_32(_id), numPartitions)`, matching OpenSearch's non-split formula | **only** for indices whose users need by-id `GET`/`update`/`delete` |

The ingester learns `numPartitions` from an **explicit index registration API**,
not from OpenSearch cluster state — reading cluster state would put OpenSearch
credentials in the ingester, which ADR-0005 rejected for the same reason.

**A partition outside `[0, numPartitions)` is rejected**, loudly, with the
registered value in the error. Never folded with a modulo.

## Alternatives considered

- **Always reproduce OpenSearch routing.** Rejected: it constrains placement for
  a guarantee OpenSearch's own default mapper does not provide, and it forces the
  service to parse `_id` out of every document — the most expensive thing it
  could do (`streaming-io §3`).
- **Service reads the cluster state to learn shard counts.** Rejected: couples
  the ingester to OpenSearch credentials and topology, contradicting ADR-0005.
- **Fold out-of-range partitions with modulo.** Rejected: silently mis-places
  data and hides a producer/index mismatch until someone notices missing
  documents.

## Consequences

- Producers need no knowledge of OpenSearch internals in the default mode.
- ⚠️ **`os_routing` must be documented as reducing placement freedom**, and it is
  only correct for non-split, non-shrunk, non-virtual-shard indices; the general
  formula involves `routingNumShards` and `routingFactor`. Validate at
  registration and refuse the mode where the simple form does not hold.
- Repartitioning is not supported, because OpenSearch cannot change shard count
  for an ingesting index. `numPartitions` is fixed at registration.
- FR-13 moves from `blocked` to `agreed`.
