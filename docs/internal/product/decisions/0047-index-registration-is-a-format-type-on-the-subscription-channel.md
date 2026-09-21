<!-- SPDX-License-Identifier: Apache-2.0 -->
# 0047. Index registration is a `format` type on the subscription channel

Status: accepted
Date: 2026-09-17
Requirements: FR-16, FR-19, FR-13
Implements: [ADR-0015](0015-routing-registration-and-aliases.md) §2
Research: docs/research/20-opensearch/01-pull-based-ingestion-spi.md §3, §4

## Context

ADR-0015 §2 decided **that** the plugin registers index metadata over the
connection it already has, and listed the payload. It did not decide **what
shape those bytes take**, and bytes that cross a process boundary are a wire
format — so this is a `wire-format-change` with its own record.

The constraint that shapes it: `client` and `plugin` may not import
`io.github.huyz0.os.biningester.binstore` (ADR-0023) and `plugin` may not import `ingest`
(architecture.md). The only module both ends already depend on is `format`,
which is where `SubscriptionEvent`, `CommitDelta` and `Grant` already live for
the same reason.

## Decision

### 1. `IndexRegistration` is a record in `format`, with its own magic

`BIRG`, version 1, big-endian magic so an operator reading a hex dump can tell
one frame from another. It carries `indexUuid`, `indexName`, `aliases[]`,
`numShards`, `routingNumShards`, `routingFactor` and `routingPartitionSize`.

### 2. It carries what PLACEMENT needs and nothing else

ADR-0015 §2's payload also lists `replicationMode`, `allActive`, `lanes[]`,
`ackMode` and `quorum`. Those serve FR-17 (fast mode) and FR-18 (priority
lanes), both out of M6's scope. **A field with no reader is a field nothing can
be wrong about**, and carrying five of them would put five values on the wire
that no test constrains and no code consults. Adding them is another
wire-format change, and this paragraph is where its author starts.

### 3. The index UUID travels as a STRING

An OpenSearch index UUID is BASE64URL — `UUID.fromString` throws on every real
one, which `BinStoreConsumerFactory.indexUuidOf` records discovering on a
booting node because every in-process test had built a `java.util.UUID`
directly. What travels is what the cluster state says; converting it to stream
identity stays the reader's job, in one place that is already written down.

### 4. The decoder refuses rather than guesses

An unknown version **stops**. A torn frame stops. A count is bounded by the
bytes that could justify it before it becomes an allocation. A structurally
valid frame that is semantically impossible — a `routingFactor` that disagrees
with `routingNumShards / numShards`, a zero shard count, a blank alias — is an
`IOException`, not an `IllegalArgumentException`, because it arrived over a
wire and an unchecked throw out of a decode is the shape that kills a poll
loop.

⚠️ **The refusals matter more here than in any format this project has shipped,
and the reason is the failure mode.** A `CommitDelta` that will not decode
stops a recovery loudly. A registration that decodes *wrongly* places records
on the wrong shard, and OpenSearch never sets `_routing` on an ingested
document — so a routed search simply misses them. No exception, no failed gate,
no metric.

## Consequences

- **`routingFactor` is refused at zero and cross-checked**, because it divides.
  OpenSearch derives it as `routingNumShards / numShards`; a frame carrying a
  different value describes an index that cannot exist, and using it would
  misplace every routed record for that index.
- **`routingPartitionSize` is carried so it can be refused later.** With
  built-in tenant partitioning the shard depends on the routing value *and* the
  document `_id` (`OperationRouting:587`), which the ingester does not have.
  Where the refusal happens — the routing MODE, never the index, since explicit
  placement needs no hash — is M6.3's.
- **Nothing decodes it yet.** M6.2 ships the format, the golden files and the
  refusals; M6.3 is the reader and M6.7 is the writer. ⚠️ Stated rather than
  implied: `wire-format-change`'s "every reader, every writer, the fakes and
  the golden files in ONE commit" is satisfied **vacuously** at this point,
  because there is exactly one of each and it is this record. The obligation
  becomes real the moment M6.3 lands, and it binds every later change.
- **Two golden files, not one.** An unsplit index has
  `routingNumShards == numShards` and `routingFactor == 1`, so a fixture with
  only that shape cannot see the two fields being swapped in the encoder.

## Alternatives considered

- **A JSON object, like `IndexRegistry`'s `indices.json`.** Rejected: that file
  is a durable control object an operator `cat`s, written rarely and read by
  one component. This frame travels the subscription channel on every relevant
  cluster-state change, and the channel is already binary and versioned. A
  second encoding on the same wire is a second parser to get wrong.
- **Extending `SubscriptionEvent` with a registration variant.** Rejected: it
  travels the other direction — event is ingester → plugin, registration is
  plugin → ingester — and a type whose fields are meaningful in one direction
  only is how a decoder ends up with a mode switch nobody reads.
- **Sending `IndexMetadata` itself.** Rejected outright: it is an OpenSearch
  type, and `ingest` may not depend on OpenSearch. That dependency is the thing
  the whole module layout exists to prevent.
- **Carrying the full ADR-0015 §2 payload now.** Rejected per decision 2, and
  the cost is honest: adding the rest later is a version bump, not a free
  extension.
