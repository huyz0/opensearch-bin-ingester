# 0065. Node-scoped catch-up with live-tail priority

Status: accepted
Date: 2026-09-22
Requirements: FR-9, NFR-13
Research: docs/research/30-design-space/08-failure-domains-and-resilience.md#61-whole-az-loss-step-by-step; docs/research/30-design-space/10-client-library-and-fetch-modes.md#4-when-to-redirect-the-fan-out-rule

## Context

The current subscription is a live tail. A resumed OpenSearch shard can ask
for a fresh retained floor, but the subscription has no replay-from-offset
operation. Consequently a node killed with an acknowledged backlog cannot
recover its records without either waiting for them to be published again or
adding a new read path.

The recovery path has two constraints that must be true at the same time:

* the node resumes from its persisted `batch_start`, with no per-shard
  object-store request amplification; and
* records written after the kill remain visible within the normal interval
  bound while the old records are being recovered.

The cost budget for this path is at most one segment GET per backlog segment
per consumer node (R5). The hot path must not issue LIST, and the replay queue
must be bounded so a large backlog cannot become an OpenSearch-node heap leak.

## Decision

Add a node-scoped catch-up operation to the existing subscription seam. A node
submits the streams and their resume offsets through its one multi-subscription;
the ingester resolves committed deltas from the durable chain and returns the
same `SubscriptionEvent` data shape through a catch-up delivery lane. Existing
event bytes remain readable; the new control operation is the versioned part of
the subscription protocol. Version 1 is an explicit tagged control frame:
`CATCH_UP_REQUEST(v=1, request_id, streams[{index, partition, batch_start}])`,
followed by tagged `CATCH_UP_EVENT(v=1, request_id, stream, event)` and
`CATCH_UP_END(v=1, request_id)` frames. An old peer or an unknown version must
return `UNSUPPORTED_CATCH_UP_VERSION` without advancing or acknowledging the
live subscription; the caller then uses the existing reconnect/fallback path.
An unknown response frame is a protocol error and also cannot acknowledge the
range. M8.24's implementation includes the round-trip and mixed-version cases.

The ingester owns a per-node replay coordinator with two bounded classes:

1. live pushes are high priority and may preempt a catch-up batch; and
2. catch-up work is round-robin across streams, yields after each bounded
   batch, and receives a service turn after each bounded live quantum whenever
   both lanes are pending. The live quantum is an explicit scheduler bound,
   not an unbounded "drain while non-empty" rule, so catch-up cannot starve
   under sustained live traffic. Its value is selected and measured by the
   M8.24 T4 harness.

The coordinator coalesces segment acquisition by `(node, segment)` before
fan-out. A segment is fetched at most once for that node. When the node is the
only catch-up consumer of a cold segment, the ingester uses the existing
short-lived direct grant policy; a shared or live delivery uses the existing
proxy path. Replay never performs LIST.

The consumer keeps live and catch-up deliveries in separate bounded queues and
always drains live records first. A catch-up completion is acknowledged only
after the replayed range has been delivered, so a reconnect can repeat a
range safely from `batch_start` under the existing at-least-once semantics.

## Alternatives considered

* **Replay independently per shard.** Rejected: the same segment would be
  fetched once per shard, making request rate scale with shards instead of
  nodes and violating R5.
* **Put the catch-up queue ahead of the live queue.** Rejected: a node with a
  one-hour backlog would make a newly written record wait behind every old
  segment. That is the exact starvation failure in research 08 §6.1 and M8
  criterion 18.
* **Keep a replay history only in `SubscriptionHub`.** Rejected: it is lost on
  ingester restart or failover and therefore cannot recover an acknowledged
  backlog from durable state.
* **Give the plugin a cloud SDK or long-lived grants.** Rejected by
  [ADR-0064](0064-node-local-store-reader-for-plugin-fallback.md): credentials
  do not belong in the OpenSearch JVM, and a long-lived grant changes the
  security boundary.
* **Use LIST to discover the backlog.** Rejected: LIST is recovery-only and
  has its own hard ceiling (R2/R15); the committed chain already supplies the
  ordered segment references.

## Consequences

The replay mechanism has a durable source of truth, one segment fetch per
consumer node, and an explicit scheduling rule that can be tested both in a
small fake and in the OpenSearch kill test. The protocol gains a control
operation and therefore requires the old/new reader rollout rules and a
round-trip test before implementation lands.

The ingester must retain enough bounded replay state while resolving a request,
and a node may perform more total work during recovery. That work is bounded
by committed segments and is deliberately traded for preserving the live-tail
latency bound. The implementation task owns the exact queue-size configuration
and the T4 measurement; this ADR does not invent a latency number.
