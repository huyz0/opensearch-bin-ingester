# 0068. Bound large node-scoped catch-up requests with V2

Status: accepted
Date: 2026-09-24
Requirements: FR-9, NFR-13
Supersedes: the request-size limit described by [ADR-0065](0065-node-scoped-catch-up-with-live-tail-priority.md), only for requests larger than 1,024 streams
Research: [ADR-0065](0065-node-scoped-catch-up-with-live-tail-priority.md); docs/research/30-design-space/10-client-library-and-fetch-modes.md#4-when-to-redirect-the-fan-out-rule

## Context

ADR-0065 makes catch-up node-scoped because the ingester coalesces shared
segment acquisition before fanning records out to streams. The V1 request bound
of 1,024 streams is too small for a large OpenSearch node. Splitting a node
snapshot into several POSTs appears bounded, but each POST independently walks
the committed chain and can reacquire a shared backlog segment. That changes
the object-store request rate from at most one GET per shared segment per
consumer node to as many as one GET per request batch.

The existing HTTP adapter bounds request bodies at 1 MiB. A larger request
therefore needs an explicit version, server body limit, and mixed-version rule.
An older peer may reject the new version with HTTP 400 or its old body limit
with HTTP 413.

## Decision

Keep the V1 wire bytes and its 1,024-stream maximum unchanged. Encode requests
with 1,025 through 120,000 streams as V2, with the same field layout and
semantics. The server accepts V1 and V2 requests, and the HTTP request-body
limit is 4 MiB. At the documented maximum, worst-case UUID, partition-varint,
and offset-varint fields fit within that bound.

The client uses one node-scoped POST for either version. For a V2 request only,
HTTP 400 or 413 means the peer does not support the bounded request and maps to
the existing `UNSUPPORTED` result. The coordinator then leaves the already-open
live subscriptions intact. Other failures retain normal retry behavior. A
snapshot above 120,000 streams is explicitly refused and logged; it is not
silently truncated or retried forever.

The response event and end frames remain V1: their layouts and meanings do not
change. V2 is solely a larger request envelope. Golden files pin both request
versions.

## Alternatives considered

* **Split the node snapshot into multiple V1 POSTs.** Rejected: each request
  can reacquire the same shared segment and multiply GETs by the number of
  batches, violating ADR-0065's per-node cost bound.
* **Raise V1's maximum without changing its version.** Rejected: existing
  readers enforce the 1,024-stream bound and must not reinterpret V1 bytes.
* **Allow an unbounded request or body.** Rejected: peer-provided request size
  must remain bounded, and a count without an explicit ceiling can allocate
  excessive memory before replay begins.
* **Treat every HTTP 400 or 413 as unsupported.** Rejected: those statuses for
  V1 may indicate a malformed or operationally failed request, not version
  incompatibility. Only V2 maps them to the compatibility fallback.

## Consequences

Large supported snapshots remain one node-scoped operation, preserving segment
coalescing and the existing object-store budget: at most one GET per shared
backlog segment per consumer node and zero LIST. Deployments with an older
ingester may temporarily receive live-only delivery until upgraded; the
consumer does not break its live subscription. The supported product ceiling
is explicit at 120,000 streams per node request.
