# 0004. The ingester serves reads; the plugin does not read the object store on the hot path

Status: accepted
Date: 2026-08-30
Requirements: FR-6, NFR-4, NFR-5
Research: docs/research/30-design-space/04-discovery-and-tailing.md §2a, §2b, docs/research/30-design-space/10-client-library-and-fetch-modes.md

## Context

An earlier position held that the plugin reading the object store directly was an
advantage over WarpStream's agent-serves-fetches model. At ~9 data nodes that is
true. At the target scale (~120,000 shards ⇒ ~300 data nodes) it is false, and
the arithmetic is not close.

A node hosts ~400 of ~120,000 streams — ~0.33% of any segment — with its runs
scattered through a segment sorted by `(index, partition)`.

## Decision

Reads are served by same-AZ ingester nodes, which **stream through, never
buffer-then-forward**, and **prefetch on commit**. The pod that wrote a segment
still holds it in RAM, so the writing AZ needs no GET: **2 GETs per segment for
3 AZs, independent of node count.**

The ingester also issues short-lived **signed URLs** so a client can read the
object store directly when fan-out is 1 (catch-up replay) or the pod is under
pressure.

## Alternatives considered

Measured at a 250 ms flush, 300 nodes:

- **Plugin reads whole objects from S3.** $3,732/month, and **29.3 GiB/s of
  aggregate read bandwidth — 100 MiB/s per node for 0.33 MiB/s of useful data**.
- **Plugin reads precise per-run ranges.** $1,492,992/month. Byte-range GETs are
  billed per request.
- **A distributed cache among OpenSearch data nodes** (WarpStream's per-AZ
  distributed mmap). Rejected: a distributed cache inside someone else's JVM,
  competing with the data node's heap and circuit breakers, for the same result
  the ingester nodes give with no new tier.
- **Naive redirect-on-cache-miss.** Rejected: the fetch that would have populated
  the shared cache instead populates a node-local one, so every node misses —
  reproducing the $3,732/month design exactly.

Chosen: **$25/month, 0.33 MiB/s per node.**

## Consequences

- **Read cost stops depending on the flush interval** ($25/month at 250 ms vs
  $3/month at 2 s), so the interval can be chosen for latency. Keep 250 ms.
- The plugin needs **no cloud SDK, no object-store credentials, no block cache**
  competing with the data node's heap — a large packaging simplification.
- The ingester is now on the read critical path. It is stateless and HA, and an
  outage causes **indexing lag, not data loss**; the fallback ladder stays.
- New serving capacity to size (~33 MiB/s in and out per pod at 100 MiB/s).
- Makes placement-aware segment assembly unnecessary (Q18 closed).
- Requires the trust-domain rule: never co-bundle segments across clusters.
