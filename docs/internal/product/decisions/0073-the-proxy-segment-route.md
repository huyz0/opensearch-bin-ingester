# 0073. The proxy segment route: one GET per segment key, filled once, served in chunks

Status: accepted
Date: 2026-09-26
Requirements: FR-6, NFR-4, NFR-5, NFR-6
Research: docs/research/30-design-space/10-client-library-and-fetch-modes.md#5-the-three-delivery-modes;
  docs/research/30-design-space/05-az-topology-and-data-flow.md#the-miss-ladder

## Context

[ADR-0004](0004-the-service-serves-reads.md) decided that same-AZ ingester
nodes serve reads, streamed through, and FR-6 names `proxy` as one of the
three delivery modes. The subscription event for a `proxy` delivery carries
coordinates and no bytes (`SubscriptionEventValidation` refuses bytes on any
non-inline event). Until M10 no route served those bytes: over HTTP a
consumer handed a `proxy` delivery decoded an empty array and threw. With
the default configuration -- `direct` off, a 256 KiB inline cap -- every
segment above 256 KiB and every cold publish is served `proxy`, so this was
the common path, not an edge.

The route's shape is a subscription-protocol decision (security.md: the
subscription protocol), and its cost is NFR-4's: the reads it causes must scale
with segments and nodes, never with consumers of one segment.

## Decision

**`GET /seg?key=<segmentKey>&az=<consumerAz>` on the ingester node's front
door**, served by `SegmentService` (`http`) over `SegmentReads` (`ingest`).

1. **Addressable keys.** The key must parse as a canonical `SegmentKey`
   whose prefix is this deployment's own prefix, and be at most
   `SegmentKey.MAX_KEY_BYTES`. Anything else -- commit log, leases,
   checkpoints, `ctl/`, traversal, another deployment's prefix in a shared
   bucket -- is `400` before any store request, and the refusal does not echo
   the key. The prefix check is needed because `SegmentKey.parse` accepts
   any prefix.
2. **Trust.** A deployment is one trust domain ([ADR-0010](0010-multi-tenancy-and-security-model.md),
   security.md rule 1); a consumer that can reach `/sub` can already subscribe
   to any of its indices, so serving any of its segments adds no exposure.
   The route shares the listener and whatever fronts it.
3. **Single-flight fill, then chunked serve.** On a cache miss the first read
   takes a per-key gate and fills the node's bounded `SegmentCache` from the
   store (one GET, streamed into the cache, no consumer attached); concurrent
   reads of that key wait on the gate; then every read streams from the cache
   in `SegmentProxy`'s 64 KiB chunks. **The gate never spans a consumer's
   socket.**
4. **Uncacheable segments.** A capacity-0 cache is never filled: one GET per
   read. A segment above the cache's ceiling is discovered by filling it once;
   its key is remembered in a bounded memory (256 keys) and later reads stream
   straight from the store, so K reads cost at most K + 1 GETs.
5. **Status before the first byte.** A store failure is `503`, a missing
   object included, because the SPI's `get` throws a plain `IOException` for
   both. After the first chunk the status is committed and a failure
   truncates the chunked body, which the consumer's client reports as an error
   and `SegmentReader.open`'s footer check refuses regardless.
6. **Counted.** Every body byte is counted once as `PROXY_READ` against the
   consumer's self-reported `az`, the value the poll already uses for
   accounting. It never routes; an absent zone counts as unknown, which the
   counter treats as cross-AZ.

## Alternatives considered

- **Stream through on the miss, holding the gate while the first consumer
  reads** -- what ADR-0004's "streamed through" suggests literally. Rejected:
  a consumer at a zero TCP window holds the gate, and every other consumer of
  that segment on the node waits behind it (the failure `SegmentProxy`'s
  javadoc names for a blocking sink). What the chosen design gives up is
  first-byte latency on a cold miss: the whole store read (~80 ms for 8 MiB at
  100 MB/s) rather than one chunk (~0.6 ms). On the tail path the miss is
  rare -- the writer caches its held bytes (M10.15) and each non-writing AZ's
  ring owner is prefetched (M8.56) -- and ADR-0004's constraint that actually
  binds, memory independent of fan-out, holds: one cached copy per node.
- **No single-flight: `streamTo` per request.** Rejected on NFR-4: N consumer
  nodes arriving together on a cold segment cost N GETs, a rate in consumers
  of one segment. The cache would admit N copies' worth of reads for one kept.
- **Carry the segment on the poll answer.** That is `inline`, capped because
  the answer is held whole. An 8 MiB segment there breaks NFR-6 and the
  64 MiB per-node queue budget.
- **Serve a byte range per run.** [ADR-0044](0044-no-production-fetcher-in-m5-and-the-coordinates-stay.md):
  nothing the consumer holds names the object length, and `SegmentReader.open`
  needs the footer; per-run ranged reads were priced at $1.49M/month in
  ADR-0004. Whole object only, coalesced on the consumer node.
- **Reuse the catch-up POST.** It answers a replay request with inline event
  frames from `DurableCatchUpResponder`'s own store GET, bypassing the cache;
  the live tail through it would cost a GET per request.
- **A `HEAD` to tell 404 from 503.** Rejected: a request per failure buys an
  operator a clearer status and changes nothing a consumer does -- both mean
  "this segment is not available from this node now".

## Consequences

- `proxy` works over HTTP; M10.2 gives the consumer the fetch.
- Per segment: 0 GETs on the writer (with M10.15), 1 on each non-writing AZ's
  prefetched ring owner, and at most 1 cold GET on any other serving node
  that misses -- M9 criterion 5(a)'s budget. The peer hop that would remove
  the last term remains out of scope.
- ⚠️ **The single-flight is the route's own.** Prefetch, the cold publish and
  the route each read through `SegmentProxy`, and the cache admits only after
  a read completes, so a read landing during another path's in-flight read of
  the same segment buys its own GET. That is inside 5(a)'s cold term; M10.5
  reports how often it happens. Moving single-flight into `SegmentProxy` so
  all three share it is the fix if the rate turns out to matter.
- The route adds ingester-served HTTP requests, one per (consumer node,
  segment) once M10.2 coalesces -- not object-store requests.
- A cold miss's first byte waits for the whole store read. Not measured on
  this rig; M10.5 records serve latency beside its byte counts.
