# 0073. The proxy route is a whole-segment GET on the consumer's own endpoint

Status: accepted
Date: 2026-09-27
Requirements: FR-6, NFR-4, NFR-5
Research: docs/research/30-design-space/10-client-library-and-fetch-modes.md §1, §5, §7

## Context

FR-6 names three fetch modes and ADR-0004 makes `proxy` the default for a
high fan-out read: a same-AZ ingester streams the segment through, so an AZ
costs one store GET per segment however many OpenSearch nodes read it. Since M5
the ingester CHOOSES `PROXY` (`FetchPolicy.modeFor` answers it for any batch
above the 256 KiB inline cap that is not sent `direct`), but no route serves
the bytes: the subscription event carries only coordinates, and
`ConsumerClient.bytesOf` returned the event's empty inline array for anything
but `DIRECT`. M9 measured NFR-5 on the transports that existed and recorded the
proxy payload term — the largest one, cost.md rules 10–11 — as NOT-RUN.

Three shapes were available for the missing route, and the numbers that decide
between them are research 10's: the proxy penalty is ~0.5 ms when streamed and
~6 ms per 8 MiB segment when buffered then forwarded; and ADR-0044 found that
every degradation path already reads WHOLE objects, while the event's
`byteStart`/`byteLen` are read by nothing.

## Decision

**`GET /seg?key=<segment key>&az=<caller AZ>`, answering the whole segment,
streamed through `SegmentProxy`, from the endpoint the consumer already
subscribes to.**

1. **Whole object.** The body is the segment exactly as stored. The consumer
   decodes it with `SegmentReader` and finds its run from the directory, which
   is how every other path reads (ADR-0044). One node reads many shards of one
   segment, so the plugin fetches it once per node and decodes each shard's run
   from the same bytes (cost rule 5).
2. **Streamed, never buffered then forwarded.** The route hands the response
   stream to `SegmentProxy` as a sink: a cache hit is written from memory, a
   miss is one store GET relayed chunk by chunk while it fills the node-wide
   `SegmentCache` — the same fill the prefetcher and the subscription path use.
   A route fetch after a prefetch costs zero store requests ONLY on the pod
   that prefetched, which is the AZ's ring owner (ADR-0066); on any other pod
   of that AZ the first fetch is one cold GET. Concurrent cold fetches of one
   key JOIN a single in-flight read (M5.63, pulled into M10 for this route),
   so the bound is **≤ 1 GET per segment per serving ingester**, concurrency
   included. ⚠️ **THE ONE EXCEPTION TO "STREAMED":** a joiner of an in-flight
   read is handed the complete bytes when the winner's read finishes, so the
   K−1 concurrent callers pay research 10's buffered penalty (~6 ms per 8 MiB)
   to save K−1 store GETs. The hold is bounded by the cache's admission
   ceiling; a segment larger than that is read by each caller, streamed.
3. **The key is checked before any I/O.** It must parse as a data
   `SegmentKey` under this deployment's own trust-domain prefix. Anything else —
   a commit-log key, a lease, another domain's prefix, a traversal — is `400`
   with no store request. The route is therefore not a general object reader,
   which is what would make an unauthenticated route (the subscription routes
   are unauthenticated too, M5) a data-exfiltration primitive for control-plane
   objects. A key that parses but is not stored is `404`.
4. **Counted where it is sent, by the caller's word for its AZ.** Bytes written
   are added to `PROXY_READ` against the `az` parameter, the same unverified,
   accounting-only label the poll uses; absent is counted cross-AZ and
   unattributed rather than dropped.
5. **The consumer's own endpoint.** The consumer fetches from the ingester it is
   configured with — in production the same-AZ service — through
   `SegmentSource.fetchSegment(key)`. The event may come from any pod; the
   payload comes from the consumer's zone.

## Alternatives considered

- **A ranged read of the run only** (`byteStart`/`byteLen`). Rejected: per-run
  ranges are what ADR-0004 prices at $1,492,992/month when the plugin does them
  against S3; against the ingester they cost no money but multiply requests per
  node by the ~400 streams a node hosts, and they need the two-request
  header-then-slice read M5.66 describes because the codec lives in the
  directory. Whole-object is one request per node per segment.
- **Buffer then forward.** Rejected on research 10 §1's numbers: +6 ms per
  8 MiB segment and a full segment of heap per concurrent fetch, against
  ~0.5 ms and 64 KiB chunks streamed.
- **Carry the payload in the subscription event** (i.e. raise the inline cap).
  Rejected: an event is per stream, so the same segment bytes would cross the
  wire once per subscribed shard — the ~400× amplification ADR-0004 exists to
  avoid — and a cross-AZ subscription would carry payload cross-AZ.
- **A peer-to-peer fetch between ingesters.** Rejected by ADR-0012 already: a
  cross-AZ peer fetch is 419× a GET. The prefetcher warms each AZ's ring owner
  from the store instead.
- **Authenticating the route.** Deferred, not rejected: no consumer route is
  authenticated today, and a route that can only return data segments of its
  own domain exposes nothing the subscription route does not already announce
  the key of. Closing it is security.md's trust-domain work, not this route's.

## Consequences

- `proxy` is executable end to end, and NFR-5's largest term becomes
  measurable (M10.4).
- A consumer with no `SegmentSource` now fails loudly on a `PROXY` event
  instead of decoding an empty array.
- The route is a second reader of `SegmentCache`. M5.63 (concurrent misses)
  closes with it; M5.64 (a short read that does not throw is cached) applies
  to it unchanged, and the consumer's `SegmentReader` refuses such a segment by
  its footer rather than indexing it.
- **An unauthenticated route that reads the store can be made to spend
  requests.** A well-formed key under this domain's prefix that does not exist
  costs one GET and one STAT; the route remembers the last 1,024 absent keys, so
  a consumer retrying a deleted segment costs nothing further, but a caller
  inventing a fresh key per request buys two store requests each. That is a
  request rate scaling with inbound requests rather than segments, reachable
  only from inside the network the ingester serves; closing it is the same
  authentication work deferred in the last alternative above. ⚠️ The memory
  of absent keys is itself an exposure: a caller that asked for a key BEFORE
  its segment was stored would have it remembered absent, and a consumer
  asking afterwards answered `404` until 1,024 newer absences evict it. A
  consumer only learns a key from an event sent after the segment is durable,
  and a key carries a millisecond timestamp, a sequence, a header length and a
  filter, so the key would have to be predicted; accepted on that basis, and
  closed by the same authentication.
- Serving capacity is ADR-0004's figure: ~33 MiB/s in and out per pod at
  100 MiB/s ingest, now actually exercised.
