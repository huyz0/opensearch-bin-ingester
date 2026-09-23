**Status:** stable   **Confidence:** high   **Last updated:** 2026-09-23
**Read this if:** Implementing the peer prefetch signal or changing its bytes.
**One-line takeaway:** Send a versioned key-only hint to one deterministic cache owner per remote AZ; authenticate the direct socket against the ready EndpointSlice and treat every rejection or loss as a cache miss.

# Durable segment signal

## Purpose and correctness boundary

The signal tells one cache owner in each other availability zone that a
committed segment is available. It carries no segment bytes, offsets, or commit
decision. A signal may be lost, delayed, duplicated, stale, refused, or
unreadable without changing correctness: the consumer's existing object-store
fallback still reads the segment. The sender runs only after the segment and
its commit are durable.

This is a directed hint, not gossip. The sender selects at most one endpoint
from each remote AZ using the same deterministic rendezvous rule used by the
local cache ring. Fan-out therefore scales with AZs, not peers, streams,
partitions, or records. A receiver warms its cache at most once per remembered
key; duplicate hints do not cause duplicate GETs.

## Frame version 1

All integers in the fixed header are big-endian. The frame is:

| Offset / field | Encoding | Bound |
|---|---|---:|
| Magic | 4 bytes, ASCII `BPDS` | exactly 4 |
| Version | unsigned 32-bit integer, value `1` | exactly 4 |
| Writer pod ID | unsigned LEB128 byte length, then strict UTF-8 | 1–256 bytes |
| Writer AZ | unsigned LEB128 byte length, then strict UTF-8 | 1–256 bytes |
| Segment object key | unsigned LEB128 byte length, then strict UTF-8 | 1–1,024 bytes |

The maximum is **1,550 bytes**: 8 fixed bytes plus three two-byte length
prefixes and the maximum 1,536 field bytes. Fields must be non-empty and
well-formed UTF-8. The decoder rejects a bad magic, unknown version, zero or
over-limit length, truncation, malformed UTF-8, trailing bytes, and a frame
larger than the total bound. It never skips an unknown version.

The v1 golden bytes for `(pod, az, k)` are
`425044530000000103706f6402617a016b`. The wire record and decoder are in the
`format` module so the network adapter does not define a second encoding.

## Receiver authentication and handling

The HTTP receiver accepts only the bounded binary frame. Before invoking the
prefetch handler (which may GET the key), it requires all of the following:

1. The request's direct socket peer address is present in the current ready,
   non-terminating EndpointSlice snapshot.
2. Exactly one such endpoint matches, and its pod ID and AZ equal the frame's
   writer ID and AZ.
3. The pod ID embedded in the segment-key grammar equals the claimed writer ID.

Forwarded-address headers are not authentication. A proxy or NAT that hides the
direct pod address causes a safe refusal and loses only the optimization; the
object-store path remains correct. No rejection path invokes the handler.

## Compatibility and rollout

This endpoint and its v1 reader/writer are introduced together; no earlier
release sends this frame. During a rolling upgrade, an old pod may answer 404
because it has no route. The sender treats that as a missed hint and continues
the durable write. New readers reject unknown frame versions rather than
guessing. There is no stored or replayed frame: the signal is best-effort and
ephemeral, so no old byte shape needs retention support.

## Cost boundary

Each selected target receives one frame body, counted under
`DURABLE_SEGMENT_SIGNAL` in `CrossAzBytes`, including same-AZ deliveries in the
same/same-AZ columns and remote-AZ deliveries in cross-AZ totals. The count is
the request body, matching the existing peer transport counters; it excludes
HTTP headers and TCP/TLS framing. At the default 8 MiB segment size, two
maximum-sized remote copies are 3,100 / 8,388,608 = **0.03696%** (reported as
0.037%) of producer bytes. They consume part of, not an addition to, NFR-5's
less-than-0.1% total across every measured transport.

The signal adds no object-store request. Its receiver may cause one existing
prefetch GET at the selected cache owner per remote AZ. Duplicate keys are
suppressed by `SegmentPrefetcher`; ordinary reads still fall back to the object
store when a signal or peer is absent. See [ADR-0066](../../internal/product/decisions/0066-versioned-key-only-durable-segment-hints.md).
