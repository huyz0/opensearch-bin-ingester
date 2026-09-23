# 0066. Versioned key-only durable-segment hints

Status: accepted  
Date: 2026-09-23  
Requirements: FR-10, FR-12, NFR-4, NFR-5  
Research: [durable segment signal](../../../research/40-implementation/04-durable-segment-signal.md), [ADR-0012](0012-peer-mesh-without-gossip.md)

## Context

The per-AZ cache reduces object-store reads, but an idle cache owner cannot know
that another AZ has committed a segment without a signal. The existing data
path must remain correct when a signal is lost or a peer is unreachable. The
existing cost rule allows request rates to scale with segments, AZs, and nodes,
never records, partitions, shards, or indices.

The default segment is 8 MiB. A complete signal can be bounded to 1,550 bytes:
8 header bytes, 256-byte writer ID, 256-byte AZ, a 1,024-byte maximum object
key, and three two-byte length prefixes. Sending it to the owner in each of
two other AZs is at most 3,100 bytes or 0.03696% of producer bytes. NFR-5 still
caps all named cross-AZ transports together at less than 0.1%.

Source-address authentication assumes direct pod-to-pod sockets. A sidecar,
NAT, or proxy can obscure that address; refusal is safe and falls back to an
object-store read, while trusting a forwarded header would let the caller
assert another pod's identity.

## Decision

After durability, send a v1 binary frame containing only writer pod ID, writer
AZ, and segment key to at most one deterministic rendezvous-ring owner in each
other AZ. Version 1 uses a fixed `BPDS` magic and big-endian version, followed
by three bounded unsigned-LEB128-length-prefixed UTF-8 fields. The total frame
is at most 1,550 bytes.

The receiver must bind the claimed writer ID and AZ to the direct socket source
address in the current ready EndpointSlice view, require a unique matching
endpoint, and verify that the segment-key's embedded pod ID matches. It rejects
before any cache handler or store GET otherwise. Forwarded headers do not
participate. Unknown versions and malformed frames are rejected, not guessed.
Missing, rejected, stale, or failed hints never affect commit or read
correctness; the object-store fallback remains authoritative.

Every frame body is counted in `CrossAzBytes` as `DURABLE_SEGMENT_SIGNAL`;
request headers and network framing are excluded, consistently with the other
peer-body counters. The signal is an additional measured term within NFR-5,
not a relaxed budget. It does not add an object-store request by itself.

The endpoint, reader, writer, fake, golden bytes, research note, and this ADR
travel together. Older peers have no route and may answer 404; the best-effort
sender drops that outcome without retrying the durable write. No signal is
persisted or replayed, so there are no old frames in storage to migrate.

## Alternatives considered

- **Broadcast to every ready peer.** A 1,550-byte frame sent to 300 pods would
  be 465,000 bytes per segment (5.54% of an 8 MiB segment) and would scale with
  node count. Selecting one owner per remote AZ bounds it to 3,100 bytes in the
  three-AZ deployment (0.03696%).
- **Send the segment or a cache line across AZs.** This duplicates up to 8 MiB
  of data across zones to avoid an object-store GET; ADR-0012's measured
  cross-AZ data economics make that the wrong direction. The hint carries no
  data bytes, and the selected owner uses the existing read path.
- **Use the signal as a commit notice or offset authority.** That would put a
  correctness fact on a best-effort path. The commit log remains the only
  authority; the frame names an object key solely to warm a cache.
- **Authenticate with a forwarded header or a shared secret in the frame.** A
  forwarded header is caller-controlled, while a shared secret adds a second
  credential distribution and rotation path. The direct socket address,
  current ready membership, claimed AZ/ID, and key's embedded writer ID are
  checked together; deployments that obscure the source address lose only the
  optimization.
- **Use an unversioned JSON body.** It has larger and less precisely bounded
  encoding, permits parser-specific ambiguity, and makes future decoding less
  explicit. The bounded binary frame has a golden representation and strict
  unknown-version refusal.

## Consequences

The prefetch hint is compact, deterministic, and bounded by AZ count; its bytes
are visible in the same cost report as every other peer transport. The direct
socket requirement may disable hints behind a proxy, producing extra cold
reads but not incorrect reads. Version 1 cannot be extended by changing field
meaning: any incompatible frame requires a new version and compatible reader
rollout. Signal delivery is still not an exactly-once guarantee, and the
consumer must keep its existing fallback.
