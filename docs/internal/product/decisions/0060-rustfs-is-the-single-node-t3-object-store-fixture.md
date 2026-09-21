# 0060. RustFS is the single-node T3 object-store fixture

Status: accepted
Date: 2026-09-21
Requirements: FR-8, NFR-8
Research: docs/research/30-design-space/03-metadata-and-cas.md#2-cas-primitives-per-provider; docs/research/30-design-space/07-pluggable-store-abstraction.md#11-test-strategy

## Context

M9 needs a local S3-compatible endpoint because no AWS account is available.
The fixture must exercise the real HTTP, signing, range, listing, multipart,
presign and conditional-write paths. The commit protocol itself remains on the
deterministic in-memory simulation; a fixture must not become its oracle.

The former MinIO fixture could run the sequential conditional-write cases but
its concurrent conditional writes were unstable, so the conformance suite had
to disable both CAS race cases. That left the evidence for the store primitive
we use for leases and write-once claims incomplete.

RustFS 1.0.0 is pinned by digest in `docker-compose.test.yml`. On 2026-09-21,
the existing RustFS-backed T3 run passed 21 backend integration cases and the
36-case shared conformance suite with both concurrent conditional-write cases
enabled and zero failures. One listing-divergence case remains skipped.

## Decision

Use the pinned RustFS 1.0.0 image as the repository's single-node T3 fixture.
Use its S3 API through the existing AWS SDK adapter and keep the shared
conformance suite as the qualification gate. The concurrent `putIfAbsent` and
`putIfMatch` cases stay enabled.

This qualifies the single endpoint used by the fixture only. It does not claim
atomic conditional writes across multiple RustFS endpoints; a distributed
RustFS deployment needs a separate qualification before it can support the
sequencer protocol.

## Alternatives considered

- **Keep MinIO.** Rejected because its fixture run left both concurrent CAS
  cases disabled after unstable responses, so it could not buy the evidence
  M4's lease and write-once paths need.
- **Use a real S3 account.** Rejected for M9 because no AWS account is
  available; it would also make ordinary T3 runs external, credentialed and
  non-reproducible.
- **Use LocalFsBinStore or MemoryBinStore.** Rejected because neither exercises
  HTTP, SigV4, remote error mapping, ranges, multipart or presigned URLs.
- **Treat single-node RustFS as proof of a multi-endpoint cluster.** Rejected
  because endpoint-distributed conditional-write atomicity is a separate
  deployment property and is not exercised by this fixture.

## Consequences

- T3 now measures request counts and protocol behaviour through RustFS, while
  latency remains a local-loopback lower bound and dollars remain modelled.
- The two concurrent CAS conformance cases are executable evidence rather than
  disabled coverage.
- The fixture is not a production RustFS backend, and the distributed CAS
  qualification remains explicitly open rather than implied by the green run.
