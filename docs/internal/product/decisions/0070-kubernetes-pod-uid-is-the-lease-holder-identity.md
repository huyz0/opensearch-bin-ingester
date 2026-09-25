# 0070. Kubernetes Pod UID is the lease-holder identity

Status: accepted
Date: 2026-09-26
Requirements: FR-11, NFR-9
Research: docs/research/30-design-space/03-metadata-and-cas.md §4

## Context

The EndpointSlice watch supplies a pod name, UID, and address. Names and
addresses are both reusable across pod incarnations; transferring “seen ready”
from a dead pod to its replacement can make the replacement's unready lease
look abandoned and trigger an early challenge. The lease is already read and
conditionally written for each acquisition/renewal transition, so carrying an
identity adds no request. A Kubernetes UID is 36 ASCII characters, adding 54
bytes to the existing lease JSON.

M9.52 already committed the read-side capability: it accepts legacy
and UID-bearing lease bytes while preserving legacy writes. The reader release
must be fleet-wide before deploying this writer change.

## Decision

Persist `holderPodUid` in every newly acquired or taken-over production lease.
Match the holder in `EndpointSliceView` by `targetRef.uid` only. A lease without
the field remains readable but supplies no early-challenge evidence and waits
for its expiry. The normal and GC lease writers carry the same configured pod
UID.

## Alternatives considered

- **Match by address:** rejected because an address can be reassigned. A
  replacement can inherit the dead holder's ready history and be challenged
  before its own readiness passes.
- **Match by pod name:** rejected because StatefulSet replacements retain the
  same pod name, so the name does not identify an incarnation.
- **Disable early challenge for all holders:** safe, but gives up the existing
  sub-TTL failover path even when UID evidence is available. The UID field adds
  54 bytes to a lease write and no request, whereas disabling the watch makes
  failover wait for the configured lease TTL.

## Consequences

Old and new lease JSON remain readable by the reader-first release. UID-less
leases are safe but may fail over more slowly. Kubernetes configuration must
provide `pod.uid` from the Downward API; deployment order remains reader release
first, then writer release. This source change is verified with local fakes;
no real Kubernetes rollout is claimed.
