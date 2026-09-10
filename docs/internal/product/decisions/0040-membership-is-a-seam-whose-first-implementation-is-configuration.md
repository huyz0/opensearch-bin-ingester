# 0040. Membership is a seam whose first implementation is configuration

Status: accepted
Date: 2026-09-10
Requirements: NFR-4, NFR-5
Research: ADR-0012 decides the peer mesh itself — no gossip, membership from a
  Kubernetes `EndpointSlice` watch, placement computed rather than discovered.
  This record does not reopen that; it decides the SEAM, which code-structure.md
  rule 4 requires an ADR for.

## Context

M5.8 needs to answer "which pod in this AZ fetches this segment". The placement
half is arithmetic and needs nothing from the platform. The membership half
needs to know who is running, and ADR-0012 answers that with an `EndpointSlice`
watch — which is a Kubernetes API client, a watch loop, and a reconnect policy.

⚠️ **That watch is M8's, not M5's**, and it was reassigned there with NFR-9 for a
reason recorded in M5's SPEC: a static member list never removes a member, so
the lease challenge NFR-9 bounds would fire on an event this milestone cannot
produce.

So M5 needs placement now and cannot have real membership yet.

## Decision

**`Membership` is a seam. Its only query is AZ-scoped, and it returns
`AzPeers` rather than a list. The implementation that ships with M5 is
`StaticMembership`, built from configuration.**

Three parts, each load-bearing:

- **A seam**, because the thing behind it is I/O and non-negotiable 7 says
  business logic does not touch it. `PeerRing` is pure arithmetic over whatever
  the seam returns, so it is T0-testable with no platform at all.
- **AZ-scoped, returning a type rather than a list.** A `List<Peer>` carries no
  AZ and enforces nothing — review MEASURED that the ring answered an az-b peer
  for an az-a call on 13 of 20 segments when handed a mixed list. `AzPeers`
  refuses a foreign peer at construction, so a MIXED view is unrepresentable.
  ⚠️ **That is not yet ADR-0012's "enforced in code, not just documented"** —
  see the first consequence.
- **Configuration first**, stated rather than hidden. M5's SPEC already says
  "until the `EndpointSlice` watch lands, membership is configuration", the same
  shape as M4 deferring the transport.

## Alternatives considered

**(a) No seam — read the member list from config wherever it is needed.**
Rejected: it makes `PeerRing` depend on configuration loading, and it gives M8
nothing to substitute into. The seam is the whole reason the watch can land
without touching placement.

**(b) A seam returning the whole fleet, with callers filtering by AZ.** Rejected
on ADR-0012's "enforced in code": every caller then has to remember, and the one
that forgets costs 419x the GET it replaces. The filter belongs behind the seam.

**(c) Ship the `EndpointSlice` watch now.** Rejected: it is a platform
dependency with its own failure modes, and NFR-9 — the requirement that makes it
urgent — is M8's. Building it here would mean testing a watch loop against a
membership signal nothing in M5 consumes.

## Consequences

**A static list never removes a member.** A pod that dies stays in every ring
until configuration changes, so its share of fetches falls through to the object
store — ADR-0012's last ladder rung, correct and one extra GET. Nothing breaks;
it costs money slowly, which is the right failure direction and is why NFR-9's
challenge path is not met until M8.

**`StaticMembership` is both the shipping implementation and the T0 fake.**
code-structure.md rule 5 wants a fake in the same commit; here they are the same
class, because configuration IS the implementation until M8. When the watch
lands it becomes a fake proper, and that is the moment rule 5 starts to bite.

⚠️ **A CROSS-AZ FETCH IS STILL ADDRESSABLE, and this record must not be read as
closing that.** Nothing at this layer knows the LOCAL pod's AZ, so a caller in
az-a can ask for `inAz("az-b")` and get a usable view — review MEASURED 20 of
20 segments nameable that way. `AzPeers` stops AZs being MIXED, not a caller
naming the wrong one. **M5.9 owns the rest**, at the fetch path, and it needs a
notion of "me" that does not exist here. Anyone reaching this record from the
seam register and closing M5.9 on the strength of it would be closing it on a
property the code does not have; the fetch path would then compile
`ownerOf(seg, membership.inAz(writerAz))` at 419× a GET.

⚠️ **A DUPLICATE `podId` IS SETTLED BY THE RING, NOT REFUSED BY THE SEAM**, and
M8's `EndpointSlice` author needs to know it. Two entries sharing a podId are a
normal transient — `podShortId` is stable across a restart (ADR-0036) under a
StatefulSet (ADR-0031), so a rolling restart shows the old address beside the
new one until reconciliation finishes. `AzPeers` accepts that; `PeerRing`
settles it with a total order on `(podId, endpoint)`. An earlier version refused
it at construction and review measured the cost: `inAz` threw on every call,
taking out the whole AZ's ring including peers that were not duplicated —
turning a disagreement that ADR-0012 says costs an extra GET into a hard
failure. **So an implementation of this seam must not reject duplicates
either.**

⚠️ **The implementation is RENDEZVOUS where ADR-0012, `architecture.md` and M5's
SPEC all say "consistent-hash ring".** Both are in that family and both give
"removing a member moves only what it owned"; rendezvous needs no replica count
to tune. The substitution is recorded here because the class comment saying so
is not where a reader of the seam register arrives.

⚠️ **The score is a value two processes must AGREE on.** Changing CRC32C or the
avalanche constants re-owns every segment fleet-wide, so during a rolling
restart old and new pods disagree and each pair double-fetches. Bounded and
self-healing — ADR-0012's stale-view rule is that it costs GETs, never
correctness — but it makes the hash a compatibility surface, not an
implementation detail.

⚠️ **This seam is not in `architecture.md`'s list until it is added there in this
commit.** The list is the register; an ADR nobody can find from the seam list is
half a decision.
