# 0007. One sequencer slot by default, with the slot dimension present from day one

Status: accepted
Date: 2026-08-30
Requirements: FR-11, NFR-9
Research: docs/research/30-design-space/03-metadata-and-cas.md §4, §6, §12 (Q2)

## Context

Streams hash to slots; each slot has a leased sequencer. The design allowed
S=64. More slots means more parallelism and a smaller blast radius per failure,
but more leases, more chains, slot→chain resolution on every reader, and a
failover storm after an AZ loss.

The throughput question is answerable: Scenario B is 12 segments/s × ~5,000 runs
= **~60,000 offset assignments/s**. An assignment is a hash-map lookup and an
add. A single serial thread does that comfortably — it is nowhere near the
bottleneck, which is the commit PUT.

## Decision

**`S = 1` by default.** One cluster-wide sequencer lease, one commit chain.

The **slot dimension stays present in every path and every key** — lease paths,
chain paths, commit records — so raising `S` later is a configuration change, not
a format change.

## Alternatives considered

- **S = 64 from the start.** Rejected: 64 leases to renew, 64 chains for a reader
  to resolve, and a 64-way takeover storm after an AZ loss, to solve a throughput
  problem that does not exist at 60,000 assignments/s. Complexity with no
  measured need.
- **A slot per index (10,000 slots).** Rejected: commit rate would scale with
  index count, violating cost rule R6, and it is the mistake §6 of the research
  document exists to prevent.
- **Omit slots entirely and add them later.** Rejected: retrofitting a dimension
  into persisted key paths is a format migration
  (`wire-format-change`), which is exactly the kind of change we pay most for.

## Consequences

- Blast radius of a sequencer loss is the **whole cluster's visibility**, for one
  lease failover. NFR-9 targets < 5 s, met by early challenge on evidence of
  death; ~10 s worst case on TTL expiry. Acceptable because it is a *visibility*
  stall, not a write outage — producers keep writing and the data is durable.
- Commit PUT rate becomes `1 sequencer × flush rate`, the minimum possible.
- Raise `S` when measurement shows the sequencer saturating or the failover stall
  exceeding NFR-9 — not before.
