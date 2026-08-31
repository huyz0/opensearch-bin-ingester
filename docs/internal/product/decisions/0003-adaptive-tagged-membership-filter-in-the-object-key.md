# 0003. An adaptive tagged membership filter in the object key, not a Bloom filter

Status: accepted
Date: 2026-08-29
Requirements: FR-10, NFR-12
Research: docs/research/30-design-space/02-partition-bloom-in-key.md

## Context

Encoding which streams a segment contains into its key lets LIST-based recovery
skip irrelevant objects for free — ~80× cheaper than reading 1,000 headers. The
original design said "a Bloom filter of the partitions". At the real target scale
(10,000 indices, ~120,000 streams) the arithmetic does not survive.

## Decision

A **tagged, adaptive filter**: `A` (all), `N` (none), `Z` (exact bitmap), `R`
(run-length bitmap), `B` (Bloom). The writer emits the shortest encoding that
fits ~900 characters, degrading to `N`. The filter operates on **index ordinals**,
with the exact stream directory in the segment header one GET away.

An unknown tag and `N` both mean *read the header* — **never** *no match*.

## Alternatives considered

- **A fixed stream-level Bloom filter.** Rejected by computation: over ~120,000
  streams an exact bitmap needs 19,984 characters and a 10%-FPR Bloom over 5,000
  streams needs 3,994 — against a 1,024-byte key limit. It does not fit at any
  useful FPR.
- **A fixed exact bitmap.** Rejected for the same universe; but it *wins
  outright* when the prefix scopes the universe to one index (2,000 shards =
  334 characters, zero false positives), which is why the tag exists.
- **Hash-bucket bitmap (Bloom with k=1), avoiding the ordinal registry.**
  Rejected: 25.6% FPR at the sizes we need.
- **Per-index prefixes so LIST is a seek.** Rejected as the default: each prefix
  needs its own object stream, so 64 buckets cost ~$10,000/month in PUTs.

## Consequences

- The ordinal registry becomes **required**, not optional.
- The filter is a recovery-path optimisation only, so a 5–20% FPR is acceptable
  and short keys beat low FPR.
- Establishes the general rule: **the prefix sets the universe, and the universe
  decides the encoding.** Prefix design and filter design are one decision.
