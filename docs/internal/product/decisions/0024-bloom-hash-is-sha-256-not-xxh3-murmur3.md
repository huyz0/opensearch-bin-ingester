# 0024. The Bloom filter's hash is SHA-256, not xxh3-128/murmur3-128

Status: accepted
Date: 2026-09-02
Requirements: NFR-12
Research: docs/research/30-design-space/02-partition-bloom-in-key.md §7 (overturned)

## Context

Research doc 02 §7 names xxh3-128 or murmur3-128 for the Bloom filter's one
128-bit hash, split into `h1, h2` for the Kirsch-Mitzenmacher construction
(`h_i = h1 + i·h2 (mod m)`), and points to a benchmark against a blocked Bloom
(doc 40-implementation/03 §3, B6) as the reason.

M2.5 builds that construction (`MembershipFilter.Bloom.of`,
`Bloom.mightContain`). Neither xxh3 nor murmur3 ships in the JDK; using either
means hand-porting a hash algorithm this codebase does not otherwise need, with
no reference test vectors on hand to verify the port against — real
correctness risk for a component whose only job is to be uniform and
deterministic.

## Decision

**SHA-256** (`java.security.MessageDigest`, JDK-native), taking the digest's
first 16 bytes as two independent 64-bit longs `h1, h2`. Already used elsewhere
in this codebase for the same kind of purpose (`LocalFsBinStore`'s key hashing),
so this adds no new algorithm to trust or maintain.

The benchmark research doc 02 §7 cites is about **hot-path hashing
throughput** — many hashes per second on a request path. Bloom construction
here runs **once per distinct index per segment flush** (hundreds of ordinals,
not per-record — the same "must not scale with records" framing as
non-negotiable 6), where SHA-256's extra cost per call is immaterial: even at
5,000 distinct indices in one flush (research doc 02 §4's upper bound before
the tag degrades to `N`), this is 5,000 digest computations, not per-record
work.

## Alternatives considered

- **Hand-port xxh3-128 or murmur3-128.** Rejected for now: real implementation
  risk (no reference vectors in this repo to verify a port against) for a
  performance property (throughput) this call site does not need. Left open to
  revisit if a real throughput measurement (`bench` skill) ever shows Bloom
  construction on a hot path, which it is not today.
- **A non-cryptographic 64-bit hash split into two 32-bit halves.** Rejected:
  halves this narrow raise collision-probability concerns for the
  Kirsch-Mitzenmacher construction at this project's stated scale (up to
  10,000 indices), and buys nothing SHA-256 does not already give for free.

## Consequences

- A reader must use the exact same hash algorithm the writer used, or
  `mightContain` silently answers a different question than the writer's
  `of` computed — `h1`/`h2` are pure functions of the digest bytes, so any
  future change to this algorithm is effectively a wire-format change for
  every segment already written with a Bloom filter in its key.
- **Not yet a durable commitment.** M2.6, which wires this filter into
  `SegmentKey` and therefore into segments actually persisted to the object
  store, has not landed as of this ADR. Changing the hash algorithm before
  M2.6 ships costs nothing; changing it after does, per the point above.
  Revisit this decision explicitly if that changes.
- research doc 02 §7 carries a revision banner pointing here.
