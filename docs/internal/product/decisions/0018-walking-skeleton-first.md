# 0018. Walking skeleton before depth

Status: accepted
Date: 2026-08-30
Requirements: all of M1
Research: docs/research/20-opensearch/02-poller-semantics-and-cost.md

## Context

The roadmap built bottom-up — store SPI, format, ingest, sequencer, subscription,
plugin — with the first searchable document around M6.

⚠️ **The riskiest assumptions in the whole design are the OpenSearch ones, and
every one of them comes from reading source rather than running it:**

- that `readNext` may block for `pollTimeout`, which is what makes zero idle cost
  possible ([poller-semantics §3](../../../research/20-opensearch/02-poller-semantics-and-cost.md));
- that a single `long` pointer satisfies the SPI across restart and reset;
- that a node-level singleton can be shared by every shard's consumer;
- that `RawPayloadIngestionMessageMapper` really does break the routing invariant
  the way ADR-0006 concludes.

Bottom-up leaves all four unvalidated for months, above which a great deal would
be built.

## Decision

**M1 is a walking skeleton**: producer → ingester → a durable object on local FS →
consumer → plugin → **a document searchable in a single-node OpenSearch test.**

Stubbed to the minimum: one writer, no lanes, no WAL, no compaction, no key
filter, an in-process sequencer, `LocalFsBinStore` only. Then each layer is
deepened against a system that already works end to end.

The cost meter lands in M1 too, so every later milestone is measured against a
real baseline rather than a modelled one.

## Alternatives considered

- **Bottom-up, as the roadmap had it.** Stronger foundations, no throwaway work,
  and each module gets a real test surface immediately — a genuine fit with the
  TDD and coverage discipline. Rejected because it defers the four unvalidated
  OpenSearch assumptions to month four, and a wrong one invalidates work above it.
- **Skeleton through the ingester only**, stopping before the plugin. Rejected:
  it validates the write path, which is the part we are *most* confident about,
  and skips the part we are least confident about.

## Consequences

- The store SPI is built once, inside M1 (tasks M1.1-M1.3); M2 is the segment
  format, not a second SPI. **Accepted
  deliberately** as the price of early validation.
- ⚠️ Some M1 scaffolding is throwaway. It must be *labelled* throwaway, and the
  milestone review must check it was actually replaced rather than quietly kept.
- The TDD discipline is unchanged: the skeleton is still test-first, still
  reviewed by both agents, still coverage- and mutation-gated. **A skeleton is not
  an excuse for untested code**; it is a narrower slice of the same bar.
- If an OpenSearch assumption proves wrong, it surfaces in week 2 with almost
  nothing built on top of it. That is the entire point.
