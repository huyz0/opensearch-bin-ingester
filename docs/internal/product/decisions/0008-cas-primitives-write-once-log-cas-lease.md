# 0008. Write-once for the commit log, compare-and-swap for the lease and registry

Status: accepted
Date: 2026-08-30
Requirements: FR-8, FR-11
Research: docs/research/30-design-space/03-metadata-and-cas.md §2, §5; 07-pluggable-store-abstraction.md §6 (Q5)

## Context

The store SPI offers two conditional primitives: `putIfAbsent`
(`If-None-Match: *` / `x-goog-if-generation-match: 0`) and `putIfMatch`
(`If-Match: <etag>` / `x-goog-if-generation-match: <gen>`).

The commit log is write-once by design. Only two objects need mutation: the
sequencer **lease** and the **ordinal registry**. If those could also be
expressed as write-once sequences, `Capabilities` would require only
`putIfAbsent`, widening backend support to stores with weaker conditional writes.

## Decision

**Use the primitive that matches the access pattern, not one primitive
everywhere.**

| Object | Primitive | Rate |
|---|---|---|
| Commit-log delta, checkpoint, seal | `putIfAbsent` | 4–12/s, contention-sensitive |
| Sequencer lease (acquire, renew, release) | `putIfMatch` | ~0.33/s, single writer |
| Ordinal registry | `putIfMatch` | ~0/s, only on index creation |

`Capabilities.conditionalWrites` requires **both**, checked at startup and failed
loudly.

## Alternatives considered

- **`putIfAbsent` only, with a write-once lease-claim chain.** Genuinely
  workable: a claim is `ctl/lease/<slot>/<epoch>.claim`, renewals are
  `<epoch>.renew.<n>`, and a challenger probes forward to find the current epoch
  and the latest renewal. Rejected: it replaces one CAS on one object with a
  probe-forward protocol and two object families, to remove a primitive that
  S3, GCS and Azure all support natively. **The reason to avoid read-modify-write
  is contention at rate** — which applies to the commit log and not to an object
  rewritten every three seconds by a single writer. Recorded here because it is
  the fallback if a required backend ever lacks `putIfMatch`.
- **`putIfMatch` everywhere, including the commit log.** Rejected in ADR-0002 on
  Quickwit's published experience and on contention mechanics.

## Addendum 2026-08-30: MinIO is a fixture, not a backend

Development runs against **MinIO in Docker** to avoid paying for S3 on every test;
production is **S3**. MinIO is a *test double for the S3 wire protocol* and is
**not a supported backend**, so this ADR's primitives are not constrained by it.

⚠️ **Its conditional writes are not stable enough to test the commit protocol
against.** That simulation runs on `MemoryBinStore`, which we control and can make
S3-exact. MinIO covers signing, ranges, multipart, listing and error mapping.
See [store SPI §2b](../../../research/30-design-space/07-pluggable-store-abstraction.md).

`Capabilities.conditionalWrites` is still probed rather than inferred — that is
correct for any endpoint and is what makes a misconfigured production store fail
loudly instead of silently.

## Consequences

- Two primitives in the SPI, each used where it is cheapest to reason about.
- The conformance suite must exercise both on every backend, including the
  409/412 distinction and ETag-vs-generation semantics.
- A backend supporting only `putIfAbsent` is not supported today, and the
  migration path is written down above rather than rediscovered.
