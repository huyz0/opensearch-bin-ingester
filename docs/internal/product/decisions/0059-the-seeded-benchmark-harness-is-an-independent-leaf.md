# 0059. The seeded benchmark harness is an independent leaf

Status: accepted
Date: 2026-09-20
Requirements: NFR-1, NFR-7
Research: docs/research/40-implementation/03-benchmarking-plan.md#2-tier-2--macro-harness

## Context

M9.3 needs a reproducible workload with named document-size bands, stream
cardinality, and rate pacing. The generator is benchmark tooling, not an
ingestion runtime component. Putting it in a production module would make the
benchmark depend on runtime assembly choices and would allow a future macro
harness to leak benchmark-only dependencies into the OpenSearch plugin path.

## Decision

Create a separate `bench` Gradle module. It depends on nothing in the runtime
module graph, and no runtime module may depend on it. The module owns the
seeded load generator, bulk-body value types, and clock/sleeper pacing seams.
`check-module.sh` enforces `server` and `bench` as leaves.

## Alternatives considered

- **Put the generator in `server`.** Rejected: `server` is the composition-root
  leaf and its runtime classpath would then carry benchmark tooling.
- **Put it in `ingest` or `client`.** Rejected: both are production paths, and
  the benchmark would become an accidental dependency available to shipped
  code.
- **Keep it only in a test source set.** Rejected: M9.4's macro harness needs a
  reusable executable workload generator and pacing seam, not a test-only
  fixture hidden inside one production module.

## Consequences

The benchmark module can evolve independently and cannot add runtime
dependencies by accident. It does add one module to the build and requires the
module gate to maintain its leaf rule. M9.4 may depend on `bench` from a
benchmark runner, but runtime modules must not.
