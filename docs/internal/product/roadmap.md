# Roadmap

Milestones in execution order. Each has a completion condition that is checkable.
Only the current milestone is decomposed in `backlog.md`.

| # | Milestone | Completion condition |
|---|---|---|
| **M-1** | **Agent harness** | `AGENTS.md`, skills, adapters and the gate scripts exist and are wired in `.pre-commit-config.yaml` (AGENTS.md § Gates lists them, generated); `pre-commit run --all-files` is green. ✅ **complete** |
| **M0** | Scaffold | Gradle build, **JDK 25** toolchain (OpenSearch 3.8 bundles 25.0.3+9, so one toolchain serves both), module skeleton, CI running the gates, coverage and mutation gates wired |
| **M1** | **Walking skeleton** | ⚠️ Re-scoped by [ADR-0018](decisions/0018-walking-skeleton-first.md): a document written as `_bulk` is **searchable in a single-node OpenSearch test**, having travelled producer → ingester → local-FS segment → consumer → plugin, and an idle cluster issues **zero** object-store requests. ⚠️ **The zero is NOT provable as written — see the M1 SPEC completion condition and M1.16**: it holds by construction (no class on the consumer path holds a `BinStore`), so no consumer test can make it fail. The store SPI, both backends and `CountingBinStore` are the first tasks inside it, not a milestone of their own. Decomposed in [milestones/M1/SPEC.md](milestones/M1/SPEC.md) |
| **M2** | Segment format | Encode/decode round-trips, golden files, key grammar with the adaptive filter, NFR-12 held for pathological input |
| **M3** | Ingest path | Helidon endpoint accepts framed writes, bundles, flushes at 8 MiB **or the adaptive interval** ([ADR-0017](decisions/0017-every-pod-writes.md) — a flat 250 ms is the rejected operating point, at 20× the cost), memory flat under 10× request size (NFR-6) |
| **M4** | Sequencer and commit log | CAS leases, epoch fencing, write-once deltas, checkpoints; **I1–I5** hold across 1,000 simulation seeds. ✅ **complete** — [milestones/M4/VERIFIED.md](milestones/M4/VERIFIED.md), one evidence line per acceptance criterion. ⚠️ **I1–I5, not I1–I4**: [architecture.md](architecture.md) defines exactly five, and I5 — "the one a plausible implementation violates by accident" — is the clause this milestone had to make *testable* rather than merely assert. ⚠️ **Two limits carried forward, both specified rather than discovered**: a multi-pod deployment is not yet correct, because commit forwarding is M5's; and NFR-9's < 5 s failover is not met here, because failover is TTL-bound until M5's early-challenge path |
| **M5** | Subscription and client | Push channel with session/epoch, **all three** fetch modes (M1 ships `inline` only), prefetch on commit; zero idle requests proven at fan-out. ⚠️ **NOT "re-proven"**: the M1 baseline this once presupposed was never established — see the M1 row above and M1.16. M5 is where the property first becomes assertable, because a consumer that fetches has a store to not-fetch from |
| **M6** | OpenSearch plugin | ⚠️ M1 already makes documents searchable and survives restart at skeleton scope. M6 is what the skeleton deferred: the `IngestionMessageMapper` surface, `os_routing` and aliases (ADR-0015), error and back-pressure paths, and a multi-node cluster |
| **M7** | Retention, GC, watermarks | Watermark feed, the retention rule, GC from the commit log; no unread data ever deleted |
| **M8** | Resilience | Chaos matrix passes, including `SIGSTOP` gray failure and AZ partition; RPO 0 demonstrated |
| **M9** | Cost and performance proof | S3/MinIO benchmarks; the cost/latency curve published; NFR-1/4/5 measured, not modelled |

## Deferred into a later milestone

| Obligation | Receiving milestone |
|---|---|
| Compaction (FR-14) | after M9 — deliberately deferred, see `docs/research/30-design-space/06-compaction-and-retention.md` §1 |
| Fast mode (FR-17) | **M11, after the default path is measured.** A second write path with its own failure modes should not be built alongside the first |
| Priority lanes (FR-18) | M10 — but the `i8 lane` field lands in the wire format at M3, since adding it later is a format change |
| ~~Multi-tenant isolation and quotas (Q9)~~ | **Closed** by [ADR-0010](decisions/0010-multi-tenancy-and-security-model.md) and `docs/research/30-design-space/11-multi-tenancy-and-security.md`, both in this commit. Promotion to a dedicated prefix stays deferred with a stated threshold |
| ~~Complete security model (Q10)~~ | **Closed** by the same ADR. What remains deferred is *enforcement*: quotas land with the governor, and the trust-domain seam ships in M1.0 |
| GCS and Azure backends | M1 ships the SPI and two backends; the cloud ones follow once the conformance suite exists |
| A lone producer capped at ~44 KB/s once the interval lengthens ([ADR-0026](decisions/0026-a-lone-producers-throughput-scales-inversely-with-the-interval.md)) | **After M3, milestone not yet assigned.** `BulkService` appends in blocking 1000-record chunks, so one producer advances one chunk per flush and its throughput is `chunkBytes / interval`. ⚠️ NOT idle-pod-only: `fillRatio <= 0.4` at the 250ms floor is any pod under ~13.4 MB/s, and M3's own criterion-4 worked load (1 MiB/s per pod) settles at the ceiling by design. Accepted for M3; the two options ADR-0026 records and rejected only as out-of-scope (a blocked-producer input to the control loop; pipelined chunk appends) each need their own measurement first |
