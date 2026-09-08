# M4 — VERIFIED

One line per acceptance criterion in [SPEC.md](SPEC.md), naming the test or
command that demonstrated it. Anything not actually observed says **NOT-RUN** or
**OBSERVED-NOT** rather than claiming a pass.

Measured on 2026-09-08 on this machine, at `GATE_SCOPE=full`:
**954 tests, 0 failures, 0 skipped** — 945 across T0/T1/T2 (`./gradlew test`)
and **9 inside a real OpenSearch node** (`./gradlew :plugin:clusterTest`).

⚠️ **Read the two limits below before reading the criteria.** Both were true of the
milestone as specified and remain true now; neither is discovered here.

- **A multi-pod deployment is not yet correct.** M4 ships the LOCAL `Sequencer`
  only. Commit forwarding — the RPC by which a non-leaseholder pod reaches the
  one pod holding the lease — is M5's, with the peer mesh. The simulation
  compensates by driving many logical pods through the seam without a network,
  so multi-pod ordering, idempotency and fencing are proven; what is deferred is
  the transport, not the correctness argument. SPEC § *Deployment constraint*.
- **NFR-9's < 5 s failover is not met by M4 alone**, and criterion 8 says so.
  Failover here is TTL-bound (~10 s worst case per ADR-0007). The early-challenge
  path that closes it needs the same membership signal as the peer mesh, so it
  goes with it in M5.

1. **I1–I5 across 1,000 seeds** — `CommitProtocolSweepTest.theInvariantsHoldAcrossEverySeed` — **1,000 seeds, 85,451 readers judged, 22.2 s against a 60 s budget**. All seven fault classes the SPEC names are live: `FaultInjectingStore.Faults(0.03, 0.03, 0.1, 0.02, 0.02, 0.02)` — unreachable, ambiguous, duplicated, withheld, partitioned leaders and deferred writes/reordered completions — plus the injectable `SimulatedClock` and 3 logical pods. ⚠️ **12 of the 1,000 seeds commit nothing and therefore hold I1–I5 vacuously**; the count is asserted, not assumed. Per-class evidence: `FaultClassEvidenceTest`
2. **Exactly one winner under contention** — `LeaseManagerConcurrencyTest`, `LeaseManagerTest`, `IsOwnTermMatrixTest` (negative-control matrix, one case per path × conjunct)
3. **Both seal branches, asserted separately** — `CommitLogSealTest`, `CommitLogSealWriteTest`, `LocalSequencerSealRaceTest`, `LocalSequencerAncestorSealTest` — the fenced leader stops; the lease-holding new leader **redrives**
4. **Offsets stable across failover (NFR-11, I2)** — `LocalSequencerFailoverTest`, `CommitLogCrossEpochTest`, `ReaderInvariantsTest`; end-to-end in a real node by `OffsetMonotonicityIT` and `RestartResumeIT`
5. **Zero `list` calls, ≤ K deltas replayed** — `CheckpointDiscoveryTest`, `BoundedRecoveryCorrectnessTest`, `LocalSequencerBoundedRecoveryTest`, asserted through `CountingBinStore`
6. **Commit idempotent on `(podId, incarnationId, flushSeq)`** — `SequencerDedupTest`, `CommitProtocolSimulationFlushSeqTest`, `CommitLogAttributionTest` (ADR-0036)
7. **I5 enforced, and its violation detectable** — `AckTraceStoreTest` + `CommitProtocolSweepTest`. ⚠️ **This is the criterion M4 previously failed while reporting a pass** — see below
8. **Failover time measured and labelled** — `LocalSequencerRenewTest`, `LocalSequencerFailoverTest` on the simulated clock. ⚠️ **A MODELLED number, not a measurement**, and **not** NFR-9's < 5 s; falsified by a real-clock chaos run at M8
9. **Commit cost does not scale with pods or streams** — `BatchingSequencerTest`, `CommitLogBatchTest`, `IngestCommitPathTest` via `CountingBinStore`
10. **Compaction-trigger observable** — `CompactionObservableTest`, fed from `CommitLog.apply` and re-seeded from the replay on `recover`, so the distribution describes the LOG rather than process uptime. Exponential buckets with range-only keys plus a bounded top-K; `check-metric-cardinality.sh` **ok, no forbidden metric labels**, i.e. no `stream` label
11. **`SEAL`/`CONTINUE` round-trip, v0 still parses** — `format` golden files — `GoldenAttributedTest`, `AttributedDeltaTest`, `BatchedCommitDeltaTest`, `CheckpointPodStateTest` (ADR-0028, 0032, 0033)
12. **`sequencer` compiles with no Helidon** — `GATE_SCOPE=full ./scripts/check-module.sh` → **ok, 8 module(s) checked**

## Criterion 7 is the one worth reading

M4's completion condition was previously claimed with I5's ack-ordering clause
**asserted but not testable**, and I5 is the invariant the research corpus
singles out as *"the one a plausible implementation violates by accident"*.

The simulation built both halves of the acknowledgement trace from one
`commit()` return, adjacently:

```java
CommitDelta acked = leader.commit(req);
acks.add(AckEvent.confirmed(epoch, acked.sequence()));
acks.add(AckEvent.acked(epoch, acked.sequence()));
```

The confirmation therefore preceded the acknowledgement **by construction**, so
`checkAckOrder` could not fail however the writer behaved. A writer acking
window N+1 before window N confirmed left the whole 1,000-seed sweep green.

The confirmation now comes from the **store**, which sees every PUT in real
completion order and has no view of what the writer intends to tell its caller
(`AckTraceStore`, below the fault injector so that a landed-but-lost-response
write still confirms). **Verified falsifiable:** mutating the leader to
acknowledge `sequence() + 1` turns the sweep red. Under the old trace the same
mutation was invisible.

## What changed to make the fault model complete

Three of the seven classes the SPEC requires were absent or inert:

- **`withheldPut` was implemented, unit-tested, and set to `0`** in the sweep's
  profile — the one class modelling "the response was lost and *nothing*
  landed". Now enabled at 0.05, and `FaultClassEvidenceTest` refuses a zero rate
  for any class.
- **Partitioned leaders did not exist as a fault.** `unreachable` is a per-call
  coin flip with no notion of *which* pod is calling, so it can never cut one pod
  off while another stays connected — the shape a lease fight takes. Pod identity
  is now threaded into the injector; a partition is a **state with a duration**,
  not a rate. Measured: 6,224 partition faults over 120 seeds at rate 0.2,
  commits moving 789 → 437, no invariant violated.
- **Delayed writes and reordered completions were unmodelled**, because every
  other class resolves inside the call that triggered it and the driver had
  nowhere to hold a pending operation. `deferredPut` holds the write and lands it
  at the round boundary in a seed-chosen order — one mechanism, both classes.

⚠️ **`duplicatePut` changes no outcome in any seed, and that is correct.** It
fired in 40 of 40 and moved nothing, because `putIfAbsent` is write-once and a
duplicated in-flight PUT *must* be absorbed. Its evidence is therefore stronger
rather than weaker: `aDUPLICATEDWriteReachesTheStoreAndIsAbsorbed` demands both
that the duplicate reached the store and that the resulting chain is
byte-identical to the clean run's. Drop either half and the test passes for a
system that stopped absorbing duplicates.

## Known limits, stated rather than buried

- **Zombie writes remain 0 across the sweep.** A fenced leader is sealed before
  it can commit (ADR-0037), so the zombie population I5's ack clause is *most*
  about is not exercised — the clause is proven on the leader path instead. This
  is the system behaving correctly, not the test failing to look; it is recorded
  because a reader could otherwise mistake the zombie code path for coverage.
- **The mutation gate is not wired on this branch.** `check-mutants.sh` does not
  exist here; every mutation quoted above was run by hand against a baseline
  first confirmed green in a materialised staged tree. ⚠️ That last part is not
  ceremony: an earlier attempt was run against a *working* tree that did not
  compile, where four mutations read as killed for the wrong reason. **NOT-RUN**
  as a gate.
- **Four of the five gap-closing commits were reviewed by `tools/xreview`**, an
  external reviewer that shares no code with the harness being replaced. It
  returned 2 blocking and 12 major findings across them, every one measured and
  every one fixed. The fifth (M4.13b) exceeded the 32 KB packet cap, which
  refuses rather than truncating, and says so in its commit body.
- **`check-coverage.sh` was not run.** Line/branch floors are unmeasured for this
  milestone. **NOT-RUN**.
