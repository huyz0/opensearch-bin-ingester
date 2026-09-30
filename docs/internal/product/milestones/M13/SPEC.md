# M13 — Fast mode (FR-17), after M12's harvest

## Completion condition

The roadmap's M13 row, restated: **an index with `wal=true` is acked and its
records reach its consumers, on any pod and whichever pods wrote them, within
NFR-15's bound (ack and visibility p99 < 10 ms), before their segment is in the
object store; every acked fast record appears later in a
committed segment at the offset it was acked with (invariant I6), through the
death of the writing pod or of the sequencer leader while a quorum of its copies
survives; the default path is unchanged for every index with `wal=false`; and
every item of M12's review harvest is a closed M13 row or dropped with its reason
below.**

M13 collects the obligations M12 handed it:

| Obligation | Assigned by |
|---|---|
| Open rows M12.27, M12.28 | M12/VERIFIED.md § Rows carried open |
| Harvest R1, R2, R3, R4, R5, R6, R7, R8, R9, R10, R11, R12, R13, R14, R15, R16, R17, R18 (R19 dropped below) | M12/VERIFIED.md § Harvest for the specification of M13 |
| The research corpus proposals (five documents) | M12/VERIFIED.md § Research corpus |
| The re-plan: R1, M12.28, R2, M12.27, then R4, R5, then fast mode | M12/VERIFIED.md § Re-plan |
| Fast mode (FR-17) | roadmap; ADR-0013 |

## Requirements

- **FR-17** fast mode: opt-in per index; ack and visibility in ~1.5–6 ms;
  `flush_timer`, `wal`, `wal_quorum` set in OpenSearch and pushed by the plugin.
- **NFR-15** fast-mode ack-and-visible latency, p99 < 10 ms (ADR-0013).
- **NFR-8** becomes conditional (ADR-0013 Consequences): an acked fast record
  lives on `wal_quorum` pods until its segment is committed.
- **NFR-1, NFR-4, NFR-5** (cost, request rates, cross-AZ bytes) and
  non-negotiable 6: fast mode adds cross-AZ bytes, never a request rate that
  scales with records, shards, partitions or indices.
- **FR-6, FR-10** (consumers, catch-up), **FR-15, FR-19** (ingest seams),
  **FR-21** (cost attribution) for the harvest rows.

## Scope

1. M12's re-plan rows first: the write-path split (R1), the harness port race
   (M12.28), `PartitionVisibilityIT` as a distribution (R2), the reader
   connection on close (M12.27), ADR-0078's amendment (R4), the silent defaults
   (R5).
2. The rest of M12's harvest, R3 and R6–R18.
3. Fast mode (FR-17), **specified by M13.41**, which adds its design, cost,
   criteria, test plan, risks and tasks (M13.22-M13.38) to this SPEC. M13.0
   was split from it at its third review round (review.md rule 12): three
   rounds of the fast-mode design found fourteen majors, and the harvest
   half had none.

### Not in scope

- **R19, the commit hooks.** Whether to install `.pre-commit-config.yaml`'s
  hooks is the owner's decision. Until then every M13 commit runs `./gradlew
  gates checkReviewed checkTdd checkTestIntegrity checkCommitMessage
  -PcommitMessageFile=<file>` by hand, and its body says so.
- **Fast mode's own scope** -- whether ADR-0080's compact event and the own-zone
  relay (H16) are weighed in it as M12's re-plan assigned, a per-lane ack mode,
  and on which rig its latency is measured -- is M13.41's to decide.
- **AWS S3 latency, TTFB and billed dollars**, NOT-RUN since M9.

## Design

### The harvest

Each harvest row is decomposed below with the finding it closes, as M12 did;
the design of each is the finding's own fix. Two carry design weight:

- **R1, the split.** `DefaultIngest` has 8 lines of headroom and `Assembly` 6,
  and fast mode lands in both. The split extracts the durable flush path from
  `DefaultIngest` (the `FlushCoordinator` wiring and the settlements) and the
  sequencer and serving construction from `Assembly`, so that fast mode adds a
  sibling rather than growing either file. `ConsumerClient` (606) goes back under
  a named ceiling, and `LocalSequencer` (700, which fast mode touches) is split
  too. The ceiling gate holds `DefaultIngest` and `Assembly` at 500 and the other
  two at 600.
- **R5, the silent defaults.** The consumer's fetch policy stops having a
  clock-less default in production code: `SegmentFetchRetry.DEFAULT` is removed
  from main sources and every production caller passes a clock; the two
  `ConsumerDeliveryQueues` and `IndexQuotas` default constructors go.

### Fast mode

Specified by M13.41 (see § Scope).

## Cost impact

- **The harvest rows:** none on the request path. R3 and R8 correct what the
  cost report says, not what it costs; R7 gates the test fixture's memory.
- **Fast mode:** stated by M13.41.

## Acceptance criteria

1. **The write path is split** (R1): `DefaultIngest` and `Assembly` are each
   **below 500 lines** -- the headroom fast mode needs, not merely the 600 they
   already meet -- and `ConsumerClient` and `LocalSequencer` below 600, all four
   pinned by the ceiling gate at those numbers, and `./gradlew test` is green
   after each split.
2. **The harness binds its own port** (M12.28): `NodeProcess` passes port 0 to
   the child and reads back the port it bound; pinned by a test.
3. **`PartitionVisibilityIT` is measured as a distribution** (R2): at least ten
   runs on M9's rig, `trigger202` and `drainEnd` recorded per run, the RustFS
   memory plateau measured or OBSERVED-NOT, variant B completed, the ~1.03 s
   trigger write investigated and its cause named or OBSERVED-NOT, and `trigger202
   ≤ drainEnd` asserted by the IT.
4. **A reader's connection closes with its subscription** (M12.27): decided and
   either fixed and pinned or refuted with its measurement.
5. **ADR-0078 matches M12.4** (R4) and **no production code carries a silent
   default** (R5): `SegmentFetchRetry.DEFAULT` and the default constructors of
   `ConsumerDeliveryQueues` and `IndexQuotas` are gone from main sources, and the
   plugin's started client receives the clocked policy.
6. **Every other harvest row (R3, R6–R18) has a disposition**, and each of the
   five research-corpus proposals is made or dropped with its reason (M13.21),
   enumerated once each in VERIFIED.md at close, as M12's criterion 17 was;
   `checkMilestoneVerified` refuses an enumeration missing a harvest ID listed
   individually in this SPEC's obligations table (M13.40).
Criteria 7 onward are fast mode's, added by M13.41.

## Test plan

| Criterion | Tier | First failing test | Mutation it must kill |
|---|---|---|---|
| 1 | T0 (gate) | `FileSizeCeilingTest` cases: `DefaultIngest` and `Assembly` refused at 500, `ConsumerClient` and `LocalSequencer` at 600 | M13.1 leaving `DefaultIngest` and `Assembly` untouched; a ceiling of 700 for the other two |
| 2 | T1 | `NodeProcessPortTest` | the probe-then-hand-over port |
| 3 | T3 | `PartitionVisibilityIT` (measurement) and its `trigger202 ≤ drainEnd` assertion | `trigger202` timed after the drain; the evidence line cites the measurement file's per-run table, at least ten rows, or is NOT-RUN |
| 4 | T1 | `SubscriptionReaderConnectionTest` close case, or the refutation's measurement | the reader client not closed |
| 5 | T0/T1 | `SilentDefaultsGoneTest` (reflection) and `FetchBackoffClockWiringTest`'s started-client case | any of the three defaults left in main; a client built on a clock-less policy |
| 6 | T0 (gate) | `MilestoneEvidenceTest` harvest-enumeration case (M13.40) | a harvest ID missing from the enumeration |

Fast mode's test plan is added by M13.41.

## Risks

- **R2 may not name the trigger write's cause.** Then criterion 3 records it
  OBSERVED-NOT with the variants measured; the bound is never moved.
- **M12.27 may be a JDK behaviour, not ours.** Then criterion 4 is met by its
  refutation, with the measurement.
- **Fast mode's risks** are stated by M13.41.

## Decisions

- ADR-0078 amended (R4).
- Fast mode's decisions are M13.41's.

## Tasks

| ID | Task | Serves |
|---|---|---|
| M13.0 | Specify M13 | — |
| M13.1 | R1: split `DefaultIngest` and `Assembly` below 500 along the write path, `ConsumerClient` and `LocalSequencer` below 600, the gate at those numbers | — (quality) |
| M13.2 | M12.28: `NodeProcess` binds port 0 and reports it | — (quality) |
| M13.3 | R2: `PartitionVisibilityIT` as a distribution | — (evidence) |
| M13.4 | M12.27: the reader connection on close | FR-6 |
| M13.5 | R4: ADR-0078 amended for M12.4 | — (docs) |
| M13.6 | R5: no silent defaults; the started client's clock pinned | FR-6, FR-19 |
| M13.7 | R3: M11.5 T2 (the top-K log's prices) and the catch-up GET's share | FR-21 |
| M13.8 | R6: quarantine the legacy buildSrc failures | — (harness) |
| M13.9 | R7: gate the compose `mem_limit`; build.md's script references | — (harness) |
| M13.10 | R8: the top-K line's residue | FR-21 |
| M13.11 | R9: the explicit-wait residue | FR-13 |
| M13.12 | R10: two nodes' jitter and the cross-lane attempts pinned | FR-6 |
| M13.13 | R11: two test doubles in production's buffered order | — (quality) |
| M13.14 | R12: admission and quota residue pins | — (quality) |
| M13.15 | R13: behavioural gate-wiring pins; the dump property in every task | — (harness) |
| M13.16 | R14: `PushQueue` abandonment exposed and pinned | FR-5 |
| M13.17 | R15: `ConsumerClient`'s gap report returned; `expiredOr` pinned | FR-6 |
| M13.18 | R16: two stale javadocs | — (docs) |
| M13.19 | R17: `checkCommitMessage` requires a `Cost:` line; `PeerCommitTest` | — (harness) |
| M13.20 | R18: a shared `Ingest` test base | — (quality) |
| M13.21 | The research corpus updates M12 proposed (research 12's banner is M13.41's) | — (docs) |
| M13.40 | `checkMilestoneVerified` refuses a VERIFIED.md enumeration missing a harvest ID its SPEC lists | — (harness) |
| M13.41 | Specify fast mode: its design, the protocol's obligations, cost, criteria 7 onward, test plan, risks and tasks M13.22-M13.38 | FR-17 |
| M13.39 | Close M13 | — (evidence) |

M13.22-M13.38 are fast mode's IDs, reserved here and defined by M13.41.
