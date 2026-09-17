<!-- SPDX-License-Identifier: Apache-2.0 -->
# 0051. The plugin CAN read the committed pointer, and reporting it moves the error to the SAFE side rather than removing it

Status: accepted
Date: 2026-09-17
Requirements: FR-9, NFR-13
Amends: [ADR-0005](0005-no-consumer-offset-store.md) § Consequences
Measurement: M6 (and M5's bound) — `CommitIntervalProbeIT`, M7.14

## Context

ADR-0005 records that a consumer watermark is **optimistic**: OpenSearch's
`IngestionEngine.getIngestionState()` reports `streamPoller.getBatchStartPointer()`
— the **in-memory** pointer — while only `lastCommittedBatchStartPointer`
survives a crash, and "OpenSearch does not expose the committed one". Everything
downstream is sized against that gap: `safetyMargin` exists to cover it, and
50-open-questions §3 lists measurement **M6** — whether the plugin can observe
the committed pointer — as the thing that would shrink it.

M7.14 measured it.

## What was measured

`CommitIntervalProbeIT`, one shard on a real single-node cluster, reading the
shard's **last Lucene commit user data** through
`IndexShard.acquireLastIndexCommit`:

```
commitUserDataKeys=[max_seq_no, batch_start, max_unsafe_auto_id_timestamp,
                    translog_uuid, history_uuid, local_checkpoint,
                    min_retained_seq_no]
batchStartAtCreation=Optional[0000000000000000000]
batchStart=Optional[0000000000000000059]
naturalWaitsMillis=[10027, 10025, 10027]
recordsIndexed=60
```

Two facts, and they point in opposite directions:

1. **`batch_start` IS in the commit, and it MOVES.** `IngestionEngine` writes
   `StreamPoller.BATCH_START` into the same `writer.commit()` as the documents,
   and a plugin running **inside the node** reads it back: `0` at shard creation,
   `59` after sixty records. It is not exposed by an API — ADR-0005's statement
   is true of the ingestion-state API — but the plugin is not an API client.
   ⚠️ **THE KEY ALONE WOULD HAVE PROVED NOTHING**, and the first version of the
   probe asserted exactly that: an empty commit written at shard creation
   already carries `batch_start=0` beside `max_seq_no=-1`, so review measured the
   assertion passing with the flush removed and sixty records live and
   unaccounted for.
2. **The pointer does not advance on its own on any timescale a test will
   wait.** Three consecutive 10-second observations saw no advance at all, with
   sixty records already indexed and searchable; the second commit had to be
   FORCED with an explicit flush. A Lucene commit is driven by the **translog
   flush policy** — 512 MB or 30 minutes by default — not by ingestion.

## Decision

### 1. The reporter reads the COMMITTED pointer, not the in-memory one

`ProgressReporter.Positions`' production implementation (M7.17) reads
`batch_start` from the shard's last commit. What it reports is then a position
that a crash cannot walk back, which is the property ADR-0005 wanted and
believed unavailable.

### 2. It moves the error to the SAFE side; it does not remove it

⚠️ **AN EARLIER DRAFT OF THIS SECTION CLAIMED THE MARGIN NOW COVERS ONLY THE
REPORT INTERVAL, AND REVIEW REFUTED IT WITH FACT 2.** Sixty records were live
and searchable while the committed pointer still read `0`, for more than thirty
seconds. Reporting the committed pointer does not shrink the staleness to the
reporting interval: the whole flush window is still in it. What changes is the
DIRECTION of the error:

- The in-memory pointer is **ahead** of what a restart would resume from. A
  watermark built on it is optimistic, and the margin exists to stop GC deleting
  records the restart re-reads. Too small a margin **loses data**.
- The committed pointer is **behind** what the shard has actually indexed. A
  watermark built on it is pessimistic, so GC keeps longer than it must. Too
  small a margin costs **storage**, never records.

That is the whole argument: the same uncertainty, on the side where being wrong
is a bill rather than an incident.

⚠️ **AND IT HAS A REAL COST, WHICH THIS ADR STATES RATHER THAN DISCOVERS
LATER**: a LOW-RATE stream may not fill a translog flush for a long time, so its
committed pointer barely moves, its watermark barely moves, and its data is kept
until `maxRetention` — where the ceiling deletes it and ALARMS. On such a stream
the alarm is not an incident but the normal end of the retention window, and an
operator reading M7.13's alarms needs to know which of the two they are looking
at.

### 3. And it stays non-zero

A zero margin would assume the frame in flight is instantaneous and that no
report is ever lost. Both are false — a failed push is not retried (M7.3, by
design, because the next interval supersedes it), so two intervals can pass
between reports. ⚠️ Under §2 the margin's job is smaller than it was, because
the pointer it guards is already on the safe side; it is kept because a report
in flight is still a report nobody has yet acted on.

## Alternatives considered

- **Keep reporting the in-memory pointer.** Rejected: it is strictly worse
  information for the same cost, and the margin sized for it has to cover a
  translog flush window nobody controls.
- **Poll `GetIngestionStateAction`.** Still rejected, for ADR-0005's original
  reasons, and it returns the in-memory value anyway.
- **Set `safetyMargin = 0` once the committed pointer is reported.** Rejected —
  §3. ⚠️ Note that the FIRST version of this ADR would have made that alternative
  look reasonable, which is why §2 was rewritten rather than softened.
- **Force a flush to make the pointer advance.** Rejected: it would trade a
  retention margin for write amplification on every shard, which is a far larger
  cost than keeping a few thousand extra records.

## Consequences

- ⚠️ **`RetentionRule.DEFAULT_SAFETY_MARGIN` is 10,000 records and that is an
  UPPER BOUND ON A GUESS, not a measurement of production.** The probe runs one
  shard, indexes sixty records and observes the pointer advance by 59 offsets
  across the only two commits it sees — one of them forced. What that
  establishes is that the mechanism works, not what a flush window holds at
  Scenario A's rate. ⚠️ **AND THE PROBE'S OWN ASSERTION IS AGAINST THAT SMALL
  NUMBER**, so it would pass at a margin of 61: it pins the mechanism, never the
  constant.
- ⚠️ **M7.17 becomes a bigger task than "find a handle".** It now has a
  specified source — the commit's user data — and the reason to prefer it.
- ⚠️ **The probe is a T4 and `./gradlew build` does not run `clusterTest`**
  (M6.20), so this measurement is not re-taken by the default suite. A change in
  OpenSearch's commit-data keys would be found by running the probe, not by CI.
