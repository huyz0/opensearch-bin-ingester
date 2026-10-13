# 0086. Split flushes ride in the checkpoint

Status: accepted
Date: 2026-10-14
Requirements: FR-17
Research: docs/research/30-design-space/03-metadata-and-cas.md §9 (checkpoints and bounded recovery)

## Context

ADR-0081 §8 commits a switch's paused run in a later delta than the rest of
its segment. One flush's segment then has its runs in two deltas, sometimes
more. The idempotency window (ADR-0036) answers a retry of that flush:

- **In the process that committed it**, M13.34b keeps each recent flush's
  runs with the delta carrying each, and answers from all of them.
- **After a crash takeover**, M13.34v seeds the same map from the deltas the
  successor's replay read.
- **After a graceful handover**, the leader's close writes a checkpoint
  (`CheckpointWriter.checkpointAndClose`). The successor's replay starts
  past both deltas. It knows the flush only by `Checkpoint.PodState`'s one
  pointer, which names one delta. M13.34v refuses such a retry rather than
  answer it from one delta with offsets for only some of its runs. Refused,
  it is never answered: the producer's write fails although it is durable.

A graceful handover is the common leader change (a rolling deploy), and a
switch is the common source of split flushes. Each one is rare, but they
coincide on every deploy that lands during a switch.

## Decision

A checkpoint carries each pointed pod slot's recent **split flushes**: a flush
whose segment was committed across more than one delta, as `(flushSeq,
segmentKey, [deltas])`, the deltas ascending, at least two.

- **Format.** Checkpoint version 3, a superset of version 2: streams carry
  the per-stream watermark flag and pods the v1 slot. After a pointed slot's
  pointer comes a uvarint count of split flushes. Each one is: flushSeq
  uvarint, segment key (uvarint length, UTF-8), a uvarint count of deltas,
  and each delta's epoch and sequence as uvarints. They are sorted by
  flushSeq, then key. Every count is bounded by the bytes left. A split of
  one delta, a descending list, a split on a bare slot, and a flush listed
  twice are refused.
- **Chosen by content.** A checkpoint with no split flush encodes as v2, v1 or
  v0 did, byte for byte. A cluster that never switches never writes v3.
- **Reader.** `IdempotencyWindow.seed` keeps each slot's split flushes.
  `resolveSplit` reads a flush's deltas once, one GET each, when a retry
  first asks for it. It is called from `answer`, `answerByDelta` and the
  leader's commit answer. From then on the flush is answered as one this
  process committed. Every merge of two slots of one incarnation keeps both
  slots' split flushes (`PodState.withSplitsOf`): in the replay, and in the
  successor's merge of its own and its predecessor's slots.
- **Rollout.** The reader lands first (M13.34w), with the format, its golden
  file and this ADR. The checkpoint writer starts writing split flushes in a
  later commit (M13.34x), once every pod reads v3.

## Alternatives considered

- **Every run of every recent flush in the checkpoint.** It would answer
  every retry with no GET. It is rejected because a flush's segment spans
  every partition written in that flush. At 64 retained flushes per
  incarnation and hundreds of runs per segment, the checkpoint would grow
  with partitions times pods. Today it is about 38 KiB for 1,600 streams
  (research 03 §9). Only split flushes need more than the pointer, and they
  are a few per switch.
- **Scan forward from the pointer** until every asked run is found, with no
  format change. Rejected: ADR-0036 bounds a replay's answer at one GET per
  distinct delta it names. A scan's GETs grow with the deltas committed
  between the two halves of a split, unbounded under load. It would also
  need a stopping rule it cannot know.
- **Refuse, as M13.34v does, and accept it.** Rejected for the graceful
  handover above: a durable write reported failed on every deploy during a
  switch, retried by the producer, and so written twice. That is the
  duplicate ADR-0036 exists to prevent.
- **Never split a segment.** That means holding the whole request until the
  switch, as M13.34r did. Rejected by ADR-0081 §8 itself: "No other stream's
  acks wait."

## Consequences

- A graceful handover answers a split flush's retry from every delta,
  for one GET per listed delta the first time it is asked. ⚠️ Only while
  the flush is among its incarnation's newest `RETAINED_FLUSHES` (64) the
  window keeps: an older one's GETs are paid and the retry is still refused,
  as any retry that old is. M13.34x writes only those within the window.
- A checkpoint grows only by its split flushes: about 20 bytes plus the key
  per split. They are pruned with the window's retained flushes once
  M13.34x writes them.
- A v3 object written while a pod still runs the previous release would be
  refused by that pod (an unknown version). The rollout order above is what
  prevents that, so M13.34x must not ship in the same release as M13.34w.
- Golden files: `checkpoint-v3.bin`. The v0 to v2 goldens are unchanged and
  still decode.
