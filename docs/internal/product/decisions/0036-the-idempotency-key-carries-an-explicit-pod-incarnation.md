# 0036. The idempotency key carries an explicit pod incarnation

Status: accepted
Date: 2026-09-07
Requirements: FR-3
Research: docs/research/30-design-space/03-metadata-and-cas.md

## Context

M4.10's row asks for commit idempotency on `(podId, flushSeq)`, and its own text
records why that pair cannot carry it: `DefaultIngest` holds `flushSeq` as an
instance field and `podShortId` is stable, so a pod that dies and comes back
reissues 0, 1, 2 for entirely new segments. Dedup on the bare pair would
**suppress real commits** — worse than the duplication it exists to prevent.

Two repairs were tried and both fail on the same shape: they discriminate a
restart only by something that is not the pod's identity.

**The lease epoch** is the sequencer's term. It separates incarnations only
while producer and sequencer share a process, and stops the moment M5's commit
forwarding lands — so it passes every test M4 can write.

**The segment key** looked better, because `SegmentPublisher` publishes before
the commit and calls its keys "unique WITHOUT coordination". Measured false:
its `timestampMillis` is `Accumulator.firstAppendMillis`, not a commit stamp, so
two publisher instances with the same `podShortId` and *different* records mint
a byte-identical key — 200/200 under a frozen clock. (A further 175/200 under
`Clock.systemUTC()` is a fixture whose appends fall inside one millisecond, not
a production rate; the trigger is a clock that does not advance across a
restart.) That claim reduces to "a pod's wall
clock never regresses across a restart", which is a precondition, not a fact,
and a single pod with a stepped-back clock is *in* protocol.

## Decision

**The idempotency key is `(podId, incarnationId, flushSeq)`, where
`incarnationId` is minted once per INGESTER process — the same object that mints
`flushSeq`, i.e. one per `DefaultIngest` instance — and carried in
`CommitRequest`.**

⚠️ THE MINTING SITE IS PART OF THE DECISION, not an implementation detail. The
producer is the record supplier (glossary.md), and is the wrong noun: it is the
ingester that holds `flushSeq`. Minting per FLUSH instead — inside
`flushLocked` — makes dedup a total no-op in production while every
sequencer-seam test still passes, because those tests supply the incarnation
themselves. A test at the seam cannot see this; `IngestCommitPathTest`'s
recording sequencer can.

A new process is a new incarnation by construction — no clock, no coordination,
and nothing that changes meaning when M5 moves the sequencer out of process.

**A replay is `flushSeq <= lastAppliedFlushSeq` for the same
`(podId, incarnationId)`.** `flushSeq` is dense and monotone within an incarnation over ISSUED flushes --
`DefaultIngest` evaluates `flushSeq++` as the commit argument, so a throw
consumes the number -- and therefore a REFUSAL IS NOT PROOF OF DUPLICATION:
flush 5 failing, 6 applying, and 5 being retried satisfies `5 <= 6` while
nothing was ever applied under 5. M4.10d owns what the caller does with that.
Within that reading, one number gives an **unbounded-depth** window: a replay of any
earlier commit is caught, not merely the pod's most recent one. A batch may
carry several requests from one pod — `commitAll` is the primitive — and `<=`
handles a replay of the lower of them. A *different* `incarnationId` is accepted, which is the case the bare pair got
wrong.

⚠️ A DETECTED REPLAY IS ANSWERED FROM THE POINTED DELTA, OR REFUSED — NEVER
FROM THE WRONG ONE. The slot holds ONE pointer, to the delta that LAST applied,
so a replay of an older `flushSeq` need not be in it: two flushes committed in
separate batches sit in separate deltas. Detection is unbounded-depth;
answering is not. So the rule is a lookup, never an assumption: read the
pointed delta and answer only if it carries that exact
`(podId, incarnationId, flushSeq)`. If it does not, REFUSE with a
distinguishable error. ⚠️ THE POINTED DELTA MUST STILL EXIST: nothing deletes
chain objects before M7, and when M7's retention lands it owes this pointer a
rule, or criterion 6's "ALWAYS answered" stops holding. Returning the pointed delta's offsets would hand back
another flush's `firstOffset` — the invariant M4.46 names, that a committed
offset resolves to the records committed at it.

⚠️ AN INCARNATION ID IS UNORDERED, AND THAT IS A PRICE, NOT A FREE PROPERTY. It
buys independence from any clock and from coordination, and costs the ability
to tell a NEWER incarnation from an OLDER one. The checkpoint holds one slot
per pod, so a commit from a different incarnation replaces it: two incarnations
of one pod interleaving destroys the window WITHIN an incarnation — X commits
`(Ix, 5)`, Y commits `(Iy, 3)` and takes the slot, and X's retry of 5 is then
tested against a foreign watermark and accepted. M4.10d must pin the update
rule and this bound. ⚠️ ADR-0031 DOES NOT COVER IT: it bounds OFFSET
ASSIGNMENT for two live processes sharing a `podId` and says nothing about
idempotency, so it is a precondition here, not an argument.

**`incarnationId` is a VALUE beside `flushSeq`, never a qualifier on the pod
id.** Qualifying the id (`"poda"` -> `"poda@e7"`) adds a permanent `pods` entry
per restart, so the checkpoint grows with history against ADR-0033. Widening the
value stays one entry per pod.

## Which formats change

**Both.** This is the shape M4.10c was decomposed for.

- **`Checkpoint`** — not a `ChainEntry` at all: own MAGIC, own VERSION, no kind
  byte, and ADR-0033 requires it never decode as one. `pods` becomes `podId -> (incarnationId,
  lastAppliedFlushSeq, epoch, sequence)`: the watermark AND a pointer to the
  delta that last applied for that pod. ⚠️ The pointer is normative HERE, not
  only in Consequences; without it answering is bounded to the uncheckpointed
  tail, which is the ground an alternative is rejected on below. `lastAppliedFlushSeq` **survives**; it is the window,
  and ADR-0033's per-pod half is amended rather than dropped.
- **`CommitDelta`** — carries the full triple PER `SegmentCommit`, not once per
  delta, so a successor can rebuild the window from the uncheckpointed tail.
  ⚠️ `podId` IS CARRIED, and an earlier draft dropped it on the ground that
  `segmentKey` already holds `podShortId`. That is a property of ONE CALLER, not
  of the record: `SegmentCommit.segmentKey` is a `String` checked only for
  non-emptiness, `SegmentKey` exposes no parser for the pod, and the golden
  `chain-delta-batched-v1.bin` holds `bins/a.seg` with no pod in it at all.
  Without `podId` on the chain, a window rebuilt from the tail has no key to be
  written back under — `CheckpointWriter` merges on `request.podId()` — so the
  loss is deferred by one checkpoint, not removed.

  ⚠️ THE DELTA'S `podId` IS AUTHORITATIVE for the window; `segmentKey` stays
  opaque to the sequencer. Stating which side wins is what keeps two pod
  attributions on one record from being the hazard `SegmentCommit`'s javadoc
  names.
  ⚠️ The placement is load-bearing. No PRODUCTION delta in M4 is multi-segment,
  so a once-per-delta field is byte-equivalent in production today and drops
  every pod but one on M5's first forwarded multi-pod batch. ⚠️ IT IS NOT
  INVISIBLE TO TESTS, and an earlier draft said it was: `BatchedCommitDeltaTest`
  round-trips a three-segment three-pod delta, `golden/chain-delta-batched-v1.bin`
  carries two distinct segments, and `BatchingSequencerTest` asserts a two-pod
  `commitAll`. ⚠️ WHAT THEY MUST ASSERT, not what data
  they must carry: the PER-SEGMENT triple, after round-trip and after
  `commitAll`, on a fixture whose segments differ in `flushSeq` or
  `incarnationId` — NOT merely in `podId`, which the partial hoist preserves.
  Two requests from ONE pod at `flushSeq` 0 and 1 in a single batch is the
  shape; `commitAll`'s distinct-key rule permits it. ⚠️ NO FIXTURE IN THE TREE
  DOES THIS TODAY: the three-pod case is `new CommitRequest("pod" + pod, 0,
  ...)`, flushSeq 0 for every pod. Two earlier drafts prescribed fixture DATA and were wrong in both
  directions. The mutation none of the three catches today is the PARTIAL hoist
  — `podId` per segment, incarnation and flushSeq per delta — because
  `BatchingSequencerTest` asserts only `hasSize(2)`.

  Nothing on the chain carries `incarnationId` or `flushSeq` today, so without
  this a takeover loses every commit made since the predecessor's last
  checkpoint — which is when duplicates are likeliest.

⚠️ Both delta paths carry the field — `VERSION_DELTA` and the kinded batch, both
or neither — BY A NEW VERSION, not by changing either existing layout.
`GoldenChainEntryTest` asserts `delta.encode()` is BYTE-IDENTICAL to both
goldens, and `ChainEntry` states why: a version IS a layout, and every delta
already in a bucket must stay readable for the retention window without a
rewrite. So a delta carrying no triple must still encode exactly as it does
today. ADR-0028 leaves the version path open, and the kind value ADR-0032 used
had been RESERVED — `ChainEntry` records that reservation consumed, so a new
kind needs reserving rather than assuming. Existing goldens are untouched and
the DISCRIMINATING golden is a NEW file beside `chain-delta-batched-v1.bin` (M3.0: "SegmentReader accepts BOTH versions ... `golden/segment-v0.bin` is
untouched, not regenerated").

## Consequences

**The slot carries a POINTER beside the watermark**, `(epoch, sequence)` of the
delta that last applied for that pod. Without it, detection is unbounded-depth
and answering is bounded to the uncheckpointed tail, because entries below the
newest checkpoint are never GET — which is the very defect that rejects
"bound the window to the tail" below. An earlier draft dropped the pointer on a
size ground that applies to storing a segment KEY (up to `MAX_KEY_BYTES`, 1024)
and not to two longs. The delta it points at carries `SegmentCommit.segmentKey`
and `RunCommit.firstOffset`, an absolute offset (ADR-0001), so one GET answers
a replay THE POINTED DELTA CARRIES. It is not a general answer: the slot holds
one pointer, and a replay below the watermark need not be in it.

**Recovery reads no more than today.** `ChainReplay.applyChain` already LISTs
from the seeded checkpoint and GETs every entry past it, so the tail's pod
attribution arrives in objects the leader was going to read anyway. Entries
below the newest checkpoint are never GET — the `Hop` javadoc says they "are
never GET at all" — which is why the checkpoint carries the pod state rather
than the chain being walked backwards for it.

**No commit-path request scales with pods, on the steady-state path.** The
retry path costs one GET per distinct pointed delta, so pods whose last-applied
commits sit in DIFFERENT objects cost one GET each; that scales with replaying
nodes. ⚠️ NO RULE LICENSES THE POD AXIS — pods appear on neither cost.md's
permitted list nor its forbidden one, and SPEC.md's own commit-cost criterion
forbids it outright — so this is bounded by the coalescing stated here, not
permitted by citation. Detection is an in-memory comparison against state recovered once
per takeover. ANSWERING reads the pointed delta, and the slot is per pod: under
M4.7 a batched delta is ONE object shared by the window, and an ambiguous PUT is
ambiguous for every caller in it at once, so N pods can retry together with N
slots naming the same `(epoch, sequence)`. M4.10d must issue AT MOST ONE GET per
distinct `(epoch, sequence)` per batch; N GETs of one object inside one
`commitAll` is cost.md R5 and R4 on the commit path, on the pod axis M4.7 exists
to remove.

**The checkpoint grows by fixed-width fields per pod.** An `incarnationId` is
16 bytes as a UUID and the pointer two longs, about 32 bytes per pod;
`CheckpointTest` asserts under 64 KiB with a 1,600-stream body already spending
about 38 KiB, and 32 bytes per pod does not
approach the remainder. A segment key would have cost up to `MAX_KEY_BYTES`
(1024) per pod, which is why it is not stored.

**A window recovered from a pre-change checkpoint is empty for those pods**,
which requires `Checkpoint.decode` to accept an ENUMERATED set `{0, 1}`, never
`version <= VERSION` — a range would decode a future v2 body with v1's layout,
and `CheckpointDecodeRefusalTest`'s `bytes[7] = 1` stops being an unknown
version once 1 is current. Today it is a strict equality check, so a v0 object would raise `IOException` on takeover and
`CheckpointCursor` reports that as "the store is unreachable". The empty window
is ACCEPTED rather than refused, and the COST of that choice is that during the
window a genuine replay is assigned a SECOND offset range — the duplication
direction, chosen because the alternative suppresses real commits: refusing would suppress legitimate commits,
which is this ADR's stated primary harm. It closes at the first post-upgrade
checkpoint.

**ADR-0033 is partially amended**, not superseded: its per-pod
`lastAppliedFlushSeq` gains an `incarnationId` and a pointer beside it. Its size argument and
its exclusion of the offset-to-segment index stand unchanged, and are what
rejected the segment-key-in-checkpoint alternative here.

**ADR-0031 is a PRECONDITION here, not an argument.** It bounds offset
assignment when two live processes share a `podId`, and says nothing about
idempotency; the slot-overwrite bound above is what this ADR owes on that
scenario. Two such processes would share an `incarnationId` only by minting
collision, which a UUID makes negligible without coordination.

**Nothing retries yet, and today's retry unit is the wrong one.**
`DefaultIngest.flushLocked` publishes and commits under one lock and discards
`published` on failure, so a producer-level retry is a NEW flush under a new key
with an incremented `flushSeq` — not a replay at all. M4.10d must add a
commit-level retry that resubmits the same `(incarnationId, flushSeq)` and the
same published segment, or state plainly that its coverage stops at the
sequencer seam.

## Alternatives considered

**Qualify `(podId, flushSeq)` with the lease epoch.** Rejected in Context: the
sequencer's term, not the pod's incarnation.

**Use the segment key.** Rejected in Context: measured to collide across
incarnations under a frozen or regressed clock.

**Qualify the pod id by incarnation.** Rejected: an entry per restart, growth
with history.

**Bound the window to the uncheckpointed tail.** Rejected: a replay older than
the newest checkpoint would be neither detected nor answered.
