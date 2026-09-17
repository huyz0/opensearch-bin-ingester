<!-- SPDX-License-Identifier: Apache-2.0 -->
# M7 — retention, GC and the consumer watermark

Nothing in this tree deletes anything. Every segment, every delta and every
checkpoint ever written is still in the bucket, which is why storage has not
yet appeared in a cost table and why
[ADR-0036](../../decisions/0036-the-idempotency-key-carries-an-explicit-pod-incarnation.md)
could say "the pointed delta must still exist" without owing a rule. M7 is
where deletion starts, and therefore where the project acquires its first
mechanism that can destroy committed data.

⚠️ **THE HEADLINE RISK IS DELETING DATA A CONSUMER HAS NOT READ**, and it is
silent in the same way M6's placement bug was silent: the delete succeeds, the
metric goes green, storage drops, and the loss is discovered by a shard that
comes back from a ten-minute restart to a `404` on the segment it was about to
fetch. A retention bug is not an exception and not a failed gate. So every
criterion below is written against the *keep* direction as well as the *delete*
direction — a rule that deletes nothing passes half of them and fails the other
half, and so does a rule that deletes everything.

⚠️ **AND THE WATERMARK IT RELIES ON IS OPTIMISTIC BY CONSTRUCTION.**
`IngestionEngine.getIngestionState()` reports `streamPoller.getBatchStartPointer()`
— the in-memory pointer — while only `lastCommittedBatchStartPointer` survives a
crash, and OpenSearch exposes no way to read the latter
([ADR-0005](../../decisions/0005-no-consumer-offset-store.md),
[research 09 §6.1](../../../../research/30-design-space/09-consumer-position-and-watermarks.md)).
Every watermark this milestone collects can therefore be **ahead of what a
restart would resume from**. That is not a bug to fix; it is the reason
`safetyMargin` exists, the reason a watermark may only *extend* retention, and
the reason the time floor — not the watermark — is the safety net.

## Completion condition

A segment that every known consumer copy has read, and that is older than
`minRetention`, is **deleted**; a segment that any copy has not read, or that is
younger than `minRetention`, is **kept** — including when the copy that has not
read it has gone silent. GC runs from the commit log, issues **zero LIST on the
hot path**, and a consumer offline for the whole retention window resumes with
**no records lost** (NFR-13).

## Requirements

| ID | Requirement | Served by |
|---|---|---|
| FR-9 | Retention and GC driven by time **and** a conservative consumer watermark | scope 1-6; criteria 1-10, 14-17 ⚠️ **Enumerated rather than given as a range**: an earlier draft of this cell said "criteria 1-11", which claimed criterion 11 (NFR-13's) for FR-9 and silently claimed nothing for the orphan sweep, the lease and the alarms, because at that point they had no criteria at all |
| NFR-13 | Consumer outage tolerance = retention (default 6 h) | scope 5; criteria 11, 12 ⚠️ **Reassigned from M5** by M5.21, because it is a property of retention and M5 cited it with no criterion behind it |
| FR-10 | Consumers keep working without the ingester, via a documented fallback ladder | scope 6; criteria 10, 12 ⚠️ **The `oldestRetainedOffset` half only.** A consumer whose position has been GC'd past must be told so; the rest of the ladder is M5.18's and stays a policy nothing executes |
| FR-9 | ⚠️ **Measurements M5 and M6**, assigned to M7 by [50-open-questions §3](../../../../research/50-open-questions.md) — not a requirement of their own, and named here because criterion 18 would otherwise be served by no row | criterion 18 |
| NFR-2 | Idle cost: zero object-store requests from consumers | criteria 13, 16 — **re-asserted, because M7 adds a progress frame on the subscription and a GC loop, and neither may buy a request when nothing has expired** |
| NFR-16 | Governor refusals in steady state: zero | ⚠️ **NOT SERVED, and named so it is not silently claimed.** No governor exists in this tree; the LIST ceiling (R15) is asserted in M7 by a counting store, not refused by a governor. M10's row |

## Scope

1. **The progress frame (FR-9, ADR-0005).** A `ConsumerProgress` type in
   `format`, pushed by the plugin over the subscription it already holds,
   batched across every partition on the node: `(indexUUID, partition,
   shardCopy, consumedUpTo)` per entry. ⚠️ **A wire-format change**, so it
   carries the
   [`wire-format-change`](../../../../../.agents/skills/wire-format-change/SKILL.md)
   obligations and an ADR of its own — the same shape ADR-0047 gave
   `IndexRegistration`, on the same channel, for the same reason: it is
   metadata-only, same-AZ and costs no object-store request.
2. **The watermark table (research 09 §6.2-6.4).** In `ingest`, keyed by
   `(RunKey, shardCopy)`, answering `min()` over **all known copies** with an
   explicit freshness verdict. ⚠️ **SILENCE IS NOT PROGRESS**: a copy that stops
   reporting freezes at its last value and is **not** dropped from the `min()`,
   and a new allocation id appearing for a partition does **not** retire the old
   one — both legitimately exist mid-relocation. ⚠️ **AND A COPY'S REPORTED
   OFFSET CAN GO BACKWARDS, WHICH IS NORMAL AND MUST BE TAKEN.** The reported
   pointer is the in-memory one (ADR-0005), so a shard that restarts resumes
   from its last Lucene commit and reports a **lower** offset than it did
   before the crash. A per-copy `max()` — the natural implementation of
   something called a watermark — keeps answering the pre-crash value and GC
   deletes exactly the records that shard is about to re-read. **The latest
   report wins per copy; monotonicity is a property of the *rule*, never of the
   *feed*.**
3. **`consumerWatermark` in the checkpoint (research 09 §8).** Per stream,
   beside the `nextOffset` and `oldestRetainedOffset` the checkpoint already
   carries. ⚠️ **No new object and no new CAS** — and a **second wire-format
   change**, `Checkpoint` v2, with the v0 and v1 golden files still decoding.
4. **The retention rule (research 09 §7).** A pure predicate over
   `(segment age, per-stream end offsets, min watermark, freshness)` with
   `minRetention` / `safetyMargin` / `maxRetention` / `reportTimeout`. The
   `maxRetention` ceiling **deletes and alarms**; it is an incident, not a
   routine path.
5. **GC from the commit log (research 06 §4).** A **leased** role on the
   existing lease mechanism — never two pods at once — deleting expired
   segments, superseded deltas and superseded checkpoints in batched DELETEs,
   and advancing `oldestRetainedOffset`. ⚠️ **IT OWES ADR-0036's POINTER A
   RULE**: a delta that any live checkpoint pod-slot points at is **never**
   deleted, whatever its age, or ADR-0036's criterion 6 ("a detected replay is
   ALWAYS answered") stops holding for an inherited pointer of unbounded age
   (M5.55). ⚠️ **AND "SUPERSEDED" IS DEFINED HERE, BECAUSE THE OBVIOUS READING
   OF IT LOSES RETAINED DATA.** A checkpoint carries offsets and pod slots and
   **deliberately not the offset-to-segment index** (ADR-0033,
   `Checkpoint`'s own javadoc), so the delta is the **only** object that can
   say which segment holds a given offset. Deleting every delta below the
   newest checkpoint sequence therefore leaves *retained* segments
   unlocatable — a consumer reading inside the retention window gets nothing,
   and every criterion about deletion still passes. A delta is collectable
   **only** when all three hold: (a) its sequence is below the newest
   checkpoint's, (b) **every segment it indexes has itself been deleted** by
   the retention rule — research 06 §4's second conjunct, "and past retention",
   which the one-line version of this rule drops — and (c) no live checkpoint
   pod-slot points at it. The same three-part test applies to a checkpoint: only
   the newest is load-bearing, and an older one goes when nothing points into
   it.
6. **The orphan sweep.** A periodic LIST reconciliation, one hour-prefix at a
   time, for segments PUT by a pod that died before committing — garbage the log
   **by definition cannot know about**. Bounded to one LIST per 1,000 keys and
   below the R15 ceiling, and gated behind a **grace period longer than the
   maximum possible commit delay**, because getting that wrong deletes live
   data. ⚠️ **DEFAULTS, NAMED RATHER THAN LEFT TO THE IMPLEMENTATION**:
   `orphanGrace = 1 h` and a sweep period of 1 h, both from research 06 §4,
   both configurable, and the grace is deliberately generous because the
   degraded `ctl/inbox/` commit path (M8's) is slower than the direct one and
   does not exist yet to be measured. ⚠️ **AND R15 IS A RATE WHILE A COUNTING
   STORE COUNTS**: the budget is asserted as LIST calls per sweep over an
   injected clock — one per 1,000 keys, one prefix per pass — which bounds the
   rate only because the period is named here.
7. **The two measurement constants M7 owes**
   ([50-open-questions §3](../../../../research/50-open-questions.md)): M5,
   `safetyMargin`, sized **above the observed Lucene commit interval** rather
   than guessed; and M6, whether the plugin can observe the *committed* pointer
   instead of the in-memory one. ⚠️ **A measured answer or a recorded
   `NOT-MEASURED`** — the standing rule is that the answer to a measurement
   constant is to build the thing that measures it, never to defer the design
   around it.

### Not in scope

- **Compaction (FR-14).** Deferred past M9 by research 06 §1 and Q17, and its
  trigger threshold is measurement M4. M7 deletes; it never rewrites or remaps.
- **Object-store lifecycle rules as a backstop.** Research 06 §6 leaves it open
  and it can delete unconsumed data. It is documented as an operator setting
  well beyond `maxRetention`, and nothing in this tree sets it.
- **Abandoned multipart uploads.** A bucket lifecycle rule
  (`AbortIncompleteMultipartUpload`, 1 day) — explicitly *not* hand-rolled
  (research 06 §4).
- **A cost governor refusing the LIST rate (NFR-16).** M10.
- **WAL retention.** `ack_mode=wal` disks are M8/M11's; M7 is the object store.

## Design

**The rule, stated once, normatively** (research 09 §7):

```
delete(segment) iff
      (     age(segment) > minRetention                        # the time floor
        AND every known copy of every stream in the segment
            reported within reportTimeout                      # else the min() is stale
        AND min(watermark over ALL known copies)
              > segment.endOffset(stream) + safetyMargin )
  OR  age(segment) > maxRetention                              # ceiling: delete AND ALARM

# ⚠️ THE PARENTHESES ARE LOAD-BEARING. Research 09 §7 writes this without
# them, and read with OR binding tighter the ceiling stops firing on its own --
# which is the one clause that must fire when every other says keep.
```

Four properties of that rule are the whole design, and each is a place a
plausible implementation goes wrong:

- **The watermark is a brake, never an accelerator.** It can only push deletion
  *later* than the time floor. An implementation that deletes a fully-consumed
  20-minute-old segment is faster, cheaper and wrong: the outage budget
  (NFR-13) *is* `minRetention`, and a consumer that restarts into a
  freshly-emptied bucket has lost data it had not committed (research 09 §6.1).
- **Staleness fails closed.** If any known copy has not reported within
  `reportTimeout`, the `min()` is not trustworthy and the watermark clause is
  simply unsatisfied — GC keeps. A failover means "GC pauses briefly", which is
  the correct failure direction (research 09 §8).
- **`min()` is over copies, not over the primary.** `all_active = true` means
  every shard copy independently consumes the partition
  ([ADR-0009](../../decisions/0009-replication-mode-segment-replication-primary-only-ingest.md),
  research 08 §5). A lagging replica in another AZ is a consumer.
- **A backwards report is truth, not noise.** It means a restart, and the
  records between the two positions are about to be read again.
- **The ceiling alarms.** Crossing `maxRetention` means a consumer *will* lose
  data — a paused shard pins retention forever (research 09 §6.5) — and
  silently deleting is worse than a loud alert.

**Where GC gets its facts.** From the commit chain, which already holds
everything needed: `CommitDelta → SegmentCommit(segmentKey, runs) →
RunCommit(RunKey, recordCount, firstOffset)`, so a segment's last offset for a
stream is `firstOffset + recordCount - 1`, and its age is the timestamp already
in `SegmentKey`. No LIST, no HEAD, no per-segment GET. This is why research 06
§4 calls commit-log-driven GC "essentially free": DELETE requests cost nothing
and `DeleteObjects` batches 1,000 keys.

**Rejected alternatives:**

- **A Kafka-style offset store.** Rejected by ADR-0005 and not re-opened here.
  The position OpenSearch commits atomically with the documents is the true
  one; a second copy disagrees with it and loses data when it does.
- **Polling `GetIngestionStateAction` from the ingester.** Rejected by ADR-0005:
  it needs OpenSearch credentials in the ingester and returns the same in-memory
  value anyway.
- **Driving GC from LIST.** Rejected by R2 and research 06 §4: exact information
  is already in the log, and LIST costs a PUT each. LIST survives only as the
  periodic orphan reconciliation, which is the one thing the log cannot answer.
- **Dropping a silent copy from the `min()`.** Rejected — it is exactly how a
  node down for ten minutes comes back to deleted data (research 09 §6.3).
- **Retiring an allocation id because a new one appeared for the partition.**
  Rejected — both copies legitimately exist mid-relocation (research 09 §6.4).
- **A watermark that can shorten retention below the floor.** Rejected by
  ADR-0005; it is the optimistic-pointer trap (§6.1) turned into data loss.
- **Deleting every delta below the newest checkpoint.** Rejected: ADR-0033
  keeps the offset-to-segment index in the deltas and out of the checkpoint, so
  that is how a segment inside the retention window becomes unreadable while
  every deletion criterion stays green. Scope 5 states the three-part test.
- **A per-copy `max()` on the reported offset.** Rejected: the feed is the
  in-memory pointer, which legitimately regresses on restart (ADR-0005), and a
  `max()` pins the watermark at the pre-crash value — deleting precisely what
  the restarted shard re-reads.
- **Deleting the pointed delta once it is past retention.** Rejected: ADR-0036
  owes it, and M5.55 made inherited pointers unbounded in age. GC pins it
  instead, and the cost of that pin is a criterion rather than a footnote.

## Cost impact

| Rule | Effect |
|---|---|
| **R8** (retention is a cost dial; past ~3 h storage exceeds API cost) | **This milestone is R8's implementation.** At Scenario A's 8.64 TB/day, 6 h retention is ~$50/month of storage against ~$8 at 1 h and ~$199 at 24 h (research 06 §3). The default stays **6 h**, configurable per index |
| **R2** (never LIST on a hot path) | Held. GC reads the commit log it already replays; LIST appears only in the orphan sweep, off any hot path |
| **R15** (LIST ceiling ~1/s sustained) | ⚠️ **AS DELIVERED, THE 44 IS ARITHMETIC AND NOT AN ASSERTION** — M7.22, found by the milestone review; the case that landed puts 2,500 keys and asserts 3 LIST. The rest of this cell is as specified: the sweep is bounded to one LIST per 1,000 keys over one hour-prefix at a time — **44** LIST for a 43,200-object hour (`ceil(43200/1000)`, not 43: research 06 §4 rounds down and an assertion written to the literal 43 reds a CORRECT sweep), ≈ $0.0002 — and asserted under the ceiling by `CountingBinStore` |
| **R6** (commit/metadata write rate independent of index count) | Unchanged. `consumerWatermark` adds one varint per stream to an object that is already written on the same schedule: no new object, no new CAS, no new request |
| **R3 / NFR-2** (idle consumers issue zero requests) | The progress frame rides the open subscription: metadata-only, same AZ, **zero object-store requests**. Re-asserted rather than assumed — criterion 13 |
| **DELETE** | Free, batched 1,000 keys per call. Budget: **≤ 1 DELETE call per 1,000 keys deleted**, asserted |

⚠️ **The one number that moves the wrong way is the checkpoint's size**, by one
varint per stream — a few kilobytes at 1,600 streams, on an object already
bounded by ADR-0033. Stated because "cost-neutral" is a claim this project
does not accept without a number.

## Acceptance criteria

1. **A progress frame round-trips and refuses malformed input.**
   `ConsumerProgress` encode/decode is exact for a batch of ≥3 entries across
   ≥2 indices; a truncated frame, a forward version, a wrong magic, an entry
   count larger than the frame and trailing bytes are each **refused**, and
   `golden/consumer-progress-v1.bin` pins the bytes.
2. **The watermark is the minimum across copies.** Three copies of one
   partition at offsets 100, 90 and 300: the answer is **90**. With the 90 copy
   removed by explicit deregistration, **100**. ⚠️ **AND A COPY THAT REPORTS
   BACKWARDS IS BELIEVED**: the 300 copy restarts and reports **150**, so the
   answer becomes **100** and not 150 — the `min()` moves with it, and a
   per-copy `max()` (which would answer 100 here too) is separated by the
   two-copy case where the regressing copy holds the minimum: 90 then 40 answers
   **40**.
3. **A silent copy freezes the watermark rather than being dropped.** The 90
   copy stops reporting while the others advance to 500: the answer stays
   **90**, and GC deletes nothing above it. ⚠️ A `min()` over *reporting* copies
   would answer 500 and pass every other criterion here.
4. **A silent copy past `reportTimeout` makes the table unfresh, and GC keeps.**
   With **one** copy beyond `reportTimeout` and **two others reporting in this
   interval**, `RetentionRule` deletes **no** segment on the watermark clause —
   only the `maxRetention` ceiling can fire. ⚠️ **THE MIX IS THE FIXTURE.**
   Freshness taken from the *newest* report rather than the *oldest* is the
   mutation this criterion exists to catch, and where every copy is stale the
   two readings agree and it survives.
5. **A relocation does not retire the old copy.** A new `shardCopy` reporting
   for a partition while the old one is silent leaves both in the `min()`; the
   old one is retired **only** by explicit deregistration or by an expiry
   greater than `minRetention` — and **both retirement paths are asserted to
   fire**, the deregistration and the expiry. ⚠️ A criterion asserting only that
   nothing is retired is satisfied by an expiry path that never runs, which
   pins retention on the first relocation and never releases it.
6. **The plugin pushes progress for every partition on the node, batched, at
   zero store cost, and each entry carries that shard's consumed position.**
   Over a node hosting ≥8 partitions across ≥2 indices, one interval produces
   **one** frame carrying 8 entries; `CountingBinStore` records **zero**
   requests attributable to the reporter; **each entry's `consumedUpTo` equals
   the offset that shard has actually consumed**, with the eight shards given
   **eight different** positions and at least one of them **behind the stream's
   tail**; and a partition this node does not host appears in **no** entry.
   ⚠️ **THE CONTENT HALF IS THE POINT.** A reporter that sends the stream's
   *tail* offset, or a constant, satisfies a count-and-cost criterion exactly —
   one frame, eight entries, zero requests — and drives `min()` to the live edge
   forever, so GC deletes records no shard has indexed. Criteria 2-5 cannot
   catch it: they are T0 cases feeding `WatermarkTable` from fixtures, not from
   the reporter.
7. **`Checkpoint` v2 carries the watermark and the older versions still
   decode.** A v2 round-trip preserves `consumerWatermark` per stream;
   `golden/checkpoint-v0.bin` and `golden/checkpoint-v1.bin` still decode to the
   same values they do today.
8. **The rule keeps what it must keep.** (a) A fully-consumed segment younger
   than `minRetention` is **kept**. (b) A segment older than `minRetention`
   whose stream's min watermark is below `endOffset + safetyMargin` is
   **kept** — including at exactly `endOffset + safetyMargin`, the boundary.
   (c) A segment older than `minRetention` and below the min watermark by more
   than `safetyMargin` is **deleted**. (d) A segment past `maxRetention` is
   deleted **and** raises the alarm — asserted as the alarm firing, not as the
   deletion alone.
9. **GC from the commit log issues zero LIST and batches its deletes.** Over
   ≥2,500 expired segments: `CountingBinStore` records **0** LIST, **0** GET of
   any segment, and **≤ 1 DELETE call per 1,000 keys**.
10. **A delta a checkpoint pod-slot points at is never deleted, and a consumer
    below `oldestRetainedOffset` is told so.** (a) With a pod slot pointing at a
    delta older than `maxRetention`, GC keeps that delta and a replay of that
    pod's `(incarnationId, flushSeq)` is still answered. (b)
    `oldestRetainedOffset` in the checkpoint advances to what GC actually
    deleted, and a subscription from an offset below it is refused
    **distinguishably** — not answered with silence or an empty delivery.
11. **NFR-13: a consumer offline for the whole retention window loses nothing.**
    A consumer stops at offset *k*, the ingester keeps committing for a
    simulated `minRetention`, GC runs throughout, and the consumer resumes from
    *k* and reads **every** record — asserted by count and by content, not by
    absence of exception.
12. **Past the ceiling, data is lost and it is loud.** The same consumer offline
    past `maxRetention` finds its position gone, receives criterion 10's
    distinguishable refusal, and the alarm of criterion 8(d) has fired.
13. **NFR-2 re-asserted with the progress path live.** ≥1,000 idle consumers
    over ≥3,000 intervals of an injected clock, reporting progress every
    interval, with a GC loop running and nothing expired: **zero**
    object-store requests and **zero** grants.
14. **The orphan sweep keeps what is merely slow to commit, and its LIST budget
    holds.** A segment PUT but never committed is **kept** while it is younger
    than `orphanGrace` and **deleted** once older; a segment the log references
    is never a candidate whatever its age; one pass covers **one** hour-prefix
    and issues **≤1 LIST per 1,000 keys** — **44** LIST over a 43,200-key hour (`ceil`, not the corpus's rounded 43; ⚠️ **NOT ASSERTED AS DELIVERED** — M7.22, and [VERIFIED.md](VERIFIED.md)'s criterion 14 line says so),
    asserted by `CountingBinStore`, with the period named so the rate is under
    R15's ~1/s ceiling. ⚠️ The within-grace *keep* is the case that matters;
    the past-grace delete is the easy half.
15. **A delta is collected only when nothing needs it, and a retained offset
    stays locatable.** With a checkpoint newer than a delta whose segments are
    **still retained**, that delta is **kept**, and a consumer reading an offset
    it indexes still resolves to a live segment. Once every segment it indexes
    has been deleted and no pod slot points at it, it is collected. The **newest
    checkpoint is never deleted**.
16. **GC is leased, and a fenced GC deletes nothing.** Two GC roles cannot run
    at once; a GC that loses the lease mid-pass issues **no further DELETE** —
    asserted as the absence of a delete after fencing, counted at the store, not
    as an exception being thrown. And a node holding no lease issues **zero**
    store requests of any kind, which is criterion 13's other half.
17. **The operator can see the outage budget, and the alarms have no
    high-cardinality label.** `minRetention − age(oldest unread data)` is
    exported, and **read at two fixture ages the LATER reading is SMALLER by
    the elapsed time** — ⚠️ a gauge returning the constant `minRetention`, or 0, is
    exported, carries no label and satisfies every other clause here, while the
    one tile worth a dashboard reads a healthy six hours with the budget minutes
    from zero; the three alarms of research 09 §9 (a copy silent past
    `reportTimeout`, a segment approaching `maxRetention`, a paused shard older
    than `minRetention`) each fire in a test; and `check-metric-cardinality.sh`
    is green with **no** per-stream, per-index, per-partition or per-copy label
    — the detail rides a bounded top-K event, the shape M4.14 established.
18. **`safetyMargin` is measured, or recorded as not measured.** The observed
    interval between committed-pointer advances on a real node is reported as a
    number, and `safetyMargin`'s default is above it — or the constant is
    recorded `NOT-MEASURED` in `VERIFIED.md` with what stopped it. ⚠️ The same
    line answers measurement M6 (can the plugin see the *committed* pointer),
    since a `yes` shrinks this margin and is an ADR.

## Test plan

| Tier | Behaviour | Fails first against | Mutation it catches |
|---|---|---|---|
| T0 | progress frame encode/decode + golden | `ConsumerProgressTest`, `GoldenConsumerProgressTest` | a reordered field; a truncated frame accepted; a forward version accepted; an entry count trusted past the frame length |
| T0 | `min()` across copies, silence frozen, relocation | `WatermarkTableTest` | `min()` over **reporting** copies only (criterion 3 is the only case that reds it); retiring an allocation id when a new one appears; ⚠️ **a per-copy `max()` on the reported offset**, which is what "watermark" suggests and what a restart's backwards report makes fatal — separated only by the case where the REGRESSING copy holds the minimum; dropping a copy at `reportTimeout` instead of marking the table unfresh; an expiry path that never fires |
| T0 | freshness gates the watermark clause | `WatermarkTableTest`, `RetentionRuleTest` | a stale table treated as fresh; freshness computed from the newest report rather than the **oldest** |
| T0 | the retention rule, both directions and the boundary | `RetentionRuleTest` | `>` → `>=` on `endOffset + safetyMargin`; dropping `safetyMargin`; deleting on the watermark alone with no time floor (criterion 8a is the only case that reds it); the ceiling deleting **without** alarming |
| T0 | `Checkpoint` v2 + v0/v1 goldens | `CheckpointTest`, `GoldenCheckpointTest` (extended) | a v2 field written into a v1 object; a v1 object decoded as v2; the watermark written where `oldestRetainedOffset` belongs — distinguishable only because the fixture gives them **different** values |
| T1 | GC from the log: what is deleted, what is kept, and what it costs | `CommitLogGcTest` | a GET per segment; a LIST anywhere; one DELETE per key; deleting a segment whose stream is only partly consumed; deleting the **newest** checkpoint; ⚠️ **deleting every delta below the newest checkpoint** — the obvious reading of "superseded", which leaves a RETAINED segment unlocatable because ADR-0033 keeps the offset-to-segment index in the deltas, and which no deletion criterion can see |
| T1 | the pointed delta is pinned | `PointedDeltaPinnedTest` | pinning by age instead of by pointer (M5.55's inherited pointers are unbounded in age, so an age-based pin passes for a young chain and fails for a real one — the fixture uses a pointer **older** than `maxRetention`) |
| T1 | the orphan sweep | `OrphanSweepTest` | deleting an uncommitted segment **within** the grace period (the live-data delete); sweeping more than one hour-prefix per pass; a LIST per key rather than per 1,000; deleting a key the log references |
| T1 | GC is leased, never concurrent | `GcLeaseTest` | two pods sweeping at once; a GC that continues after losing the lease — asserted as **no delete after fencing**, not as an exception |
| T1 | `oldestRetainedOffset` advances to what GC deleted | `RetainedOffsetAdvanceTest` | ⚠️ **THE ONE THAT SURVIVES EVERYTHING ELSE**: leave `CheckpointWriter` writing the literal 0 it writes today and every other row in this plan stays green, while criteria 10(b), 12 and the whole refusal path become unreachable by construction. Also: advancing to the segment's FIRST offset rather than past its last, which under-reports by one segment and refuses a consumer that is fine |
| T0 | a position below `oldestRetainedOffset` is refused, distinguishably | `RetainedOffsetRefusalTest` | answering an empty delivery (indistinguishable from "nothing new"); answering a GAP (M6.1's type, which means "some records were dropped", not "your position is gone"); refusing at `>=` rather than `>`, which refuses the oldest RETAINED offset itself |
| T1 | the plugin's reporter: one frame per node per interval, zero store | `ProgressReporterTest` | ⚠️ **the tail offset, or a constant, in place of the consumed pointer** — the mutation the count-and-cost half cannot see, and the one that drives `min()` to the live edge and deletes unindexed data; a frame per shard (8× at 8 partitions); a timer rather than the interval seam; reporting a partition this node does not host; a reporter that touches the store |
| T1 | NFR-2 with progress and GC live | `IdleConsumerCostTest` (extended) | a GC pass that lists when nothing has expired; a progress push that reads the store |
| T1 | the three alarms and the outage-budget tile | `RetentionAlarmsTest` | ⚠️ **the budget gauge replaced by a constant** — `return minRetention` has no label, is exported, and passes every emitter case, so the gauge is read at two fixture ages and the LATER reading must be SMALLER by exactly the elapsed time -- a difference-only assertion is survived by a sign flip, `minRetention + age(oldest unread)`, which counts UP as the budget runs out; ⚠️ each alarm emitter NO-OPPED one at a time — the mutation the criterion's own wording invites, since "the alarms exist" is green against a constant-zero budget and two dead emitters; a `stream`, `index` or `shardCopy` label, which `check-metric-cardinality.sh` catches and this row names so the gate is not its only reader |
| T1 | the ceiling alarm fires **on the GC path**, not only in the rule | `CeilingAlarmOnGcTest` | ⚠️ **no-op the alarm where GC crosses the ceiling** — `RetentionRuleTest`'s T0 fixture still fires it, so criterion 8(d) passes and criterion 12 passes with a silent ceiling. The deletion and the alarm are asserted at the same call, by the same pass |
| T2 | NFR-13 across a simulated retention window | `ConsumerOutageToleranceTest` | GC deleting below a frozen watermark; a resume that skips records silently; a resume that reports success having read none |
| T4 | the committed-pointer interval, on a real node | `CommitIntervalProbeIT` | ⚠️ **A MEASUREMENT, NOT A GATE.** It reports a number; it asserts only that the number exists and that `safetyMargin`'s default exceeds it. A test that asserted a specific interval would pin OpenSearch's translog policy, which is not ours to pin |
| T4 | a position GC'd past is refused distinguishably, on a real node | `RetentionRefusalIT` | a shard that starts, reports healthy and indexes nothing — ADR-0020's failure mode, which is what a silent refusal produces here |

⚠️ **T2 rather than T4 for NFR-13** because the property is about *our* rule
over a simulated clock and a simulated 6 hours; a real node adds no evidence and
costs six hours. The two T4s exist because "what interval does OpenSearch commit
at" and "what does a real shard do when its position is gone" are properties of
OpenSearch, not of our seams.

⚠️ **Coverage and mutation**: `check-coverage.sh` exists but is unwired;
`check-mutants.sh` does not exist. Both facts are stated in every report this
milestone produces rather than implied away.

⚠️ **Suites this milestone extends**: the store conformance suite (DELETE
batching and a delete of a missing key), the cost assertions
(`CountingBinStore` budgets for LIST and DELETE), and `IdleConsumerCostTest`.

## Risks

| Risk | What would reveal it |
|---|---|
| **GC deletes data a consumer has not read** — the risk this milestone exists to manage | Criteria 3, 4, 8(a)(b) and 11 are all *keep* assertions, and criterion 3 is the one a `min()` over reporting copies fails alone. ⚠️ A delete-nothing implementation passes them and fails 8(c) and 9, so neither direction can be satisfied by doing less |
| **The optimistic watermark deletes what a restart would re-read** | `safetyMargin`, criterion 18's measurement, and the time floor. ⚠️ If criterion 18 lands `NOT-MEASURED`, the margin is a guess and `VERIFIED.md` must say so — that is the honest failure, not a plausible default quietly shipped |
| **The orphan sweep deletes a segment whose commit is merely slow** | Criterion 14's within-grace *keep*, `orphanGrace` defaulted to 1 h above the maximum commit delay including the degraded `ctl/inbox/` path, and `OrphanSweepTest`. ⚠️ An earlier draft of this cell answered the risk with "criterion 10's grace period", and criterion 10 contains no grace period — the sweep had no criterion at all, which the SPEC review found |
| **A retained segment becomes unlocatable because its delta was collected** | Criterion 15, and the three-part collectability test in scope 5. ⚠️ Nothing about it is visible from the delete side: the segment is still there, the offset simply resolves to nothing |
| **A restarted shard's backwards report is discarded as noise** | Criterion 2's regression half, with a two-copy case where the regressing copy holds the minimum so a per-copy `max()` is distinguishable |
| **ADR-0036's pointed delta is collected and a replay becomes unanswerable** | Criterion 10(a), with a pointer **older** than `maxRetention` so an age-based pin cannot pass |
| **Two pods GC concurrently and one deletes what the other is reading** | `GcLeaseTest`, and the fencing case asserting no delete after lease loss |
| **A paused shard pins retention forever and storage grows without bound** | The `maxRetention` ceiling and criterion 8(d)'s alarm, plus criterion 12's loud loss |
| **M7 quietly buys an object-store request** | Criterion 13, re-asserted with both the progress path and the GC loop live |

## Tasks

One commit each, in dependency order.

| ID | Task |
|---|---|
| M7.0 | This spec, its backlog rows, and the spec review |
| M7.1 | `ConsumerProgress` in `format` + ADR + golden file (wire-format change) |
| M7.2 | `WatermarkTable` in `ingest`: `min()` across copies, silence frozen, relocation safe |
| M7.3 | The plugin reports progress: one frame per node per interval, zero store cost |
| M7.4 | `Checkpoint` v2 — `consumerWatermark` per stream + ADR + goldens (wire-format change) |
| M7.5 | `RetentionRule`: the predicate of research 09 §7, both directions and the boundary |
| M7.6 | GC from the commit log: expired segments deleted in batches, zero LIST |
| M7.7 | The pointed delta is pinned; superseded deltas and checkpoints collected |
| M7.8 | The orphan sweep: one hour-prefix per pass, grace period, LIST budget |
| M7.9 | GC is a leased role, and a fenced GC deletes nothing |
| M7.10 | `oldestRetainedOffset` becomes true: the checkpoint records what GC deleted |
| M7.16 | A position below `oldestRetainedOffset` is refused distinguishably — and whether that refusal is a wire-format change is decided, not assumed |
| M7.11 | NFR-13: a consumer offline for the retention window loses nothing, past the ceiling loses loudly |
| M7.12 | NFR-2 re-asserted with progress and GC live; the LIST and DELETE budgets asserted |
| M7.13 | Observability: the watermark, the oldest retained age, the outage budget, and the three alarms |
| M7.14 | Measurement M5 and M6: the committed-pointer interval on a real node, and `safetyMargin` sized above it |
| M7.15 | M7's `VERIFIED.md` and the milestone review (eighteen criteria) |
