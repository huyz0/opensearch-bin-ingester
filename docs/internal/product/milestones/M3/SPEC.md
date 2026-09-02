# M3 — Ingest path

**Status:** draft, awaiting review · **Decided by:**
[ADR-0017](../../decisions/0017-every-pod-writes.md) (the adaptive interval is
the cost dial, no forwarding, no writer lease) carrying forward
[ADR-0016](../../decisions/0016-designated-writer-per-az.md) §2 (the
`fillRatio` mechanism itself, superseded only in its writer-count-scaling
half)

**Completion condition:** the Helidon endpoint and the in-process API both
flush at 8 MiB **or the adaptive interval**, per pod, with no coordination
between pods — replacing M1's fixed 250 ms operating point, which ADR-0017
prices at $311/month against the adaptive interval's $15.55/month at a 5 s
ceiling (a 20× difference, NFR-1). Memory stays flat and bounded, independent
of request size (NFR-6), under the adaptive interval, not only the fixed one
M1.18 already proved. The on-disk directory entry reserves the byte
[ADR-0014](../../decisions/0014-priority-lanes.md) commits to landing here
("adding it later is a format change"), so M10's priority lanes need no
second wire-format change — with no lane *logic* wired yet.

⚠️ **M1 accepted the fixed interval knowingly.** `IngestConfig`'s own javadoc
says so: *"M1 fixes it at 250 ms KNOWINGLY — the adaptive loop is M3."* This
milestone is that loop, not a new mechanism invented from scratch — ADR-0016
§2 already specifies its direction and hysteresis; M3's job is implementing
it correctly, per-pod, with no writer-count scaling (that half is withdrawn
by ADR-0017).

⚠️ **Scope was checked against a real forward-reference before this draft was
written.** `roadmap.md`'s "Deferred into a later milestone" table separately
notes *"the `i8 lane` field lands in the wire format at M3, since adding it
later is a format change"* — a promise this SPEC's own Scope section resolves
explicitly (§below) rather than silently drops or silently absorbs in full.

---

## Requirements

| ID | Text | Status |
|---|---|---|
| NFR-1 | Cost | R1/R1b: PUT rate must track `bytes ÷ segmentSize`, never a fixed timer; the flush interval is the lever | agreed |
| NFR-6 | Service memory | bounded and independent of request size | agreed |
| FR-1 | Accept record writes over HTTP for many indices and partitions on one connection, streamed, without materialising the body | agreed (M1 delivers the streaming; M3 changes only the flush trigger) |
| FR-2 | Bundle records from many indices and partitions into one segment per pod per flush | agreed (M1 delivers bundling at a fixed interval; M3 makes the interval adaptive) |

FR-18 (priority lanes) is **not** served by this milestone beyond the byte
reservation described in Scope — the roadmap places its actual delivery at
M10, and this SPEC does not move that date.

## Scope

### In

1. **The adaptive flush interval**, implemented in `ingest.Accumulator` /
   `ingest.IngestConfig`, replacing M1's fixed `DEFAULT_FLUSH_INTERVAL =
   250ms`. Per pod, no coordination, no forward-ack, no writer lease — every
   pod already writes its own segments (ADR-0017 point 1, unchanged from M1).
2. **NFR-6 re-proven under the adaptive interval.** M1.18's
   `MemoryFlatUnderTenXBodySizeTest` proves flatness only at the fixed
   250 ms/8 MiB operating point. This milestone extends or adds a sibling
   proving the same property once the interval can grow toward its ceiling
   (more wall-clock time between flushes is more opportunity to accumulate).
3. **One reserved byte in the on-disk directory entry**, for the `i8 lane`
   field ADR-0014 places in "the segment run entry" — written as `0`
   (meaning "no lane," M10's own default) on every segment this milestone
   produces, read back and exposed as a field, but **never computed, never
   read from a producer, and never used to change scheduling**. This is a
   genuine wire-format change (`DIRECTORY_ENTRY_BYTES` widens by one byte)
   and follows the [`wire-format-change`](../../../../../.agents/skills/wire-format-change/SKILL.md)
   skill in full: format, every reader, every writer, the fakes, the golden
   files and a new ADR, in one commit.

### Out — deliberately

- **Priority-lane scheduling** (a different `maxStreamLatency` per lane, a
  different flush trigger per lane, runs placed first in a segment for a
  high lane) — ADR-0014's full feature, M10's job. Nothing in this milestone
  computes, accepts, or acts on a lane value; the reserved byte is always 0.
- **Routing-value partition resolution** (ADR-0015: a producer-supplied
  routing string, murmur3-hashed to a partition matching OpenSearch's own
  `OperationRouting`, and the plugin pushing index metadata to the ingester
  over its subscription channel to make that possible). Not started here —
  it is a materially larger, separate piece of work than the interval and
  memory work this milestone actually completes, and the roadmap's own M3
  completion condition does not name it. ⚠️ **Confirmed with the person
  running this session before drafting this SPEC**, given the size
  difference and that it touches a wire format either way — recorded here so
  a future reader does not have to re-derive why ADR-0015's own machinery is
  absent from a milestone whose roadmap row mentions "framed writes."
- **Per-lane latency ceiling** (`intervalCeiling = min(absolute cap, tightest
  lane deadline)`, ADR-0017 point 4). With no lanes yet, `intervalCeiling` is
  a single global `IngestConfig` value, not derived from any per-lane
  deadline. M10 changes this formula; it does not need to exist yet.
  Deferred, not proven false — M3 does not implement the multi-deadline
  minimum this formula describes.
- **Fleet/pod-count autoscaling** (ADR-0017 point 3: an operator's HPA
  concern, driven by ingest bytes/s per pod). Infrastructure, not this
  project's code.
- **The PRODUCER-FACING half of ADR-0014's lane field.** ADR-0014 requires
  lane to be first-class in BOTH "the wire frame and the segment run entry"
  — this milestone reserves only the SECOND half (the on-disk directory
  entry, per Scope item 3). Neither `BulkParser`'s `_bulk` action-line
  parsing nor the in-process `RecordStream` contract gains a lane (or
  routing) field: no producer, HTTP or in-process, can supply a lane value
  this milestone, so the reserved byte has no path to ever be non-zero
  before M10. Round-1 spec review flagged the original wording here as
  ambiguous enough to read as including the producer-facing frame; this
  bullet replaces it.

## Design

### The adaptive interval

⚠️ **Read this before implementing** — ADR-0016's own wording is easy to
misread backwards, and getting the direction wrong here is a real risk (see
Risks). Two passages in the SAME document describe the mechanism at
different levels, and they must be reconciled correctly, not read in
isolation:

- **The unambiguous, authoritative direction** (ADR-0016 §Decision point 2,
  verbatim): *"The flush interval adapts to hit a segment-size target.
  Target 8 MiB; **lengthen the interval** (250 ms → 30 s, capped by a
  per-lane latency ceiling) **while segments come in under target**,
  **shorten it as volume rises**."*
- **A table further down the SAME document** (ADR-0016 §2b) reads `fillRatio
  ≥ 0.9 sustained → "scale up"` and `fillRatio ≤ 0.4 at max interval →
  "scale down"`. ⚠️ **This table is about WRITER COUNT, the half of ADR-0016
  that ADR-0017 explicitly withdraws** ("only the writer consolidation is
  withdrawn"). "Scale up" there means *add a writer pod*, not *lengthen the
  interval* — in that fleet-scaling context, `fillRatio ≥ 0.9` (segments
  already full) means "add capacity," the OPPOSITE direction from what the
  same words would suggest if misread as "increase the interval." ADR-0017
  says the loop's "asymmetry and hysteresis still apply to the interval,"
  which carries forward the **hysteresis band (0.4/0.9) and the timing
  asymmetry** (react fast in one direction, slow in the other) — not the
  literal words "scale up." The direction that actually moves the interval
  is the first bullet above, restated concretely below.

**The rule, concretely, per pod, on `ingest.Accumulator`:**

```
fillRatio = actualSegmentBytes ÷ targetSegmentSize     (targetSegmentSize = maxSegmentBytes, 8 MiB, unchanged)

fillRatio sustained ≤ 0.4 (segments coming in well under target)
    → LENGTHEN the interval, slowly, toward intervalCeiling
fillRatio ≥ 0.9 (segments hitting the size trigger before the timer)
    → SHORTEN the interval, quickly, back toward intervalFloor (250 ms)
```

- **`intervalFloor` = 250 ms** — M1's own fixed value, now the lower bound
  rather than the only value.
- **`intervalCeiling`** — a single `IngestConfig` value (no per-lane deadline
  exists yet; see Scope). Default **5 s**, matching the cost figure this
  whole design exists to hit ($15.55/month, ADR-0017); configurable up to
  ADR-0016's own stated absolute range of 30 s.
- **Asymmetric hysteresis**, mapped from ADR-0016's "scale up fast, scale
  down slow" (there: add a writer within seconds, remove one only after
  minutes, to avoid flapping while never being caught under-provisioned) —
  translated to a single pod's own interval: **shortening reacts fast**
  (protect latency the moment volume rises — the same "under-provisioned is
  worse than over-provisioned" reasoning, applied to one pod instead of a
  fleet), **lengthening reacts slowly** (only after `fillRatio` has stayed
  low for a sustained window, so a momentary lull does not immediately
  balloon the interval and then have to walk it back). Concrete constants
  (`T_shorten`, `T_lengthen`) are calibration values, not yet measured
  against real traffic — see Risks and the "deferred to measurement" list in
  `docs/research/50-open-questions.md` §3. Chosen defaults: `T_shorten` = one
  sustained high-`fillRatio` flush (react immediately, matching "scale up
  fast" read as "protect against underprovisioning without delay");
  `T_lengthen` = 2 minutes of sustained low `fillRatio` before growing.
- **No coordination** (ADR-0017 point 2): no forward-ack, no shared signal,
  no lease. Every pod's `Accumulator` instance owns its own current interval
  as mutable state, computed from only its own flushes.
- A `Clock` seam (matching `ConsumerClient`'s own precedent from M1.16c) is
  what makes the hysteresis timing testable without a real sleep.

### The reserved lane byte

`format.SegmentFormat`'s directory entry widens from 48 to 49 bytes: a
trailing `i8 lane` field, always encoded as `0` by `SegmentWriter` in this
milestone (no caller has a real lane value to pass — `RunKey` gains no lane
component; the concept does not exist above the format layer yet).
`SegmentReader` decodes the same byte into a new `RunEntry.lane()` field,
which every M3 reader observes as `0`. Golden segment fixtures
(`GoldenSegmentTest`'s committed bytes) are regenerated once, via the
project's own scratch-generate-then-paste workflow (matching M2.4's
precedent for filter golden files), never assumed correct without being
independently re-derived. A new ADR (0025, next available number) records
this specific, narrower decision — reserve now, wire later — distinct from
ADR-0014's own broader "lane is a first-class field" framing, because
*when* the byte lands and *what writes real values into it* are two
different decisions with two different milestones.

### Module boundaries respected

Per `architecture.md` rules 4/5: the interval-adjustment logic lives
entirely in `ingest` (`Accumulator`/`IngestConfig`), compiles without
Helidon on the classpath, and is exercised by both delivery surfaces
identically (ADR-0019) — `http`'s `BulkService` and any in-process caller of
`Ingest.append` share the same `Accumulator` behaviour; neither implements
its own flush policy.

## Cost impact

Directly R1/R1b (`docs/research/00-problem/02-cost-model.md`): the flush
interval is the PUT-rate lever, not the writer count (ADR-0017's own
decomposition — 95% of the $311→$15.55/month saving came from the interval
alone, only 2.5% from writer consolidation, which is why M3 needs no
forwarding/lease machinery at all). Before this milestone: fixed 250 ms,
$311/month at 1 MiB/s per pod. After: adaptive up to a 5 s ceiling,
$15.55/month at the same load — a 20× reduction, measured against the same
worked example ADR-0017 itself cites (`docs/research/30-design-space/
13-worked-example-multi-tenant.md` §8, §10). The reserved lane byte adds
exactly one byte per run entry to every segment's directory — negligible
against the 8 MiB target and the existing 48-byte entry, and does not change
request counts at all (rule 6: request rates scale with segments/AZs/nodes,
never with bytes-per-entry).

## Acceptance criteria

1. The flush interval **lengthens** toward `intervalCeiling` when
   `fillRatio` is sustained at or below 0.4, and **shortens** toward
   `intervalFloor` (250 ms) when `fillRatio` reaches 0.9 — proven directly
   against the fillRatio/threshold logic, not merely "a flush eventually
   happens."
2. The interval never exceeds the configured ceiling and never drops below
   250 ms, under any sequence of fillRatio observations, including
   adversarial ones (oscillating between 0.4 and 0.9 every flush).
3. The asymmetric hysteresis holds: shortening reacts within `T_shorten`;
   lengthening requires `fillRatio` to stay at or below 0.4 for the full
   `T_lengthen` window. A test that swaps the two reaction times, or makes
   them symmetric, must fail.
4. At a modeled low-traffic load (1 MiB/s per pod, matching ADR-0017's own
   worked example), once the interval has settled at its 5 s ceiling (the
   size trigger does not bind at this rate: 8 MiB at 1 MiB/s takes 8 s,
   longer than the ceiling, so the timer fires first), a sustained run's
   measured PUT rate is **within ±20% of `1 ÷ intervalCeiling` = 0.2
   PUT/s** — not the fixed-250ms rate's 4 PUT/s (ADR-0016's own worked
   table: 6 writers at a 5 s ceiling produce 1.2 PUT/s combined, i.e. 0.2
   PUT/s each). ⚠️ Round-1 spec review found the original wording
   ("approaches the ceiling-driven rate") unchecable — no stated tolerance,
   contra sdd.md rule 6 ("a number inside a bound"). Measured via
   `CountingBinStore`, not assumed from the interval value alone.
5. **NFR-6 holds under the adaptive interval**: memory stays flat and
   bounded as request size grows 10×, including the case where the interval
   has grown to its ceiling (segments held open for up to 5 s, potentially
   accumulating more before a flush than the fixed-250ms path ever did).
   Proven with a real, bounded heap (`-Xmx`), not inferred from the interval
   logic's own correctness.
6. The reserved lane byte round-trips: every segment produced by this
   milestone decodes with `RunEntry.lane() == 0`; the golden segment
   fixtures are regenerated and re-pinned, not hand-edited; every existing
   reader and writer of the directory entry compiles and passes against the
   new 49-byte width.
7. No module below `http` gains a Helidon dependency; the interval logic is
   reachable and testable from `ingest`'s own test suite with no socket
   (architecture.md rules 4/5).

## Test plan

| Tier | What | Fails first against |
|---|---|---|
| T0 | `fillRatio` computation, threshold crossing (0.4/0.9), asymmetric hysteresis timing via a `Clock` seam, floor/ceiling clamping | a symmetric or swapped-direction interval adjustment |
| T0 | Adversarial oscillation: `fillRatio` alternating 0.4/0.9 every flush never drives the interval outside [floor, ceiling], and never flaps faster than the hysteresis windows allow | an implementation with no debounce, or one that clamps only at the edges and overshoots between them |
| T1 | `Accumulator` + `CountingBinStore` at a modeled 1 MiB/s: measured PUT rate over a sustained run, once the interval settles at its 5 s ceiling, is within +/-20% of 0.2 PUT/s, not the fixed-250ms rate's 4 PUT/s | an interval that never actually lengthens past its initial value under real (not hand-fed) traffic |
| T0 | Directory-entry round-trip with the reserved lane byte: `SegmentWriter` always emits 0, `SegmentReader` always decodes 0, for every existing segment-shape test in the suite | a writer that emits a stray value, or a reader that reads the wrong offset once the entry widens |
| T12 (`-Xmx` bound) | Memory flatness under the adaptive interval, at the ceiling | an accumulation path that grows unbounded once flushes are less frequent |

Each new test named above must have a red record (`scripts/tdd-red.sh`)
before the code it tests exists, same as every prior milestone.

## Risks

- **Getting the fillRatio direction backwards.** ADR-0016's own "scale
  up"/"scale down" table (about writer count, withdrawn by ADR-0017) reads,
  out of context, as if `fillRatio ≥ 0.9` means "lengthen" — it means the
  opposite once translated to a single pod's interval (see Design). A wrong
  reading here would ship an interval that shortens under light load and
  lengthens under heavy load, the reverse of the cost story this milestone
  exists to prove. Mitigated by stating the concrete rule in Design rather
  than the ADR's own ambiguous table, and by acceptance criterion 1 pinning
  the direction with a fresh test that would fail under the swapped reading.
- **Hysteresis timing constants are not yet measured against real traffic**
  (`T_shorten`, `T_lengthen`, and the intervalCeiling default of 5 s). The
  chosen defaults are reasoned, not benchmarked; `docs/research/
  50-open-questions.md` §3 already lists comparable constants as "deferred
  to measurement." Revisit once real production traffic exists (M9's job,
  per the roadmap).
- **Real-time-based hysteresis is a flakiness risk** in tests. Mitigated by
  the same `Clock`-seam pattern M1.16c already established for
  `ConsumerClient`'s own timer-dependent behaviour.
- **Widening the directory entry could silently break an existing golden
  fixture's assumed 48-byte width** anywhere it is hardcoded rather than
  read from `SegmentFormat.DIRECTORY_ENTRY_BYTES`. Mitigated by the
  wire-format-change skill's own checklist: sweep every reader/writer/fake,
  not only the two obvious ones.

## Tasks

Decomposed into `backlog.md` once this SPEC is reviewed, per the `spec`
skill. Expected shape (subject to the review finding a better split):

1. Reserve the lane byte (wire-format-change discipline, its own small ADR).
2. `IngestConfig` gains floor/ceiling/threshold/hysteresis-timing fields,
   replacing the single fixed `flushInterval`.
3. `Accumulator` computes and exposes `fillRatio` per flush (no interval
   adjustment yet — split out for reviewability).
4. `Accumulator` adjusts its own interval per the fillRatio/hysteresis rule,
   with a `Clock` seam.
5. Integration proof at a modeled low-traffic load (`CountingBinStore`).
6. NFR-6 memory-flatness re-proof under the adaptive interval.
