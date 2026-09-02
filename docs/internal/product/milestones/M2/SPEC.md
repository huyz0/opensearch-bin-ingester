# M2 — Segment format

**Status:** draft, awaiting review · **Decided by:**
[ADR-0003](../../decisions/0003-adaptive-tagged-membership-filter-in-the-object-key.md)
(the filter design itself) and
[ADR-0008](../../decisions/0008-cas-primitives-write-once-log-cas-lease.md)
(the `putIfMatch` primitive the registry needs)

**Completion condition:** the object key's membership filter is real — the
writer emits the shortest of `A`/`Z`/`R`/`B` that fits the segment's actual
index population, degrading to `N` only when none fits — and no key ever
exceeds 1024 bytes, for any input including a segment that touches all
10,000 indices. Readers treat an unknown tag and `N` identically: "must read
the header," never "no match." `BinStore` gains the two primitives M1
deliberately left out (`putIfMatch`, multipart) because this is where they
are needed.

⚠️ **This is the slot M1 built and never filled.** `SegmentKey`'s own javadoc
says so: *"M1 writes tag `N` for the filter... the SLOT is here from the
first key so that adding it later is not a key-grammar change."* M2 is that
addition — not a new key-grammar change, if the slot was built correctly. If
it turns out not to fit without changing the surrounding grammar, that is a
spec bug to report, not something to route around silently.

---

## Requirements

- **NFR-12** (object key length ≤ 1024 bytes, including pathological input) —
  the acceptance criterion this milestone exists to prove.
- **FR-10** (consumers keep working without the ingester, via a documented
  fallback ladder down to LIST recovery) — ADR-0003's own cited
  requirement: the filter is precisely what makes that fallback's LIST scan
  affordable at 10,000 indices, not merely possible.
- **NFR-4** (read request rate scales with segments/AZs/nodes, never with
  indices) and **R2** (LIST confined to recovery/GC) — the filter is what
  keeps a 10,000-index recovery walk from costing one GET per segment per
  index; see [cost impact](#cost-impact) below for the actual numbers.
- **FR-8** (pluggable store SPI) — `putIfMatch` and multipart are named
  explicitly in `BinStore.java`'s own M1-subset comment as arriving here.
- **FR-2** (bundle many indices/partitions into one segment) — unaffected;
  this milestone changes what the KEY says about that bundle, not the
  bundling itself.

## Scope

**In.**
- `BinStore.putIfMatch(String key, Body body, Version expected)` on the SPI,
  implemented on `MemoryBinStore` and `LocalFsBinStore`, with conformance
  cases (ADR-0008: version-moved is an empty `Optional`, not an exception;
  the 409/412 distinction where a backend can express it).
- The index-ordinal registry (`<prefix>/ctl/registry/indices.json`): a small,
  CAS-updated (`putIfMatch`) mapping `indexUUID -> dense int ordinal`,
  assigned once per index and cached indefinitely by readers and writers
  (research doc 01 §6; ADR-0008's "~0/s, only on index creation" rate).
- The adaptive tagged filter (research doc 02 §7): `A` (every registered
  index present), `N` (no filter, read the header), `Z<bitmap>` (exact, for a
  scoped ordinal space), `R<RLE>` (run-length, dense-with-gaps), `B<k><bits>`
  (Bloom over index ordinals, the expected default at 10,000-index scale).
  The writer computes candidates from the segment's own distinct-index set
  and emits the shortest that fits within the byte budget, degrading to `N`.
- Reader-side handling: an unknown tag byte and `N` are both "must read the
  header" — never "no match." Property test: **no false negatives, ever.**
- `SegmentKey.headerLenOf()` made robust against filter-payload content (see
  [Risks](#risks) — a base64url payload can legally contain the substring
  `-h`, which the current `lastIndexOf`-based parse does not defend against).
- Golden files per tag, so key formats stay stable across versions
  ([wire-format-change](../../../../../.agents/skills/wire-format-change/SKILL.md)).
- `MultipartWriter multipart(String key)` on the SPI and its backends, for a
  segment that exceeds the configured part-size threshold. Sized by
  `Capabilities.minPartSize`; the 8 MiB default flush size stays under the
  16 MiB threshold from research doc 01 §7, so this is a capability that
  must exist and be conformance-tested, not one M2's own segments are
  expected to exercise by default.

**Out — deliberately, and each already designed elsewhere:**
- Changing `RunEntry`'s own directory-entry shape (UUID → ordinal inside the
  segment's binary header). Research doc 01 §3 calls the current 48 B/run
  overhead "acceptable" and the ordinal shrink "if it becomes a problem" —
  optional, not required. The filter's own ordinal use is confined to the
  **key string**, computed by looking up each run's `indexId` in the
  registry at write time; it does not require the segment's own directory
  to store ordinals instead of UUIDs. Keeping this out of scope avoids a
  second, riskier wire-format change (the segment's binary layout, not just
  the key string) in the same milestone.
- Compaction and retention (doc 06; roadmap M2 does not claim these).
- The sequencer lease (ADR-0002, ADR-0008's other `putIfMatch` consumer) —
  M4's row, not this one. M2 only builds the primitive; M4 is its other
  caller.
- Real S3/GCS/Azure backends. Conformance runs against `MemoryBinStore` and
  `LocalFsBinStore` only, matching M1's own backend scope (FR-8 lists S3/
  GCS/Azure as agreed but nothing in M1 or M2 builds them). The research
  corpus's own testing requirement ("every emitted key round-trips through
  S3, GCS, Azure and local FS," doc 02 §8) is accepted as **not fully
  achievable this milestone** and is not silently narrowed without saying so.

⚠️ **Throwaway must be labelled**, same rule as M1: nothing in this milestone
is expected to be replaced by a later one, so no `SKELETON:` markers are
anticipated — if one turns out to be needed, it is added honestly rather than
retrofitted after the fact.

## Design

**The filter lives in the key, not the segment.** Per research doc 02 §1,
the filter answers "might this segment contain data for index X?" from a
`LIST` result alone, on the recovery/GC path only — a subscribed consumer
never looks at the key (it gets `(key, byteRange)` from the ingester
directly). This is why the filter's cost is judged by recovery-path
economics, not hot-path latency.

**Ordinals, not UUIDs, inside the filter.** A 16-byte UUID makes even a
modest Bloom or bitmap far too large for the ~900-character budget (doc 02
§4: 10,000 UUIDs would need 1,667 base64url characters even as an exact
bitmap). The registry's dense `int` ordinal is what makes any encoding fit
at all — this is why ADR-0008 calls the registry's CAS primitive
`putIfMatch`, not `putIfAbsent`: it is one mutating object, not a sequence.

**The writer picks the shortest fitting encoding, per segment.** Candidates,
in the order research doc 02 §7 lists them: `A` if literally every
registered index is present (rare, but free to check and free to emit — one
character); `Z` (exact bitmap) if the segment is scoped to a small ordinal
space (a mega-index's own shard range, doc 02 §5's "334 chars, 0% FPR" case
— genuinely possible once per-mega-index prefixing exists, not before, so in
practice this tag is reachable code without being the common case yet);
`R` (RLE) if the exact bitmap is dense-with-gaps; `B` (Bloom) as the default
at this project's expected scale (doc 02 §5: ~500 distinct indices/segment
once the per-stream trickle policy — already built in M1's `Accumulator`,
research doc 01 §7b — does its job, well within the 1%–10% FPR budgets doc
02 §3 computes); `N` if nothing fits inside the byte budget. The order
matters because it is cheapest-first: computing `A` costs one registry
lookup per distinct index in the segment against the registry's own total
count, computing `Z`/`R` needs the same distinct set sorted into a scoped
ordinal space, and `B` is tried only once the others are known not to fit.

**The Bloom filter itself:** one 128-bit hash (research doc 02 §7 names
xxh3-128 or murmur3-128), split into two halves `h1, h2`, then
Kirsch–Mitzenmacher `h_i = h1 + i·h2 (mod m)` for the `k` probe positions —
this project's own accepted way to get `k` independent-enough hash
positions from one hash computation rather than `k` separate ones.

**Parsing hazard found while planning this, not yet while implementing it:**
`SegmentKey.headerLenOf()` currently does
`key.lastIndexOf("-h")` then the next `-` after it. Base64url's alphabet
(`A`–`Z`, `a`–`z`, `0`–`9`, `-`, `_`) legally contains `-h` as two adjacent
characters in an otherwise ordinary Bloom or bitmap payload — a filter
component placed AFTER `-h<headerLen>-` in the key could then contain a
LATER `-h` occurrence that `lastIndexOf` would find instead of the real one,
corrupting the parsed header length for every reader. M1 never hit this
because its only filter value was the literal string `N`, which cannot
contain `-h`. Fixed here by parsing `-h<digits>-` from a bounded window
immediately after the pod-id/sequence component (a fixed-shape prefix that
ends before the filter begins), not by scanning the whole key for the last
match.

## Cost impact

**Hot path: unchanged.** The filter is written once per segment (already
happening — M1 writes the literal `N`) and never read on a subscribed
consumer's path. No new PUT, GET or LIST is added to ingestion or delivery.

**Recovery/GC path: this is the entire point.** Research doc 02 §2's
worked number, unchanged by this implementation: recovering over 1,000
segments costs one LIST plus matching GETs with the filter, versus one LIST
plus 1,000 header GETs without it — **~80× cheaper**, and to the exact ratio
doc 02 §2 already computed ($5.0×10⁻⁶ vs $4.05×10⁻⁴). This milestone does
not re-derive that number; it makes the design that number describes real.

**The registry:** ADR-0008's own rate estimate, "~0/s, only on index
creation" — cacheable indefinitely by every reader and writer, revalidated
with one conditional GET. Not a request-rate concern at any realistic index
creation rate.

**R2 held:** LIST stays confined to recovery and GC; nothing in this
milestone gives the hot path a reason to call `list`.

## Acceptance criteria

Each is checkable by something other than an opinion.

1. `putIfMatch` is implemented on `MemoryBinStore` and `LocalFsBinStore`,
   passes the store conformance suite (a version-moved write returns an
   empty `Optional`, never throws; a matching version succeeds and advances
   the version), and `Capabilities.conditionalWrites` reflects both
   `putIfAbsent` and `putIfMatch` support, checked at startup per ADR-0008.
2. `MultipartWriter` is implemented on both in-tree backends and passes
   conformance: parts below `minPartSize` are refused (where the backend can
   express that), the assembled object reads back byte-identical to what was
   written part-by-part.
3. The ordinal registry assigns a dense `int` per `indexUUID`, is
   CAS-updated (`putIfMatch`), survives concurrent first-registration of the
   same index (loser reads the winner's assignment, does not retry into a
   second ordinal for the same UUID), and is readable with one conditional
   GET after the first read (no repeated full reads).
4. The adaptive filter round-trips: encode then decode reproduces the exact
   membership the writer computed, for each of `A`/`N`/`Z`/`R`/`B`, pinned by
   a golden file per tag.
5. **No false negatives, ever** — a property test across randomised index
   populations: for every encoded filter, every index actually present in
   the segment reports "might be present" (never "definitely absent").
   False positives are expected and bounded (below), never a failure.
6. Measured false-positive rate is within tolerance of the analytic
   prediction (research doc 02 §3's table) at n = 100, 500 and 1,000 distinct
   indices per segment.
7. **NFR-12, the pathological case**: a segment touching all 10,000
   registered indices produces a key that degrades to `N` rather than
   overflowing — and the emitted key is measured, not assumed, to be
   ≤ 1024 bytes for every tag, including the adversarial input that makes
   every other tag too large to fit.
8. `SegmentKey.headerLenOf()` correctly extracts the header length when the
   filter payload contains the literal substring `-h` — the parsing-hazard
   case named in Design, proven by a specific adversarial fixture, not
   inferred from the fix alone.
9. Readers treat an unknown filter tag exactly like `N`: "must read the
   header." Proven by a fixture using a tag byte that does not exist in the
   current grammar (simulating a future writer's format this reader has not
   seen yet) and asserting the reader still finds the data, rather than
   silently treating the unknown tag as "no match."
10. Every emitted key round-trips through `MemoryBinStore` and
    `LocalFsBinStore` unchanged. (Real S3/GCS/Azure round-tripping is
    explicitly out of scope — see Scope — and this criterion says so rather
    than silently narrowing the research corpus's own broader claim.)

## Test plan

| Tier | What | Fails first against |
|---|---|---|
| T0 | `putIfMatch` conformance (both backends): version match succeeds, mismatch returns empty, absent key with a non-null expected version fails distinctly from a mismatch | a naive `put`-then-check race, or an exception where an empty `Optional` is contracted |
| T0 | `MultipartWriter` conformance: parts assemble byte-identical; a part below `minPartSize` is refused where expressible | a writer that silently drops or reorders parts |
| T0 | Ordinal registry: first registration wins, concurrent first-registration converges on one ordinal, a later read of an already-known index needs no further conditional GET beyond the cached copy | a registry that assigns two ordinals to the same UUID under a race |
| T0 | Filter encode/decode round-trip, one golden file per tag (`A`/`N`/`Z`/`R`/`B`) | a codec that cannot reproduce its own encoding, or a golden file silently regenerated instead of asserted against |
| T0 | No-false-negative property test, randomised index populations at several sizes | a Bloom/bitmap bit computed from the wrong hash or the wrong ordinal |
| T0 | Measured FPR vs analytic prediction, n = 100/500/1,000 | a `k` or `m` computed from the wrong formula |
| T0 | NFR-12 pathological case: 10,000-index segment, key ≤ 1024 bytes, degrades to `N` | a filter that overflows instead of degrading, or a degrade path that is never reached because a smaller encoding is (wrongly) reported as fitting |
| T0 | `headerLenOf()` adversarial fixture: a filter payload containing the literal substring `-h` | the current `lastIndexOf`-based parse, which this milestone's own design section names as already broken for this input |
| T0 | Unknown-tag fixture: a byte outside `{A,N,Z,R,B}` | a reader that treats an unrecognised tag as "no match" instead of "must read the header" |
| T0/T1 | Key round-trip through both in-tree backends | a backend-specific character escaping bug (base64url's `-`/`_` are unusual in some key-naming conventions) |

Each new test named above must have a red record (`scripts/tdd-red.sh`)
before the code it tests exists, same as every task in M1.

**Coverage:** ≥95% line / ≥90% branch on changed code; **mutation ≥80%** on
changed code, once `check-mutants.sh` exists (M0.14) — until then, the
mutations named in each task's own commit message are the evidence, exactly
as M1's commits have done throughout.

**Suites this milestone extends:** the store conformance suite (adds
`putIfMatch` and multipart cases to every backend already in it); no new
tier is introduced.

## Risks

| Risk | What would reveal it | If it happens |
|---|---|---|
| The `-h` parsing hazard (Design) turns out to affect more than `headerLenOf` — e.g. `hourPrefix` or key construction itself | the adversarial fixture in acceptance criterion 8, or a broader audit of every place a key string is parsed rather than only constructed | Audit every `SegmentKey` parse site, not just the one named; this is a class of bug, not a single line |
| The filter's ordinal lookup at write time requires reading the registry once per distinct index in a segment, which could become a per-flush cost if not cached | a flush-latency regression once real segment volumes are measured, or the registry being read via the object store rather than an in-memory cache after the first read | Cache the registry client-side per pod, invalidated only by a conditional GET (ADR-0008's own framing); do not re-fetch per flush |
| `putIfMatch` semantics genuinely differ enough between S3's `If-Match` and GCS's generation match that the conformance suite (built against `MemoryBinStore`/`LocalFsBinStore` only) does not catch a real backend's divergence | only measurable once a real S3/GCS backend exists — explicitly out of this milestone's scope | Recorded as a known gap, not silently assumed away; a future milestone building the real backends inherits this risk explicitly |
| The Bloom filter's measured FPR does not match the analytic prediction closely enough at the tested `n` values, suggesting a hashing or bit-indexing bug rather than expected statistical variance | acceptance criterion 6 failing outside a reasonable tolerance band across repeated seeds | Do not widen the tolerance to make the test pass — find the indexing bug. A systematic (not random) deviation is the signal |

## Tasks

Each is one commit, cites a requirement, and leaves the tree green.

| ID | Task | Serves |
|---|---|---|
| M2.0 | `BinStore.putIfMatch` on the SPI, `MemoryBinStore` and `LocalFsBinStore`, conformance cases (ADR-0008) | FR-8 |
| M2.1 | `Capabilities.conditionalWrites` reflects both `putIfAbsent` and `putIfMatch`, checked at startup, failing loudly if either is missing | FR-8 |
| M2.2 | `MultipartWriter` on the SPI and both in-tree backends, conformance cases | FR-8 |
| M2.3 | The index-ordinal registry: read, first-registration-wins CAS assignment, client-side caching | FR-8 |
| M2.4 | The adaptive filter codec: `A`/`N`/`Z`/`R`/`B` encode and decode, one golden file per tag | NFR-12 |
| M2.5 | The Bloom filter (`B`): Kirsch–Mitzenmacher construction, no-false-negative property test, measured-FPR test at n=100/500/1,000 | NFR-12 |
| M2.6 | Fix `SegmentKey.headerLenOf()`'s parsing hazard FIRST (Design), with the adversarial `-h`-in-payload fixture, THEN wire the filter into `SegmentKey`'s writer path in the same commit: compute candidates from the segment's distinct-index set, emit the shortest that fits, degrade to `N`. ⚠️ Merged deliberately, not sequenced as two tasks — round-1 spec review found that landing the writer change before the parsing fix ships a real header-length corruption for any produced key whose filter payload happens to contain the literal substring `-h`, for however long the gap between two separate commits lasts. The fix has no dependency on the writer change, so there is no reason to accept that window | NFR-12 |
| M2.7 | Reader-side unknown-tag handling: unrecognised tag treated identically to `N`, proven by fixture | NFR-12 |
| M2.8 | NFR-12 pathological-input test: 10,000-index segment, key ≤ 1024 bytes, degrades to `N` | NFR-12 |
| M2.9 | Added post-decomposition (found writing this milestone's own `VERIFIED.md`): criterion 10 / the "key round-trip through both in-tree backends" test-plan row had no task. A key whose filter is a real base64url payload round-trips, byte-identical and findable, through both `MemoryBinStore` and `LocalFsBinStore` | NFR-12 |
