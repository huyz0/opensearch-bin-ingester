# 0044. No production fetcher ships in M5, and the event's coordinates stay

Status: accepted
Date: 2026-09-14
Requirements: FR-6, NFR-4
Research: docs/research/30-design-space/10-client-library-and-fetch-modes.md §7-§8
  (the client's responsibilities and the fallback ladder — §8's degradation claim
  is corrected by this record and carries a revision banner);
  docs/research/30-design-space/04-discovery-and-tailing.md §2c (the event shape
  these coordinates come from).

## Context

ADR-0043 put the grant on the wire as `io.github.huyz0.os.biningester.format.Grant` and brought back
`byteStart`/`byteLen` as coordinates, then **reopened its own decision (b)** when
M5.45b's review established that the consumer's request is a plain whole-object
GET: the coordinates buy nothing at the fetch, and `SegmentReader` reads the
directory regardless. It assigned the question to M5.45c by name. M5.45c was in
turn split, and **M5.45f — this record — inherits two questions that have to be
answered together**, because the first settles the second: a fetcher that cannot
range at all makes the coordinates dead by construction, and one that can makes
a bounded read a live option.

**Four facts about the tree today, each counted rather than recalled:**

| Fact | Count | Where |
|---|---|---|
| Backends that can presign | **0** | `MemoryBinStore`, `LocalFsBinStore` — neither declares `presign` |
| Production `SubscriptionTransport` implementations | **0** | the interface and `ConsumerClient` in `client`, `NodeSubscriptions` in `plugin`; every implementation is a test one. **M1.11b is `todo`** |
| Modules exempt from `check-io-seam` | **1** | `binstore-backends`, named rather than derived |
| `client`'s production dependencies | **1** | `api(project(":format"))` |

So there is no presigning backend for a fetcher to fetch from, and no production
network client anywhere in the consumer path — not even the subscription hop the
fetch presupposes.

⚠️ **`check-module` already permits HTTP in `client`.** Its `NO_HTTP` list is
`format binstore-spi sequencer ingest`, and its own comment says *"NOT `client`
or `plugin` — the consumer must talk to the ingester, so an HTTP client there is
the design, not a violation."* The obstacle is not the module rule. It is
`check-io-seam`, which bans `java.net` and `javax.net` in every `src/main` file
outside the one exempt module, and non-negotiable 7 behind it.

## Decision

**(a) No production fetcher ships in M5. M5.45g ships a test implementation, and
the gap is named here rather than left to be discovered.**

The seam is real and lands in `client`; what does not land is anything that
opens a socket. The choice between a new adapter module, a second
`check-io-seam` exemption, and a JDK `HttpClient` behind the seam is deferred to
the row that has a caller for it — **not** deferred as a preference, but because
all three are decisions about where an adapter lives, and every one of them
would today produce code that:

- has nothing to fetch from (0 presigning backends), and
- is reached through a transport that does not exist (0 production
  `SubscriptionTransport`s, M1.11b `todo`), so
- cannot be exercised end to end by anything, in any test tier.

⚠️ **AND IT WOULD ARRIVE OUT OF ORDER.** A direct fetcher is the consumer's
*second* network client. The first is the subscription hop itself. Choosing
where HTTP lives in the consumer path on behalf of a fetcher, while the hop that
must already have solved the same problem is unwritten, is choosing with the
smaller of the two callers in hand.

**(b) `byteStart` and `byteLen` STAY on the wire, and ADR-0043's justification
for them is REPLACED rather than repaired.**

ADR-0043 says what they buy is *"coalescing without a directory read"* — a node
holding K runs unions them into one **ranged** GET. M5.45c's review established
that the union cannot be a ranged GET: `SegmentReader.open` checks the footer magic
before the directory, so a prefix ending at the last wanted run throws
`"segment is truncated: no footer magic"`; `ConsumerClient.decodeInto` opens the
whole array with no partial path; and `[0, objectLen)` cannot be named because
nothing the consumer holds carries the object length — `SegmentKey` carries
`h<headerLen>`, the header's extent. **That justification is dead, and this
record does not keep it alive.**

⚠️ **AND "NO RANGE IS EXPRESSIBLE" IS TOO STRONG, WHICH THIS RECORD FOUND BY
SWEEPING ITS OWN CLAIM BEFORE A FOURTH REVIEW ROUND RATHER THAN AFTER.** Three
ranges must be told apart:

| Range | Expressible today? | Why |
|---|---|---|
| `[0, PREAMBLE + headerLen)` — preamble **plus directory** | **YES** | exactly what `SegmentKey`'s `h<headerLen>` is for. ⚠️ A CONSUMER names it from `SegmentKey.headerLenOf` + `SegmentFormat.PREAMBLE_BYTES`, both in `format`, which `client` depends on. `SegmentPublisher.headerRangeEndInclusive` computes the same bound and `SegmentPublisherTest` fetches it — but it lives in `ingest`, which no consumer depends on, so it is the existence proof and not the call site |
| a run's DATA slice `[byteStart, byteStart + byteLen)` | **YES** as a request | the event carries the coordinates; what is missing is a reader that accepts it |
| `[0, objectLen)` — the whole object as a range | **NO** | nothing the consumer holds carries the object length |
| a prefix `[0, max(byteStart + byteLen))` | **NO** | `SegmentReader.open` checks the FOOTER magic before the directory, so it throws |

⚠️ **SO A BOUNDED READ IS TWO EXPRESSIBLE REQUESTS, NOT AN IMPOSSIBILITY** —
header range, then slice — **and the codec objection dissolves with it**: a
reader that fetched the header HAS the directory, so it has the per-run
`codecFlags` that a slice-only GET would have been missing. What is actually
absent is narrower than earlier drafts of this record said: a `SegmentReader`
entry point that takes a directory and a slice SEPARATELY. That is a new method,
**not a format change**, which makes M5.66 cheaper than its first framing and
makes these fields more clearly worth keeping, not less. The decision below is
unchanged; the reason for it is now the right one.


⚠️ **AND THE JUSTIFICATION THAT FIRST REPLACED IT IS REFUTED TOO — BY THE
CORRECTION ABOVE, WHICH IS THE THIRD TIME THESE TWO FIELDS HAVE LOST THEIR
REASON.** An earlier draft of this record kept them as *"the only thing that
makes a bounded read possible later"*. They are not.
`RunEntry(RunKey, int recordCount, long byteStart, int byteLen, …)` carries the
same extents **in the directory**, `SegmentReader.find(RunKey)` resolves them,
and the two-request design above fetches that directory FIRST — it has to, for
`codecFlags`. So a reader doing a bounded read already holds every run's extent
before it issues the slice GET. ADR-0043 says so in the sentence this record
quotes as dead: *"Without them it must read the directory first to learn the
same thing."*

⚠️ **AND A DUPLICATE ACTED ON IN PARALLEL OWES A TIE-BREAK RULE, WHICH THIS
RECORD STATES RATHER THAN LEAVES TO THE IMPLEMENTER: THE DIRECTORY WINS.** The
sequential design is safe by construction — there is only one copy of the extent
in play — but the parallel one these fields exist to enable reads the slice using
the EVENT's copy while the directory carries its own. If they disagree the
consumer has fetched the wrong bytes under a valid signature, which is ADR-0032's
silent data error one layer out. So the event's coordinates are a HINT that
buys concurrency, the segment's own directory is the truth, and a bounded reader
that finds them disagreeing must refuse rather than decode. M5.66 carries the
assertion.

**What actually survives is one round trip, and that is the whole of it.** With
the event's coordinates a consumer can issue the slice GET **without waiting for
the header**, concurrently rather than after it; without them the two requests
are strictly sequential. So the fields buy **parallelism, not possibility** —
and they are kept on that, at 5.4% of the subscription stream.

⚠️ **THAT IS A LATENCY ARGUMENT IN A PROJECT WHOSE PITCH IS TRADING SECONDS OF
LATENCY FOR COST, AND IT IS DELIBERATELY A WEAK ONE.** It is enough to keep the
fields today only because the counter-move — removal — is a format change whose
value is also unmeasured, and because nothing production reads or writes v3 yet,
so the decision stays cheap to revisit in either direction. ⚠️ **M5.66 NOW OWNS A
REAL QUESTION RATHER THAN A RHETORICAL ONE**: is one round trip on the `direct`
path worth 5.4% of the subscription stream? That is answerable with a number,
and it is the number that should decide whether these fields stay.

⚠️ **AND THE ANSWER LEANS TOWARDS NO, WHICH THIS RECORD SAYS RATHER THAN LEAVES
FOR THE NEXT READER TO DISCOVER.** `cost.md` and `CostTable` price REQUESTS
only, and `02-cost-model.md` says it outright — *"Bytes moved between S3 and
same-region compute are free. Only requests cost money"*. ⚠️ The tree DOES hold
a per-byte term — `FetchPolicyConfig.DEFAULT_CROSS_AZ_MICRO_DOLLARS_PER_GB`,
R12's $0.02/GB — but it prices CROSS-AZ traffic on the peer mesh, not an
object-store GET, whose bytes are free in any AZ. So in region a bounded read is one extra GET for no
dollars saved, and the bytes it avoids buy TIME rather than money — which is
precisely the currency this project trades away. The fields are kept here on the
reversal asymmetry and on nothing stronger, and M5.66 should expect to find that
removing them is right unless a LATENCY budget says otherwise. ⚠️ **AND THE
CROSS-AZ ESCAPE IS NOT ONE**, which an earlier draft of this sentence offered:
R12's $0.02/GB is EC2 cross-AZ traffic (ADR-0012's peer mesh), and the bounded
read's extra request is an OBJECT-STORE GET, whose bytes are in-region free in
any AZ. An owner taking that arm would price the bounded read hundreds of times
in favour of keeping, on a rate no request on the `direct` path incurs. The only
currency left is time. That is a prediction, not a decision, and it is recorded
so the row is not scoped as a formality.

⚠️ **THE NUMBER, because an alternative rejected without one is an assertion —
and this is the ONE place it is derived. Every other site points here rather
than restating it; three review rounds were spent on copies of this figure
disagreeing with each other.**

**Measured**, from the golden files, which is the only part of this that is a
measurement:

- `subscription-event-direct-v3.bin` = **128 bytes**
- `subscription-event-direct-v3-whole.bin` = **123 bytes** (same event, grant, no
  range)
- delta **5 bytes** = that fixture's two uvarints (`byteStart = 4096` → 2 bytes,
  `byteLen = 65536` → 3)

`encode` writes the presence flag unconditionally in v3, so both files pay it.
**Removing the fields from the format saves the varints AND the flag**: 6 bytes
of that 128-byte fixture, **4.7%**.

**Modelled**, at the 8 MiB / ~1,600-run segment this project actually writes —
and modelled is not measured, which three sites in an earlier draft called it:

- `byteStart` reaches 8,388,608 → 24 bits → **4 uvarint bytes**
- `byteLen` averages 8 MiB / 1,600 ≈ 5.2 KiB → 13 bits → **2 bytes**
- plus the flag = **7 bytes**, on an event of 128 − 5 + 6 = **129 bytes**

| Denominator | Arithmetic | Result |
|---|---|---|
| an event carrying a range | 7 / 129 | **5.4%** |
| the subscription stream for one segment | 1,600 × 7 = 11,200 of 1,600 × 129 = 206,400 | **5.4%** |
| the segment those events describe | 11,200 / 8,388,608 | **0.134%** |
| that stream against the segment | 206,400 / 8,388,608 | **2.46%** |
| per node, at ADR-0042's ~400-event share | 2,800 / 8,388,608 | **0.033%** |

Cross-check, which is what an earlier draft's pair of figures failed:
0.134 / 2.46 = **5.4%**, matching the first row, and the stream figure is
**40×** the segment one.

⚠️ **THE DECISION IS TAKEN ON 5.4%**, the subscription stream — the bytes travel
there, not on the segment. It is the honest price of keeping these fields, it is
small against a stream that is itself 2.46% of the segment it describes, and
neither figure is object-store requests, which is the currency cost.md governs.
⚠️ M5.66's measured crossover must derive its own number rather than reach for
any of these.

⚠️ **AND "NO CONSUMER TODAY" IS RECORDED IN THE RECORD RATHER THAN DISCOVERED A
THIRD TIME.** Zero readers in the tree use the event's copies. The bounded reader
that would is **M5.66**, which this record creates so the question has an owner
instead of being reopened again — it is the third time these two fields have been
argued (ADR-0042 removed them, ADR-0043 restored them, ADR-0043 then reopened
itself).

**(c) security.md rule 3's range half is NOT closed, and it now has an owner.**

ADR-0043 records that *"rule 3's range half has **no owner**"* and hands the
settling to M5.45f by name. Answering (a) and (b) without answering that would
leave the chain reading ADR-0043 → M5.45f → done with the standard's clause
marked settled having never been addressed, which is worse than leaving it
visibly open. So, explicitly: **the grant is scoped to one key and to no range,
and "where possible one range" is not possible today** — but NOT because a range
cannot be NAMED, which Decision (b) above sets out that two can be. It is
because no reader in the tree issues one, so there is no range for a signature
to cover. ⚠️ An earlier draft of this sentence read "no range is expressible for
the reasons under (b)", citing the text that refutes it, and would have had a
reader decline the clause as physically impossible where this record documents
it as merely unbuilt. That is
a statement about capability rather than a decision to forgo the rule. **M5.66
owns it, and its criterion 4 names this rule and this standard** — review
measured that handing a clause to a row whose criteria do not mention it is how
ADR-0043 lost this same clause to M5.45b: the row takes an arm, corrects the
sites its criteria enumerate, marks itself done, and the standard is closed by
nobody. M5.66 is the row that either builds a bounded read — at which point a range
becomes expressible and rule 3's second half becomes answerable — or takes the
fields off the wire, at which point the clause is closed by declining it with a
reason. Either way the answer arrives with the reader, not before it.

## Alternatives considered

**(a1) A new adapter module, exempt from `check-io-seam`, that `plugin` wires
in.** Rejected *for now*, not on the merits: it is the most likely eventual
answer, since it mirrors `binstore-backends` exactly and keeps `client` pure.
The cost is a second exemption, and the exemption list is *named, not derived* —
`check-io-seam`'s own comment records that deriving it from `implements` was
measured wrong at 11 files. A second name is a permanent widening of the one
gate that stands behind non-negotiable 7, and buying it for code with no caller,
no backend and no transport is the worst moment to price it.

**(a2) A second exemption for `client` itself.** Rejected. `client` is bundled
into the OpenSearch node process through `plugin`, and `check-module`'s
`NO_CLOUD="client plugin"` exists to keep that JVM's dependency surface minimal.
Exempting the module that ships inside someone else's process is the widest of
the three options and the hardest to narrow later.

**(a3) A JDK `java.net.http.HttpClient` behind the seam, in `client`.**
Rejected. It needs no coordinate, so `check-module`'s classpath grep cannot see
it — the gate's own comment says so: *"`java.net.http` needs no coordinate at
all… NO classpath check can see it."* `check-io-seam` would catch the import
today, and the way past it is to exempt `client`, which is (a2). Reaching for
the option whose only obstacle is the gate is how a gate gets weakened.

**(b1) Remove `byteStart`/`byteLen` from the event.** Rejected, and this was the
closest call in this record. It is genuinely cheapest **now**: no production
writer, no production reader, and no v3 byte has ever crossed a real process
boundary, so removal is contained to `format`, both readers, both writers, the
fakes, six golden files and two ADRs — one mechanical commit under
non-negotiable 8. What defeats it is the asymmetry running the other way: at
**5.4%** of the subscription stream — the figure Decision (b) is taken on, not
the 0.13% segment-denominated one — keeping them is cheap, while removal
followed by the re-addition a bounded read would require is **two** format
changes against zero.

⚠️ **AND THIS ALTERNATIVE IS NOW ARGUED AGAINST A MUCH WEAKER KEEP-CASE THAN
WHEN IT WAS FIRST REJECTED**, which a reader should weigh rather than inherit.
Decision (b) above no longer claims the fields make a bounded read POSSIBLE —
the directory carries the same extents — only that they save one round trip.
Round 5 review put it plainly: *"(b1), the closest call in this record, was
rejected against"* a justification that no longer stands. It is still rejected,
on the reversal asymmetry alone and on nothing else: removal is one format
change now against an unmeasured benefit, and **M5.66** is where the number that
should decide it gets taken. If that number says one RTT is not worth 5.4%, this
alternative becomes the decision and this record is superseded, not patched.

⚠️ **AND THE CORPUS ARGUMENT THAT STOOD HERE IS WITHDRAWN, BECAUSE THIS COMMIT
RETIRES IT.** An earlier draft rejected removal partly on the ground that *"the
corpus designed for ranges throughout (doc 04 §2c, doc 10 §7's 'merge adjacent
ranges before requesting, in every mode')"* — and both of those lines carry a
revision banner added by this very commit, sixty lines below this paragraph.
Citing as live design the text you are retiring is not an argument. What
survives of it is narrower and still true: the corpus designed for ranges, that
design is not reachable today, and **M5.66** is where it becomes reachable or is
abandoned. Removal remains the closest call, and it loses on the reversal
asymmetry alone.

**(b2) Keep them and keep ADR-0043's justification.** Rejected as false. The
coalescing-without-a-directory-read claim requires a node to union its runs into
one ranged GET *and skip the directory*, and the directory is exactly what a
bounded read must fetch first. Leaving it standing is the defect this milestone has corrected most
often: a justification that outlived the thing justifying it.

**(b3) Land the bounded reader now, so they earn their place immediately.**
Rejected as out of scope, **not as expensive** — and an earlier draft of this
paragraph rejected it as "two things the current layout forbids", including the
codec objection Decision (b) establishes dissolves once the header is fetched.
What it actually needs is ONE new `SegmentReader` entry point taking a directory
and a slice separately, because `open` checks the footer magic before the
directory so no prefix it accepts exists. That is a new method, **not** a format
change, and it is **M5.66**. Mispricing it as a format change is how that row
gets scoped at six golden files and three ADRs, or has its removal arm taken for
the wrong reason.

## Consequences

**`direct` is complete at the ingester and deliberately incomplete at the
consumer, and the asymmetry is now written down.** M5.45d serves it; M5.45g gives
the consumer a seam and a test fetcher; nothing in the tree fetches with a grant
in production, and nothing can, because no backend mints one.

**M5.45g's criterion 6 is the enforcement, and it is the only one.** No
production fetcher wired into a consumer path until M5.45h. This record is where
a reader choosing the adapter learns the ordering exists; the numbered obligation
is in the row.

⚠️ **What this forecloses: nothing, deliberately.** Both (a) and (b) are
decisions to *not* spend a reversal. (a) keeps all three adapter options open at
the cost of one milestone's delay; (b) keeps **one round trip** available on a
bounded read, at a cost of **5.4% of the subscription stream** (0.134% of the
segment). ⚠️ AN EARLIER VERSION OF THIS LINE SAID (b) "keeps the bounded read
possible", 175 lines below the paragraph establishing that the fields buy
parallelism and NOT possibility — and it is false on this record's own (b3) as
well: the bounded read needs one new `SegmentReader` method and no format
change, so removal forecloses nothing. This is the summary a re-pricer quotes,
so it mattered: M5.66's owner could measure one RTT as not worth 5.4%, read that
removal forecloses the bounded read, and decline the arm this record says would
supersede it.

⚠️ **What it makes harder is honesty about `direct`'s status.** With the mode
served, elected and encodable, every surface reads as though `direct` works.
It does not reach a consumer, and three separate places now have to keep saying
so: this record, M5.45g's cell, and `SubscriptionHub.publishSegment`'s comment.

**Research doc 10 §8 is corrected**, with a ⚠️ revision banner. It says *"Because
every event carries coordinates regardless of mode, every mode can degrade to
every other mode. That is the property that makes shipping all three safe."*
Every degradation path it lists is a **whole-object** read — corrupt inline is
*"ignore it and fetch by coordinates"* where `SegmentReader` finds the run from
the directory, and tier 2+ break-glass is *"whole-object, no cleverness"* in its
own words. The coordinates are not what makes degradation safe; the segment's
own directory is. The conclusion (all three modes are safe to ship) survives; the
reason given for it does not.

### The `byteStart`/`byteLen` sweep, as M5.45f's criterion 3 requires

⚠️ **THE SWEEP'S FIRST RESULT IS THAT MOST HITS ARE A DIFFERENT THING.** The
segment DIRECTORY has fields of the same name, they are live, read on every
decode, and nothing here touches them. A sweep that "corrected" those would
break the format. Patterns: `byteStart`, `byteLen`, `coordinates`, `byte range`,
`M5.44`, `ADR-0043`, `VERSION_3`.

| Site | Disposition |
|---|---|
| `format/…/SegmentFormat.java`, `SegmentWriter.java`, `GoldenSegmentV1Test`, `SegmentWriterTest`, M1 `SPEC.md` | **Not this question** — the segment directory's own coordinates, live and used on every decode |
| `format/…/RunEntry.java`:19, `SegmentReader.find(RunKey)` | ⚠️ **THIS IS THE QUESTION, and an earlier version of this table said "not this question" alongside the row above.** `RunEntry` carries `byteStart`/`byteLen` in the DIRECTORY and `find` resolves them, so a reader that fetched the header — which the two-request design requires, for `codecFlags` — already holds every run's extent. That is what makes the EVENT's copies redundant for possibility, and it is why Decision (b) now keeps them for one round trip and nothing more. The same false-confirmed shape as the doc 13 row below, and the one that mattered most |
| `format/…/SubscriptionEvent.java` | **Kept**; javadoc corrected to say no reader consumes them and to name M5.66 |
| `docs/…/decisions/0043-…md` Decision (b) | **Amended** by this record; its justification replaced, not repaired |
| `docs/…/decisions/0041-…md`:114 | **Confirmed** — says a range *"would have to come from the subscription protocol's coordinates"*, which is still true and is exactly what M5.66 would use |
| `ingest/…/SegmentDelivery.java`:32 | **Confirmed** — cites doc 04 §2c for the CONTRACT (what a delivery is about), not for the fetch |
| `ingest/…/FetchPolicyConfig.java`:19 | **Confirmed, different sense** — *"above it, send coordinates"* means send a reference rather than the bytes, i.e. do not inline. Nothing to do with these two fields. ⚠️ Round-3 review named this a sweep miss because it matches none of the five original patterns; `coordinates` was added to the list for that reason, and this row is the result |
| `format/…/GoldenSubscriptionEventTest`, the six golden files | **Unchanged** — the fields stay, so the bytes stay |
| `docs/research/30-design-space/10-…md` §8 | **Corrected**, revision banner — see Consequences |
| `docs/research/30-design-space/10-…md` §7:182 | **Corrected**, revision banner. *"Read coalescing — merge adjacent ranges before requesting, in every mode"* survived the §8 banner, and alternative (b1) below **quotes this very line** as live corpus design. The coalescing stands and is M5.45h's; the ranges do not. Round-2 review found it, and it is the one-of-two-places defect a fifth time in this family |
| `format/…/SubscriptionEventTest.java`:32, `GrantTest.java`:12 | **Confirmed, nothing to correct** — `coordinatesEvent` and an `M5.44` citation, the same "different sense" class as the `FetchPolicyConfig` row. Listed because the artifact is only worth what its enumeration is |
| `docs/research/30-design-space/04-…md` §2 body (the "Metadata only" and "Coalescable" bullets) | **Corrected**, revision banner. ⚠️ Found by sweeping this record's own CLAIM — "ranged GET", "merge adjacent", "fall back" — rather than the field names, BEFORE a fourth review round rather than after, which is what the three rounds before it cost. It says the plugin fetches `segment[byteStart, byteStart+byteLen)` and that `FetchCoalescer` turns events into one ranged GET; no reader consumes those coordinates, and the merge is into one WHOLE-OBJECT GET (M5.45h) |
| `docs/research/50-open-questions.md` Q8 | **Corrected**, revision banner. Its last sentence — *"Every event carries coordinates even when it also carries inline bytes, so a subscriber can always fall back"* — is the doc 10 §8 claim one document over, in **the decision log AGENTS.md routes a reader to first**. Round-3 review found it. The ANSWER (push is best-effort) stands; the reason given for it does not |
| `docs/research/30-design-space/04-…md` §2c banner | **Corrected**, revision banner. ⚠️ Round-1 review found this absent from an earlier draft of this table and called it blocking, correctly: the banner asserts all three claims this record retires, including the span `[0, max(byteStart+byteLen))` that `SegmentReader.open` refuses — and AGENTS.md says a banner WINS over the prose it corrects, so M5.66's owner, routed there by the `research` skill, would have implemented it. ADR-0043:74-77 had already killed that span and the correction did not reach here: the one-of-two-places defect, a fourth time |
| `format/…/SubscriptionEventGrantTest.java` | **Corrected** — its class javadoc said bringing the fields back is what closing security.md rule 3's *"where possible one range"* means, and *"It does."*; Decision (c) above says it does not. The assertions are untouched: review MEASURED that forcing `decode` to return `RANGE_ABSENT` reds three cases, so the fields are unused but NOT unpinned |
| `format/…/SegmentReaderTest.java`, research doc 01 §3 | **Not this question** — the segment directory again, same as the `RunEntry` row |
| `docs/research/…/13-…md` §4 and §5 | **Corrected**, revision banner. ⚠️ An earlier version of this table dispositioned doc 13 as "the segment directory again", and that was **FALSE OF TWO OF ITS THREE HITS**: §5's event shape carries `byteStart: 184320, byteLen: 65536` — the subscription event, not the directory — and §4's *"on the hot path none of them is read: the ingester pushes exact byte ranges"* is the claim this record retires. ⚠️ **A FALSE "CONFIRMED" IS WORSE THAN AN OMISSION**, and round-2 review said so: this artifact exists so "corrected" can be told from "not looked at", and it said "looked at" about lines that were not |
| `docs/research/…/03-…md`:153 | **Not this question** — a THIRD structure, the commit log delta's per-stream coordinates, mis-dispositioned the same way in the earlier draft |
| `docs/…/decisions/0042-…md` | **Confirmed** — it REMOVED these fields and its reasoning for removing `codec` is untouched; ADR-0043 restored them and this record replaces the restoration's justification without disturbing ADR-0042 |
| `docs/internal/product/backlog.md` — rows M5.44, M5.45c, M5.45f, M5.45g, M5.66 | ⚠️ **Corrected, and ABSENT from an earlier version of this table** although the file matches four of the seven patterns. Round-6 review measured the cost: M5.66's cell — added by this very commit — still kept the fields "on the ground that they are the only thing making a bounded read possible later", the exact sentence this record quotes as dead, and `next-task` sends that row's owner to the cell BEFORE the ADR. Criterion 3 says "the table IS the enumeration", so a site missing from the table is a site never looked at |
| `docs/internal/product/milestones/M5/SPEC.md`:301 | **Confirmed** — the cross-AZ crossover row, which uses "coordinates" in `FetchPolicyConfig`'s sense (send a reference rather than the bytes) |

⚠️ **THIS TABLE IS THE SINGLE ENUMERATION, AND ROWS THAT NEED TO ACT ON IT MUST
POINT AT IT RATHER THAN COPY IT.** Three review rounds of this record each found
one more site, and each fix that enumerated sites inline went stale against the
next round's own edits — M5.66's criterion 3 said "FOUR sites" and was already
seven by the time it was written, three of them added by the commit that wrote
it. Enumerating in two places is the defect this record spent five findings on.
