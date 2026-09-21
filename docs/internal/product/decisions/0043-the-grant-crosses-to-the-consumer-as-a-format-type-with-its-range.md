# 0043. The grant crosses to the consumer as a format type, with its range

Status: accepted — ⚠️ **amended 2026-09-14 by ADR-0044** on decision (b)'s
  JUSTIFICATION only. The fields stay; what they were said to buy — "coalescing
  without a directory read" — is dead, because a bounded read must fetch the
  DIRECTORY first (that is where `codecFlags` lives), so the directory read is
  not avoided. ⚠️ NOT because a range cannot be named: two can be, and ADR-0044
  §(b) is headed "NO RANGE IS EXPRESSIBLE" IS TOO STRONG. An earlier version of
  this header said exactly that, across a line break, which is how a sweep for
  the literal string missed it. ADR-0044 gives the reason they are kept, prices it at 5.4% of
  the subscription stream the bytes travel on (0.134% of the segment), and creates M5.66 for the bounded reader that would use them. The
  reopening under Decision (b) below is CLOSED by that record. ⚠️ security.md
  rule 3's range half is **NOT closed** — it now has an OWNER, which is a
  different thing: ADR-0044 Decision (c) is headed "NOT closed" and hands it to
  M5.66, whose criterion 4 names the rule and the standard. An earlier version
  of this line said the clause was closed, contradicting the record it cites, in
  the block an auditor of ADR-0041's deferral reads first. The body below is
  left standing because it is what was believed when the field shipped, with the
  two ownership sentences marked in place.
Date: 2026-09-13
Requirements: FR-6, NFR-4
Research: docs/research/30-design-space/04-discovery-and-tailing.md §2c (the event
  shape, whose `byteStart`/`byteLen`/`codec` this record partly restores);
  docs/research/30-design-space/10-client-library-and-fetch-modes.md §5 (the three
  `via` modes from the client's side).

## Context

ADR-0041 gives the grant's delivery to the subscription protocol and assigns it
to **M5.14 by name**; the M5.13 row repeats it. M5.14 shipped the event shape
and deliberately carried neither the grant nor a range, so `FetchMode.DIRECT`
became an encodable mode with **nothing able to convey the URL a consumer would
fetch with**. ADR-0042 recorded the removal of research doc 04 §2c's
`byteStart`, `byteLen` and `codec` as deliberate, and named **M5.44** as the row
that decides whether closing security.md rule 3's "scoped to one key and where
possible one range" means bringing the coordinates back.

Two questions had to be answered together, because the wire shape depends on
both.

## Decision

**(a) The grant crosses as `io.github.huyz0.os.biningester.format.Grant`, a second type for the same
concept, not as `io.github.huyz0.os.biningester.binstore.SignedUrl` and not as a bare `String`.**

`SignedUrl` cannot make this trip. architecture.md rule 1 says `format` depends
on **nothing**, and ADR-0023 keeps `binstore-spi` out of `client` and `plugin`.
`SignedUrl`'s own javadoc already records the consequence — *"the value crossing
to the consumer at M5.14 is a String again and this guarantee does not travel
with it"*. What `Grant` restores is that guarantee: `toString` redacts, so the
accidental paths — concatenation into a log line, an exception message, a
record's generated `toString` — all yield the redaction, and `url()` is the
deliberate way out. security.md rule 4 otherwise becomes a rule every future
caller in two more modules has to remember, which non-negotiable 9 puts at the
weakest rung there is.

⚠️ **Two types for one concept is a cost, and it is the smaller one.** The
alternative is `format` depending on `binstore-spi`, which makes every consumer
of the wire format depend on the store SPI and deletes the boundary ADR-0023
exists to hold.

**(b) `byteStart` and `byteLen` come back, as COORDINATES rather than as the
grant's scope. `codec` does not.**

ADR-0042's own analysis says a reader given coordinates needs **zero** directory
reads, where one without them reads the directory and then the payload. The
saving is real and is **per (node, segment)** — one GET plus at most one
directory read becomes one ranged GET.

⚠️ **BUT "ZERO DIRECTORY READS" IS FALSE AS STATED, AND ADR-0042 STATES IT
TOO.** `u32 codecFlags` lives per run in the **directory**
(`SegmentFormat`'s layout, research doc 01 §3), and DATA per run is
`[u32 uncompressedLen][u32 crc32c][records]` with **no codec id** — so a ranged
GET that fetches only a run's DATA slice cannot decompress what it fetched, and
this record deliberately keeps `codec` off the event. The claim holds only while
every segment is `CODEC_NONE`.

⚠️ **SO THE ONE REQUEST IS A WHOLE-OBJECT GET, and an earlier draft of this
paragraph said `[0, max(byteStart + byteLen))` — wrong twice.** A prefix ending
at the last wanted run has **no FOOTER**, and `SegmentReader.open` checks the
footer magic *before* it reads the directory, so such a read throws
`"segment is truncated: no footer magic"` on every delivery;
`ConsumerClient.decodeInto` opens the whole array with no partial path. And
cost.md R7's `h<headerLen>` does **not** make that bound computable — it gives
the HEADER's extent, not the object's, and nothing the consumer holds carries
the object length, so `[0, objectLen)` is not expressible either. The request
is a plain GET with no range.

⚠️ **WHICH LEAVES THE COORDINATES WITH NO CONSUMER, AND REOPENS THIS DECISION.**
If the fetch is whole-object, `byteStart`/`byteLen` buy nothing at the fetch:
the request needs no bound, and `SegmentReader` reads the directory regardless.
They still say which slice is a run's, and no reader in the tree uses that.
**M5.45f settles it** — either a reader that can decode a run from a bounded
read, which is its own row, or this decision is amended and VERSION_3 carries a
field for a consumer that never arrives. ⚠️ IT WAS M5.45c UNTIL THAT ROW WAS
SPLIT, and M5.45f takes the question TOGETHER with where a production fetcher
may live: a fetcher that cannot range at all makes these coordinates dead by
construction, and one that can makes the bounded-read reader a live option. Recorded here because the field is
already on the wire and a reader must not take the saving as banked. `GrantIssuer`'s javadoc records that
consumer-side GETs are invisible to `CountingBinStore`, so nothing in the tree
would have measured the difference either way.

⚠️ **THE GRANT STAYS SCOPED TO THE KEY, NOT TO THE RANGE, AND AN EARLIER DRAFT
OF THIS RECORD GOT THAT BACKWARDS.** It claimed the coordinates close
security.md rule 3's "where possible one range" and priced the saving as *two
GETs per consumer per segment rather than one* — which silently assumed ONE RUN
per segment per consumer. The event is per `RunKey`, and ADR-0042 sizes the real
case at ~400 events naming the same `segmentKey` per node per 8 MiB segment. A
grant whose SIGNATURE covers one run's byte range cannot be shared by the other
399, so reading this record literally would produce **400 consumer-side ranged
GETs against one object** — a rate scaling with shards-per-node, which
non-negotiable 6 forbids by name, and the exact coalescing — M5.39's when this
was written, M5.45d's at the hub and **M5.45h**'s at the consumer now — defeated
by the field meant to help it. Review measured the inversion: the
claimed saving reverses at K ≥ 2.

⚠️ **SO WHAT THE COORDINATES BUY IS COALESCING WITHOUT A DIRECTORY READ.** A
node holding K runs of a segment has K events, each carrying its own
`byteStart`/`byteLen`, and can union them into ONE ranged GET under ONE
key-scoped grant — which is what "one grant per (node, segment)" means. Without
them it must read the directory first to learn the same thing.

⚠️ **AND RULE 3's RANGE HALF IS THEREFORE STILL OPEN.** Closing it needs a grant
minted over the union for a specific (node, segment). ⚠️ M5.45b was SPLIT and
owns none of it, and neither does M5.45c, which was split in turn. ⚠️ **AND
"DECLINED" IS MORE THAN THIS RECORD KNOWS**: an earlier draft of this sentence
said the range is declined in favour of a whole-object GET and cited M5.45g's
cell, which disclaims the decision and hands it to **M5.45f** — the same row
Decision (b) above names. So the honest statement is that no range is
EXPRESSIBLE today, for the reasons under Decision (b), and whether one ever
becomes fillable is M5.45f's to settle. ⚠️ **AND M5.45f HAS SINCE SETTLED IT:
ADR-0044 Decision (c) gives rule 3's range half to M5.66**, whose criterion 4
names the rule and the standard. An earlier version of this sentence ended
"either way rule 3's range half has **no owner**", which was true when written
and stopped being true in the record that closed this one.

⚠️ **They are not a duplicate of a per-run fact.** A subscription event is
already per run — it carries `firstOffset` and `recordCount`, the run's LOGICAL
coordinates. `byteStart`/`byteLen` are the same run's PHYSICAL coordinates, and
the directory entry that holds them (`RunEntry`) is the thing a reader would
otherwise fetch to learn them.

⚠️ **`codec` stays out, and ADR-0042's reasoning for it is untouched**: the
codec is recorded per run in the segment header, so an event repeating it would
be a second copy of a fact the bytes already carry. Nothing about a range
changes that.

**(c) The range is optional WITHIN the grant, and a presence byte says which.**

A grant may cover a whole object — that is what a consumer replaying a backlog
wants. `byteStart = 0` is the first byte of a real segment and `byteLen = 0` is
an empty range, so **there is no spare value to mean ABSENT**: a sentinel varint
would have to encode `value + 1` and put an off-by-one between the wire and the
field, which is the class of bug golden files exist to catch and the hardest to
read in a hex dump. `SESSION_EPOCH_ABSENT` can use `0` because epoch zero is not
a live session; a byte offset has no such spare value.

⚠️ **(d) THE SESSION-EPOCH SLOT IS NOT SPECIAL-CASED FOR v3, which is the
decision ADR-0042's amendment forwarded here by name** — *"either a v3 body
carries the sentinel, re-opening the in-band collision round 1 blocked on, or
the slot is special-cased… deciding that is part of adding the field, not after
it."* A v3 body always carries the epoch, so `0` there remains
`SESSION_EPOCH_ABSENT` and cannot also be a value. **The compact constructor
refuses `grant != null && sessionEpoch == SESSION_EPOCH_ABSENT`**, so a writer
cannot emit a body its own reader rejects.

⚠️ Round 1 of this row measured what its absence allowed: `encode` stamped
VERSION_3, wrote `0` into the epoch slot, and `decode` threw — an event this
class produces and cannot read back, with the ingester recording a push every
consumer rejects. The case asserting that was named *"refused on the wire"*,
describing a writer defect as reader correctness.

## Consequences

**VERSION_3, and a v3 body is a v2 body plus the grant.** A grant implies a
session epoch, so the three shapes differ by one field each, `decode` stays a
single forward pass, and the golden files are legible when diffed. v1 and v2
stay decodable; an unknown version still **stops** rather than skipping, for the
reason ADR-0042 gives.

**Four golden files become six**, and the two new ones were verified field by
field against a hex dump rather than accepted as whatever the encoder emitted:
magic, version, session, epoch, the UUID's two longs, partition, key, offsets,
`via`, the empty inline, the session epoch, a 48-byte URL, the expiry varint,
the presence byte and the two range varints.

**A range without a grant is refused in the constructor**, because `encode`
writes the range inside the grant — accepting it in memory would let a caller
build an event that does not survive a round trip.

⚠️ **`equals` and `hashCode` here are hand-written and enumerate components.**
Adding the three fields without adding them to that list would have left them
invisible to every `isEqualTo` in the tree, including the golden assertions — a
decoder that dropped the grant entirely would still have matched its golden
file. They are in the list, and a case asserts that a differing grant makes two
events unequal.

**Cost: unchanged BY THIS RECORD, and at most one directory read per
(node, segment) better when `direct` is assembled.** The commit that made this
decision added no object-store request: nothing minted a grant, `SubscriptionHub`
still threw on `DIRECT`, and the event is larger by the grant only when one is
present. ⚠️ **THAT IS NO LONGER THE STATE OF THE TREE, AND AN EARLIER DRAFT OF
THIS PARAGRAPH READ IN THE PRESENT TENSE.** M5.45d landed: the hub mints one
grant per segment and serves `DIRECT`, so a reader pricing `direct` today must
read the Status note below rather than this paragraph. The CONSUMER-side GET
those grants authorise is still not issued by anything (M5.45g), and when it is,
`CountingBinStore` will not see it — `GrantIssuer`'s javadoc records why. ⚠️ The saving is
the directory read, NOT a GET per consumer, and it is per (node, segment) rather
than per run -- see the warning under Decision (b) for why the per-run reading
inverts it.

**What this record does NOT do**: mint a grant, wire `GrantIssuer`, or give the
consumer a way to fetch with one.

⚠️ **Status 2026-09-14: M5.45b was SPLIT and owns none of these**, and
**M5.45d has since LANDED** — minting through `GrantIssuer`, the coalescing rule
at the hub and the TTL clamp are done, with the mint hoisted above both loops so
one signature serves every consumer and every run of a segment. The end-to-end
"all three modes deliver byte-identical records" is **M5.45e**.

⚠️ **M5.45c was then SPLIT TOO, and owns none of these either.** Where a
production fetcher may live, and this record's reopened `byteStart`/`byteLen`
question, are **M5.45f**; the seam in `client` and `Delivery` carrying the grant
are **M5.45g**; one fetch per (node, segment) is **M5.45h**, which depends on
**M5.62** because the node-scoped subscriber it needs is the one M5.62 already
owns building.

⚠️ The range half of security.md rule 3 that this record leaves open is NOT
CARRIED by M5.45f/g/h, and saying it is **DECLINED** would be more than this
record knows. ⚠️ **ADR-0044 Decision (c) gives it to M5.66**, which owns
whether a range ever becomes fillable — the row that either builds a bounded
read, making a range expressible, or takes the coordinates off the wire, closing
the clause by declining it with a reason. Its criterion 4 names this rule. What
is true today is narrower: no range is EXPRESSIBLE, so a whole-object GET is
scoped to one key and to no range. What M5.45h owns is the COALESCING — one request per (node, segment)
rather than one per run — which review caught the M5.45b split dropping and
which would otherwise have had no owner at all. Rule 3's range half stays open,
and the paragraph under Decision (b) says why. ⚠️ **AND M5.45g's SEAM TAKES NO RANGE PARAMETER.
M5.45f HAS SINCE ANSWERED, AND THE ANSWER IS "NOT YET"** (ADR-0044; M5.66 owns
whether one ever becomes fillable): an earlier draft of
M5.45c's criterion 1 asked for `Grant` plus a byte range, and that same cell
establishes two paragraphs later that nothing fills it. A parameter no caller
can fill reads as an implemented capability. ⚠️ ADR-0044 later found the
stronger form of this claim — "no range is expressible" — to be wrong: two
ranges are nameable, and what is absent is a reader that issues one.
