# 0042. The subscription event carries a session and an epoch

Status: accepted
Date: 2026-09-11
Requirements: FR-6, NFR-9
Research: docs/research/30-design-space/04-discovery-and-tailing.md §2c (the event
  shape); §"Do the bytes cost a fetch?" defines the three `via` modes this event
  carries. ⚠️ §2d describes a session-based incremental subscription adopted from
  AutoMQ's `SessionId` + `SessionEpoch` — that SESSION epoch is **not** the field
  shipped here, see the Decision.
  docs/research/30-design-space/10-client-library-and-fetch-modes.md §5 gives the
  same three modes from the client's side.

## Context

M5's completion condition asks for a "push channel with session/epoch". Until
now the push channel has been three in-process types — `SubscriptionHub.Push`,
`SubscriptionTransport`, `Delivery` — and **nothing has ever serialized a
subscription event**. `ConsumerClient.decodeInto` decodes *segment* bytes, not
events; the production transport (M1.11b's `HubTransport`) is unbuilt.

Two facts have to travel with a push and neither can be inferred from the other:

- **which subscription this is**, across a reconnect, so a consumer that drops
  and returns can say where it was rather than replaying from the log; and
- **which sequencer term produced it**, so a consumer learns the sequencer
  *changed* — which is a different fact from "you missed records", and one an
  offset gap cannot distinguish from a slow writer.

⚠️ **This is `wire-format-change` contract 4**, whose audience is "service,
consumer library". The trigger fires even though no socket carries these bytes
yet, because the contract is the agreement rather than the transport — the same
reasoning that made M5.10's Java-interface change contract 5.

## Decision

**A subscription event is a versioned record carrying `(session, sequencerEpoch,
key, segmentKey, firstOffset, recordCount, via, inline)`, encoded big-endian
behind an 8-byte magic-and-version header.**

- **`session` is opaque to the consumer** — the ingester mints it, the consumer
  echoes it. Nothing in the format constrains its shape, so a later scheme
  carrying, say, a shard hint needs no version bump.
  ⚠️ **A session that becomes a CREDENTIAL is a different matter, and this
  record no longer offers "signed" as a casual example.** `toString` prints the
  session in full — deliberately, because it is the identifier an operator
  needs to trace one subscription — while redacting `inline` under security.md
  rule 4. Round-3 review pointed out those two facts together are a trap: M5.15
  mints a resumption session, this sentence says a signed one needs no version
  bump and therefore no review trigger, and the first
  `"could not serve " + event` puts a bearer token in a log. **If a session ever
  carries a secret, it is redacted here first**, and that is a change to this
  record, not a drop-in.
- **`sequencerEpoch` is the sequencer TERM**, and a change means the sequencer
  moved. ⚠️ **It is named for which epoch it is, because there are two and M5's
  SPEC criterion 12 calls conflating them "the obvious defect".** An earlier
  draft of this record called the field `epoch` and cited research doc 04 §2d
  for it — but §2d's counter is KIP-227's **session epoch**, which orders
  concurrent requests *within one session* and makes retries idempotent. That is
  a different quantity with a different lifetime, and shipping the sequencer
  term under its citation is exactly the conflation the SPEC warns about.
  Round-1 review caught it. When the session epoch arrives it is a **second
  field**, not a reinterpretation of this one, and M5.15 owns keeping them
  apart.
- **`via` is written by NAME, not by ordinal.** Reordering the enum would
  silently change every encoded event; a name survives a reorder and fails
  loudly on a rename, which is the direction that can be fixed.
- **`inline` is empty rather than absent** when the bytes do not travel with the
  event, so a decoder never distinguishes "no field" from "zero bytes".
- **The pairing of `via` and `inline` is an invariant of the FORMAT**, enforced
  in the constructor and reported as a parse failure when it arrives over the
  wire. `via=INLINE` with no bytes is a delivery that decodes to nothing —
  silent record loss, which `SubscriptionHub`'s own javadoc already warns about
  for the empty-array case.

**An unknown version STOPS; it does not skip.** The skill names this as the item
people get backwards, so it is stated: a consumer that ignored an event it could
not parse would report a clean stream while losing records, where one that
refuses falls back to the commit log — which is what the log is for. ⚠️ The
contrast is the **key filter**, where an unknown tag means *read the header*
rather than *no match*: there, skipping drops data; here, continuing does.

## Alternatives considered

**(a) Infer the sequencer change from an offset gap.** Rejected: a gap and a
term change are different events with different responses. A gap means fetch
what was missed; a term change means the coordinates a consumer holds may name a
chain that no longer grows. ADR-0039 already records how expensive conflating
two similar-looking conditions is — a refusal and a silence were treated alike,
and a leader lost to node failure elected no successor.

**(b) A monotonic counter instead of a session.** Rejected: it identifies a
*position*, not a *subscription*, so two consumers of the same stream cannot be
told apart across a reconnect and the ingester cannot tell resumption from a new
subscriber. AutoMQ's `SessionId` + `SessionEpoch` (research doc 04 §2d) exists
for exactly this and is the prior art the corpus already read.

**(c) JSON, per research doc 04's `HTTP/2 + NDJSON` recommendation.** ⚠️ **Not
rejected, and this record does not overturn it.** Doc 04 recommends NDJSON for
the *channel*; this decides the shape and the invariants of one *event*. A
binary encoding is what the repository already has a `Cursor`, varints and
golden-file conventions for, and what keeps a per-flush-per-stream event small.
If the channel lands as NDJSON, this record's fields and its version
discriminator carry over unchanged — the framing is M1.11b's or M5's transport
row, not this one's.

**(d) Carry the signed URL, and the byte-range coordinates, in the event.**
Deferred — ⚠️ **and deferring them needed an owner, which a first draft of this
record did not give them.** ADR-0041 assigns *both* the grant delivery and the
range scoping to **M5.14 by name**, and the M5.13 row repeats it; this record
originally deferred both to no row at all, leaving `FetchMode.DIRECT` on the
wire with no field able to carry the URL and nobody accountable for closing it.
Round-1 review found that. **M5.44 now owns both**, ADR-0041 carries a dated note
on its `Status:` line pointing here, and the reasoning is unchanged: `via=DIRECT`
says the consumer fetches for itself, and adding a field is a version bump this
format is built to take.

⚠️ **The dropped coordinates are a deliberate removal, not an oversight.** Doc
04 §2c's event carried `byteStart`, `byteLen` and `codec`; this shape carries
none of them.

- **`byteStart`/`byteLen`**: cost.md R7 puts the header length in the object KEY
  (`h<headerLen>`), so a reader does one speculative range read that retrieves
  the preamble *and* the directory without guessing. ⚠️ **That removes the
  GUESS, not the lookup** — round-2 review corrected an earlier draft of this
  paragraph that implied otherwise. A reader given coordinates needs zero
  directory reads; without them it reads the directory and then the payload.
  The effect is confined to `direct`, because `proxy` and the per-AZ cache owner
  fetch whole segments, so SPEC criterion 9's *2 GETs per segment* is unaffected.
- **`codec`**: nothing to do with R7 — the codec is recorded **per run in the
  segment header** (research doc 01 §4), so an event repeating it would be a
  second copy of a fact the bytes already carry.

⚠️ **AND A RANGE-SCOPED GRANT IS UNCONSTRUCTIBLE WITHOUT COORDINATES**, which
this record should say rather than leave implied: security.md rule 3 asks for a
grant "scoped to one key and **where possible one range**", and with no range on
the event there is nothing to scope to. ADR-0041 already records the range half
as unimplemented; **M5.44** owns whether closing it means bringing
`byteStart`/`byteLen` back.

## Consequences

**There is no old shape, and the checklist items that assume one are recorded as
not applying rather than skipped.** No bytes exist in any bucket — the protocol
is not persisted at all — and no peer speaks it across a process, so "ship the
read side first, in an earlier commit" and "golden files for the old and the new
shape" have no subject. ⚠️ **The version byte is still written and an unknown one
still refuses**, because the first real rollout needs the discriminator already
there; adding it later is precisely the change that cannot be made compatibly.

**Two golden files, not one:** the empty-payload case is where a length prefix
and its absence are easiest to confuse, and it is the only fixture that pins
`via` to something other than `INLINE`.

⚠️ **An earlier draft justified the second file with an argument that is simply
false** — "a codec writing the ordinal instead of the name would match the inline
file whenever `INLINE` is ordinal 0". It would not: `via` is written as a
length-prefixed string, so the inline golden holds `06 49 4e 4c 49 4e 45` where
an ordinal codec emits `00`, and that fixture alone already fails. Round-3
review caught it. The danger of a false justification is specific: someone
trimming fixtures reasons from it, drops the `direct` file, and then nothing
pins that `via` varies at all.

**`toString` carries no payload.** An event is exactly what someone prints while
debugging a delivery, and `inline` is a document payload, which security.md rule
4 names beside credentials and signed URLs. Only its length is printed.

**The inline array is copied in and out.** A record's array component is shared
by default, so a caller keeping its array could rewrite an event after
construction. On the serving path that is one stream's records appearing under
another's offsets — the failure `SubscriptionHub.publish(delta, segment)`
already refuses a batched delta to avoid.

⚠️ **The event is per `RunKey`, and two open rows own what that costs.** One
event binds one run to one `segmentKey` with `via` chosen per RUN, because
`SubscriptionHub`'s subscriber map is keyed that way and is the only fan-out map
that exists. A node holding ~400 streams in one 8 MiB segment therefore receives
~400 events naming the same `segmentKey` — and if they say `via=DIRECT`, one
event/one fetch means ~400 fetches for one object, a rate scaling with shards.
**M5.40** records exactly this for `proxy` and **M5.39** for `direct` ("one
grant, or one serve, per (node, segment) rather than per run"). ⚠️ Coalescing
stays constructible because `segmentKey` is on every event; what this record
does is name the rows rather than leave the cost paragraph's "confined to
`direct`" reading as though the question were settled.

⚠️ **What this does NOT decide: the channel, the framing, or resume.** M5.15
owns resume, the reset signal and keeping the two epochs distinct — it needs
this shape to exist first. The transport that puts these bytes on a socket is
M1.11b's. Nothing here changes any persisted format: the commit log, the
checkpoint and the segment layout are untouched.
