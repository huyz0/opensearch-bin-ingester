# 0033. A checkpoint carries offsets and flush seqs, never the segment index

Status: accepted
Date: 2026-09-06
Requirements: FR-11, FR-10
Research: docs/research/30-design-space/03-metadata-and-cas.md

## Context

Recovery today is an unbounded full-chain replay: `CommitLog.recover()` LISTs the
epoch prefix and GETs every entry, so a takeover's cost grows with the term's
length and, since M4.6e made offsets cross epoch boundaries, with the cluster's
whole history. M4.9 exists to bound that, and it cannot be bounded without a
point to replay *from*.

That point is a checkpoint. The question this ADR settles is what one contains,
because the obvious answer is wrong in a way that would silently undo M4.9.

A checkpoint plausibly wants to be self-sufficient — everything a recovering
leader needs, in one object. The commit chain's deltas carry two different kinds
of fact:

- **Aggregates**: where each stream stands (`nextOffset`), what has been applied
  per pod (`lastAppliedFlushSeq`). Bounded by the number of streams and pods.
- **The offset-to-segment index**: which segment key holds the records at a
  given offset. Bounded by the number of *commits*, which grows without limit
  until M7's retention deletes anything.

Inlining the second is what makes a checkpoint self-sufficient, and it is
precisely what makes it useless: an object that grows with history replaces one
unbounded read with another, at a larger constant.

## Decision

**A checkpoint carries per-stream `nextOffset` and `oldestRetainedOffset`, and
per-pod `lastAppliedFlushSeq`. It does not carry the offset-to-segment index,
which stays in the deltas.**

Its own object, at its own key, with its own magic (`0x42434B50` — `BCKP`) and
version: a checkpoint is not a chain entry and must never decode as one.

Entries are encoded in a **total order** — streams by `RunKey`, pods by podId —
so the bytes are a function of the content rather than of the order a caller
built the maps in. Two checkpoints carrying the same facts encode identically.

`oldestRetainedOffset` is carried from the first commit although nothing reads
it before M7, for the reason `CommitRequest` carried `(podId, flushSeq)` from
M4.1: adding a field later is a format change, and this is the one commit where
a format change costs nothing.

## Consequences

**The size stays bounded by the fleet, not by uptime.** At 1,600 streams a
checkpoint is tens of kilobytes — measured at 148 bytes for the six-stream
golden fixture and asserted under 64 KiB at 1,600 streams. A checkpoint that
inlined the index would grow with every commit, and the assertion that catches
that is the *magnitude*: a bound of merely "under 1 MiB" is green against an
inlining encoder, which is why the test brackets rather than caps.

**Recovery still needs the deltas after the checkpoint.** A checkpoint says
where the streams stood; it does not say which segment holds a given record. A
reader that must resolve an offset to a segment reads deltas, and M4.9's bound
is "the newest checkpoint plus at most K deltas" rather than "the checkpoint".
This is the deliberate trade: a small object read always, and a bounded number
of larger ones read after it.

**A consistent field swap is a real hazard, and the golden file is what catches
it.** `nextOffset` and `oldestRetainedOffset` are both uvarints in adjacent
positions, so swapping them in encode *and* decode round-trips by construction.
Only a byte-level golden with distinct non-default values in both fields
discriminates — and `oldestRetainedOffset`, the field nothing consumes before
M7, is exactly the one a careless fixture leaves at 0. Measured: that swap
round-trips green and fails the golden test's value assertions.

**Nothing writes one yet.** M4.8b decides when a checkpoint is written and under
which key; M4.9 reads it. Shipping the record and its codec alone follows M4.1,
which landed the `Sequencer` seam and its fake with no implementation behind it,
and satisfies `wire-format-change` because with no writer there are no bytes and
no reader to desynchronise.

## Alternatives considered

**Inline the offset-to-segment index.** Rejected above: it makes the checkpoint
grow with history, which is the property M4.9 exists to remove.

**Reuse `ChainEntry` with a new kind.** Rejected. A checkpoint is addressed
separately and read on a different path, and putting it behind the chain's magic
invites exactly the confusion M4.8b records as a live hazard — `ChainReplay`
decodes every key under the log prefix and takes the last as the chain end.
Distinct magic makes a checkpoint decoded as a chain entry fail loudly.

**JSON, as the lease uses.** Rejected. The lease is one small object per slot
rewritten every few seconds, where readability wins; a checkpoint is thousands
of numeric fields where uvarints are several times smaller, and the chain's
existing binary idiom already has the reader, the writer and the golden-file
discipline.
