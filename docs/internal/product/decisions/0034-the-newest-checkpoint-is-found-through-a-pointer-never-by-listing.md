# 0034. The newest checkpoint is found through a pointer, never by listing

Status: accepted
Date: 2026-09-06
Requirements: FR-11, NFR-3
Research: docs/research/30-design-space/03-metadata-and-cas.md

## Context

M4.9 bounds recovery at "the newest checkpoint plus at most K deltas". It cannot
begin until a recovering leader can FIND the newest checkpoint, and acceptance
criterion 5 requires that discovery issue **zero `list` calls**.

The M4 SPEC states that bound and deliberately refuses to choose the mechanism.
That refusal is itself a finding: three of M4.0's review rounds each found a
defect in a mechanism the SPEC had specified — a circular cursor, then a
page-count bound that fifty checkpoints fit inside, then a mechanism deleted
from the SPEC but left standing in the backlog, which is what actually gets
built. Round 3's diagnosis was that the SPEC was designing a mechanism it need
not contain. This ADR is where it is chosen.

⚠️ **Zero `list` is not the real bound.** Two mechanisms satisfy it while being
wrong, and both were on the candidate list: deriving the cursor from the chain
head as a BACKWARD WALK costs zero LISTs and one GET per checkpoint, and
probing the sequence space with `stat` costs zero LISTs and one `stat` per
probe. The property that matters is TOTAL REQUESTS ACROSS ALL VERBS, CONSTANT
in the number of checkpoints.

## Decision

**A pointer object at `<logPrefix>ckpt/LATEST` carries the newest checkpoint's
own BYTES, and is overwritten on every successful checkpoint. Discovery is one
`stat` and one `get`, whatever the chain's history.**

**The pointer carries the body, not the sequence.** A pointer holding a number
would be a second wire format — its own magic, version, golden file and
`wire-format-change` obligations — to save nothing, and discovery would then
cost two GETs instead of one.

**`stat` first, never a caught `get`.** `BinStore` has no not-found type and
defines `IOException` as "the store is unreachable", so catching it and
returning empty reports an OUTAGE as a chain that has never checkpointed — after
which M4.9 replays the whole term believing that is correct. A test pins this;
it was measured surviving without one.

**The pointer is per-epoch**, because `logPrefix()` embeds the epoch. Two
leaders can never contend for it, and a fenced leader's pointer is invisible to
its successor.

## Consequences

**A checkpoint costs two PUTs rather than one.** Both are per trigger — per K
deltas or per T seconds — and neither scales with streams, pods, shards or
indices, so non-negotiable 6 holds and the M4 SPEC's "one PUT per K deltas or T
seconds — negligible" becomes two. It is the price of a constant read, paid on
the write path, which is the right side: checkpoints are written on a cadence
and read on every takeover.

**A failed pointer write retries the whole checkpoint.** The pointer is written
BEFORE the writer marks the checkpoint written, so a failure leaves the dirty
flag set and the next trigger repeats both. The seq-keyed object is already
there, so its `putIfAbsent` loses harmlessly and the pointer write is retried —
the sequence cannot advance underneath, because it only moves when a commit
does.

**The `ckpt/LATEST` key does not end in `.delta`,** so M4.8b1's allow-list
refuses it at both recovery sites. That filter is what lets any of these objects
live under the chain's prefix.

**The seq-keyed checkpoints remain**, unread by discovery. They are the history
M7's retention will prune and the record an operator reads; the pointer is a
cache of the newest.

## Alternatives considered

**A pointer field in the lease object.** Rejected. The lease is already read at
recovery, so it looks free — but it puts a SECOND WRITER on the one CAS object
whose lost race IS the fencing signal, and a checkpoint write would then be able
to lose that race and be told it is fenced when it is not.

**A `LATEST` pointer carrying the sequence, written with `putIfMatch`.**
Rejected on the wire-format cost above. `putIfMatch` would additionally require
tracking the pointer's version across failures, for monotonicity that a
single-writer-per-epoch key already has.

**Deriving the cursor from the chain head.** Rejected: zero LISTs, one request
per checkpoint. Measured — it fails the constant-total assertion at 5,000
checkpoints and passes every zero-LIST assertion.

**The pointer INSTEAD of the seq-keyed objects.** The cheapest option, and the
ADR omitted it until review asked: discovery never reads the seq-keyed
checkpoints, so one overwritten object would serve it alone, at one PUT per
trigger rather than two. Rejected because they are not written for discovery.
They are the ordered history M7's retention prunes and an operator reads: a
single mutable object keeps no record of what the chain looked like before the
last trigger. ⚠️ A FIRST DRAFT OF THIS PARAGRAPH ALSO CLAIMED M4.9's BOUND NEEDS
THEM, which is false and contradicted by this same commit's correction to
`LogKeys.checkpointKeyFor` — M4.9 reads the pointer, and the pointer carries the
checkpoint's own bytes. The second PUT buys an audit trail and nothing else, at
one per trigger.

**`list`-and-take-last.** The naive form, and the one M4.0's reviews caught
twice. It dies at ONE checkpoint against a strict zero-LIST assertion; the
5,000-checkpoint workload exists to stop the bound being weakened back to a
PAGE COUNT, which is how it survived review before.
