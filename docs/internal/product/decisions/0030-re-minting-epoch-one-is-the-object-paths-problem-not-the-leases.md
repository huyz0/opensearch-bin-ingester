# 0030. Re-minting epoch 1 is the object path's problem, not the lease's

Status: accepted
Date: 2026-09-05
Requirements: FR-11, NFR-11
Research: docs/research/30-design-space/03-metadata-and-cas.md — the CAS/epoch
  design ADR-0002 and ADR-0011 both build on.

## Context

`LeaseManager.refreshed()` treats an ABSENT lease object as genuinely
superseded and drops belief, reasoning (per the class javadoc) that "nothing
legitimately deletes the object" — release writes an already-expired lease
precisely so the epoch counter survives. `tryAcquire`'s cold-start branch
disagrees: on `stat.isEmpty()` it mints epoch 1 and adopts it if `putIfAbsent`
wins, with no memory of whatever epoch this node — or any node — held before.

Both are correct on their own terms. The disagreement only reaches production
behaviour if the lease object goes missing **out of protocol** — no code path
in this class deletes it — which is exactly why M4.3e's round-1 review filed
this as a row to decide rather than a defect to fix: reaching it requires an
external actor (an operator, a bug in something else entirely, manual
intervention against the bucket) doing something the protocol does not model.

The question the row poses: **should a node ever refuse to re-mint epoch 1,
or is minting it always fine?** Answering it means tracing what a re-mint
actually collides with, not asserting a probability.

### What a re-mint actually does

`LocalSequencer.start` calls `log.recover()` before `log.open(...)`.
`recover()` (`ChainReplay.replay`) reads epoch 1's chain **in full** —
whatever is already there, not merely a stat — folding every offset and
finding `nextSequence` and any `Seal`. Only then does `open()` run, and it
writes at slot 0 **unconditionally**: `putIfAbsent`, and on loss, compares the
`Continue` it would have written against whatever is already there.

That comparison is where the analysis turns. Every legitimate epoch-1
acquisition writes the identical bytes — `Continue(0, 0, 0)` — because epoch 1's
predecessor is always epoch 0 (M4.4b) and `prevSeq` is always 0. So `open()`'s
equality check can never distinguish "I am retrying my own just-written
CONTINUE" from "an unrelated, ancient epoch-1 CONTINUE happens to be
byte-identical to the one I would write." Both take the idempotent-adopt
branch. This means the write-once chain — the mechanism ADR-0002 credits with
making safety independent of the lease — does **not**, on its own, reject a
re-mint at epoch 1 specifically, because epoch 1's CONTINUE carries no
information that varies between two unrelated leaders.

What happens after adoption depends on whether that old chain was sealed:

- **Sealed** (a legitimate epoch-2+ successor once took over from it,
  properly). `recover()`'s replay finds the `Seal` and sets it on the new
  `CommitLog` instance. Every subsequent `commit()` call throws `fenced`
  immediately (`CommitLog.commit`'s `sealedAt != null` guard). `start()`
  *succeeds* — misleadingly — but the node can never actually sequence, and
  every attempt fails loudly. An operational failure, not a safety one, and
  self-announcing.
- **Unsealed** (the chain's original leader was abandoned and never fenced —
  itself the M4.16/ADR-0029 defect class, NARROWED but not closed by M4.32:
  that row shares `ChainReplay.neverOpened`'s existing origin caveat rather
  than closing it, and a wholly-empty *immediate* predecessor is still left
  unwalked — M4.23's row, still `todo` at the time of this ADR. So a chain
  written before M4.32, or one that trips M4.23's still-open gap, is not a
  hypothetical prior state but a live one). `recover()` inherits its offsets
  and `nextSequence` with no error. The new leader silently **continues** an
  ancient, logically unrelated term as though it were its own — correct in
  the narrow sense that no offset is reassigned (I2 holds: offsets keep
  advancing monotonically, whoever's chain they came from), but wrong in the
  sense that mattered to the row: epoch 1's identity has been reused by two
  unrelated leadership eras stitched into one chain, with nothing surfacing
  that this happened.

The unsealed case is the one real gap, and it needs two independent
conditions to bite: an out-of-protocol lease deletion, **and** a pre-existing
unsealed epoch-1 chain sitting in the store at that moment.

## Decision

**No lease-layer or sequencer-layer guard is added.** `tryAcquire` keeps
minting epoch 1 on an absent lease object, unconditionally, exactly as
today.

This is not "nothing needs to change" as a default — it is the outcome of
weighing the guard against what it would cost, below, against a threat class
(out-of-protocol store tampering) this protocol accepts elsewhere without
defending against it, per ADR-0002's "the epoch is in the object path" resting
entirely on protocol-internal actors.

## Alternatives considered

**(a) `LeaseManager` checks the sequencer's epoch-1 slot before minting.**
Rejected on layering: `LeaseManager`'s own javadoc frames it as the
CAS'd read/write side of `Lease` alone, and it imports nothing from
`CommitLog` or the object-key grammar today. Reaching into the commit-log
prefix to stat slot 0 would make the lease manager know about a sibling
class's addressing scheme for one narrow case, and it would only ever close
the epoch-1 gap — no OTHER epoch can be cold-start re-minted this way, so the
coupling buys exactly one case and costs a permanent dependency.

**(b) `LocalSequencer.start` refuses to adopt a chain with unsealed content
it did not just write.** The more surgical option — stays inside the
sequencer, touches no lease code. Rejected because there is no clean
predicate for it: "content I did not just write" cannot be distinguished from
"content THIS acquisition wrote before a crash mid-`start`, which a legitimate
retry must still be able to adopt" — the exact idempotence `open()`'s
equality check exists to provide (a leader that crashes between opening its
chain and returning to its caller must be able to resume, not be locked out).
A heuristic on `nextSequence > 1` would refuse legitimate retries as often as
it catches the real gap.

**(c) Persist a separate "highest epoch ever believed held" marker, outside
the lease object, so a re-mint can be compared against it.** Rejected: it
adds a new persisted object and a new write path to defend against a threat
(external deletion of ONE object) by hoping the actor doesn't also delete the
new one — the marker is exactly as deletable as the lease it protects, and a
fresh pod that never held anything has nothing to compare against regardless.

## Consequences

**Unchanged.** `tryAcquire`'s cold-start behaviour, `refreshed()`'s
absent-lease handling, and `LocalSequencer.start`'s recovery path are all
already correct for the paths this decision was asked to weigh, and stay as
they are.

**Recorded, not silently accepted.** The unsealed-adoption gap above is real
and this ADR is where it lives: reaching it needs an out-of-protocol lease
deletion landing on a store that still holds an unsealed, abandoned epoch-1
chain. M4.32 (ADR-0029) shrinks how often an abandoned chain stays unsealed
going forward, which narrows this gap's window without closing it — the two
are related but this decision does not depend on that one.

**Foreclosed.** Nothing. If this gap is ever prioritized on its own — a
monitoring signal for "lease object missing" would surface the out-of-protocol
event directly, which is a smaller, more general fix than either rejected
alternative and belongs to observability rather than the lease or the
sequencer — that is a new row, not a reopening of this one.
