# 0031. A same-`podId` collision is bounded by the chain, not worth a nonce

Status: accepted
Date: 2026-09-05
Requirements: FR-11, NFR-11
Research: docs/research/30-design-space/03-metadata-and-cas.md — the CAS/epoch
  design ADR-0002 and ADR-0011 both build on.

## Context

Acquire-recovery (M4.3g) cannot distinguish this instance's own unverified
candidate from another process's identical one when both share a `podId` and
both cold-start: each mints `Lease(1, "podA", endpoint, expiry)`, so a losing
candidate that itself hit an ambiguous `putIfAbsent` is byte-identical, at the
fields `isOwnTerm` checks, to whichever process actually won. The loser's next
`tryAcquire` calls `refreshed(pending)`, which reads whatever landed and asks
only `current.epoch() == mine.epoch() && current.holderPodId().equals(podId)`
— both true regardless of whose write actually landed — and adopts the term
as its own.

**Out-of-protocol.** A StatefulSet exists to make pod names unique, so two
live processes named `podA` is the platform failing, not the lease protocol —
which is why M4.3g's own control test deliberately uses a *different* pod for
the winner, and why this is a row to decide rather than a defect to fix.

The row poses the question directly: does the epoch counter's fencing already
bound the damage, or is a wire-format change — a per-process nonce in the
lease object, distinguishing two writers who otherwise look identical — owed?
Answering it means tracing what the collision actually produces, not
asserting a probability, the same discipline ADR-0030 used for the adjacent
question.

### What the collision actually produces

Call the two same-`podId` processes X (the genuine winner) and Y (the loser
whose own ambiguous write recovery wrongly adopts X's term). Both now believe
they hold epoch 1 and both proceed to sequence through it.

**Opening the chain is not a race X or Y can lose to each other.** Every
epoch-1 `CommitLog.open()` writes the identical `Continue(0, 0, 0)`
(ADR-0030's finding, reused here): whichever of X or Y calls `open()` second
finds slot 0 already taken, compares it against what it would have written,
finds them equal, and takes the idempotent-adopt branch — no exception, no
fencing signal, both instances now genuinely hold an opened `CommitLog` at
epoch 1.

**Committing is where I1 does the actual work.** `CommitLog.commit`'s
lost-race branch (`CommitLog.java:242-279`) does not distinguish "I lost to a
fenced predecessor" from "I lost to a concurrent peer" except by entry SHAPE:
losing to a `Seal` throws `fenced` (I5); losing to an ordinary `CommitDelta`
folds its offsets in and retries at the next slot. X and Y committing
concurrently under the same epoch hit exactly the second case against each
other, every time — the write-once chain (`putIfAbsent`) is what arbitrates
each individual slot, and the loser's retry loop recomputes `nextOffset` from
what actually landed before proposing again. **No slot is ever double-won and
no offset is ever double-assigned** (I1, I2) — the two processes end up
cooperatively interleaving into one sequence, wastefully (extra retries) but
not unsafely.

**Renewing narrows the confusion, but round-1 review measured that it does not
reliably CLOSE it, which an earlier draft of this record overstated.**
`LeaseManager.renewLocked` uses `putIfMatch` keyed on the caller's own cached
`Version`. X and Y hold *different* versions the moment either one's write
next lands (a version is opaque per-write state, not derivable from the
lease's own fields the way `Continue(0,0,0)` is) — so at the next renew,
whichever of the two is stale usually loses its conditional write CLEANLY, an
ordinary lost race, and takes the existing, already-proven-correct fencing
path: belief dropped, sequencing stopped immediately, per
`LeaseManager.renew()`'s own javadoc (not, as an earlier draft said, anything
in `Sequencer.commit`'s).

⚠️ BUT THAT RENEW CAN ITSELF LAND AMBIGUOUSLY — a store fault, not a version
loss — and ADR-0027 documents that as a routine, expected occurrence in this
codebase, not a corner case. An ambiguous `putIfMatch` sets
`belief = mine.uncertain()` rather than dropping it (`LeaseManager.java:309-
318`), so the stale process is NOT fenced by that renew; its NEXT attempt
re-enters `refreshed()`, which re-admits whatever is currently live via
`isOwnTerm` — checking only `epoch` and `holderPodId`, never
`expiresAtMillis`, which `Lease.renewedUntil` leaves as the only field a
renewal actually changes. X and Y never diverge on the two fields `isOwnTerm`
checks, so an ambiguous renew silently RESYNCHRONIZES the stale process onto
the live lease rather than fencing it — reproducing the collision instead of
resolving it. So the honest claim is narrower than "bounded by one renew
interval": renewal usually resolves the collision within one interval, but an
ambiguous renew can extend it indefinitely, and nothing in the lease layer
guarantees termination. **What IS guaranteed, unconditionally and regardless
of how long this persists, is I1 and I2** — the write-once commit chain's
protection above does not depend on the renew layer ever resolving anything.

## Decision

**No wire-format change.** No per-process nonce is added to `Lease`. The
epoch counter's fencing — specifically the write-once chain, not the lease
layer — already bounds a same-`podId` collision to *waste* (redundant
retries, for as long as the confusion persists, which is USUALLY one renew
interval but is not GUARANTEED to be, per the ambiguous-renew finding above)
rather than to any violation of I1, I2, I3 or I5, which hold unconditionally
regardless of how long the waste persists.

This matches ADR-0002's own framing, reused rather than re-argued: the lease
is liveness and efficiency, not safety: "two processes that both believe they
lead cannot both win a sequence number, so an undetected zombie is wasteful,
not incorrect." A same-`podId` collision is exactly an undetected zombie of
this shape — USUALLY shorter-lived than the zombie ADR-0002 originally
described, since a version-keyed renew ordinarily fences the stale side
within one interval, but not GUARANTEED to be, per the ambiguous-renew
finding above.

## Alternatives considered

**(a) A per-process nonce in the lease object**, distinguishing two writers
whose epoch and `podId` are identical. Rejected on the trace above: it would
buy nothing that is not already bounded. It also **is** a wire-format change
(non-negotiable 8, `wire-format-change` skill) — every reader, every writer,
the fakes and the golden files move together — for a threat class
(StatefulSet naming failure) this protocol does not defend against anywhere
else in the object path. `Lease`'s own javadoc already reserves
`holderEndpoint` for M5's commit forwarding without spending a format change
on speculative future need; spending one here for a bounded, self-correcting,
out-of-protocol case would be the same mistake in the other direction.

**(b) Make `isOwnTerm` stricter — reject a recovery whose candidate's
`expiresAtMillis` does not exactly match the stored lease's**, on the theory
that two independently-timed cold starts would rarely compute the identical
millisecond. Rejected FOR THE COLD-START CASE: this is a probabilistic guard,
not a real one there — two processes racing the same
`clock.millis() + ttl.toMillis()` computation within the same store round-trip
can coincide, and a guard that sometimes works is worse than none. ⚠️ It is a
BETTER fit for the separate ambiguous-RENEW case the Context section's
round-1 revision found — there, `expiresAtMillis` is exactly the field a
renewal changes and the stale process's cached copy would not — but tightening
`isOwnTerm` for renewal recovery specifically is its own change, to a method
three call sites share, and this row's question was about the cold-start
collision. Left as a candidate for a future row rather than folded in here
without its own review.

**(c) Reject cold-start acquisition outright when a candidate belief already
exists for the SAME `podId`**, forcing an operator to intervene rather than
silently adopting. Rejected: `Belief` and `belief` are per-*instance* state
(one JVM process), not per-`podId` — X and Y are two different processes with
two different `LeaseManager` instances, each with its own `belief` field, so
neither can observe the other's candidate to reject it. Detecting the
collision this way would need shared state between X and Y, which is the
consensus layer ADR-0011 explicitly decided against building.

## Consequences

**Unchanged.** `tryAcquire`'s recovery path, `CommitLog.open`'s idempotent
adopt, `CommitLog.commit`'s retry loop, and `LeaseManager.renew`'s
version-keyed fencing are all already correct for the case this decision was
asked to weigh, and stay as they are.

**Recorded, not silently accepted**, same discipline as ADR-0030, and
corrected once already within this same record (round 1 review): a
same-`podId` collision costs redundant commit retries on both processes for
as long as the confusion persists. USUALLY that is one renew interval,
because a version-keyed renew ordinarily fences the stale process cleanly —
but an ambiguous renew (routine per ADR-0027, not a corner case) can extend
it indefinitely, because `isOwnTerm` re-admits the stale process on its next
recovery regardless. What is NOT recorded as bounded, because it is not, is
HOW LONG the waste lasts — only that it never becomes unsafe.

**Foreclosed.** Nothing specific to this row's own question (whether the
cold-start collision needs a wire-format fix — it does not). Tightening
`isOwnTerm` for the ambiguous-renew case specifically, using
`expiresAtMillis` per alternative (b)'s revision above, is the natural
extension if the indefinite-persistence case is ever prioritized — a new
row, not a reopening of this decision, since this decision's own scope was
always the cold-start collision and its answer (no nonce) does not change.
Separately, if `podId` uniqueness is ever found to be less reliable than the
StatefulSet guarantee assumes, that is a platform concern belonging to
whatever layer owns pod naming, not this one.
