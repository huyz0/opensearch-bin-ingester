# 0038. `close` does not wait for an election already in the store

Status: accepted
Date: 2026-09-10
Requirements: FR-11, NFR-9

## Context

`Leadership.sequencer()` elects into a vacancy on demand, and winning is not
quick: it takes a `tryAcquire`, a seal, and a `recover()` whose latency is
unbounded in the length of the predecessor's chain. A `SIGTERM` lands inside
that window easily.

So `close` and an election can overlap, and the question is what to do about it.
Two arrangements were written. The one that looks better is worse.

## Decision

**`close` raises its flag and gives back whatever it holds, without waiting for
an election in flight.** The overlap is reconciled afterwards, in `sequencer()`:
the electing thread publishes its term, re-reads the flag, and — if `close`
happened — compare-and-sets the reference away and gives the term back itself.

⚠️ **The losing arm of that compare-and-set returns `null`, not the sequencer.**
The CAS fails exactly when `close` got there first, which means `close` has
already closed that sequencer; returning it hands a caller something closed.
Winning the CAS means `close` did not take it and this thread must give it back;
losing means `close` is doing so. Neither outcome is a term the caller holds.
That arm is what M5.6f fixed, and it is the whole of the fix.

## Alternatives considered

**Have `close` take the election lock, so the two cannot overlap at all.**
Written, measured, withdrawn. It is the tidier design — the race stops existing
rather than being reconciled, and `sequencer()` can check the flag before
publishing with no branch that only a thread interleaving can reach. It was
rejected for two measured reasons. ⚠️ THEY ARE NOT EQUALLY STRONG, and saying
so is the point: the first disqualifies the version that was WRITTEN, and the
second disqualifies the DESIGN. The second carries this decision alone.

⚠️ **It stops the lease being released on the one path it exists for.**
`BoundedLock.takeOrFail` is `ReentrantLock.tryLock(nanos, …)`, which throws
`InterruptedException` when the calling thread's interrupt flag is **already
set** — uncontended or not, reentrant or not. `DefaultIngest.close` arrives in
exactly that state: `drainPushes` catches an `InterruptedException`, calls
`Thread.currentThread().interrupt()`, and the next statement in the same
`finally` is `sequencer.close()`. With the wait first on the shutdown path, that
close throws before it ever reads the reference, `mine.close()` never runs, the
lease is not handed back, and every other pod waits out the full TTL. Without
it, the same path released successfully.

⚠️ **This trap is already documented twice in this codebase**, in
`BatchingSequencer`'s close, and `LeaseManager.release` avoids it with the local
idiom — `if (!lock.tryLock()) { lock.takeOrFail("release", ttl); }` — for
precisely this reason. The withdrawn version did not follow it. ⚠️ **So this
reason is fixable and settles nothing on its own**: that idiom, or
`BatchingSequencer`'s `Thread.interrupted()` save-and-restore, dodges it
completely. It is recorded because it is what the first attempt actually did,
and because a reader who fixed only this would still be left with the second.

⚠️ **And the bound composes badly.** `BoundedLock`'s javadoc argues the TTL for
a *release*, where the wait **is** the release: "waiting up to that is never
worse than not waiting." That argument does not transfer to waiting for a lock
the release then needs *additionally*. Composed, a shutdown is this wait (≤ TTL),
then `BatchingSequencer.close`'s join (≤ 10 s), then `LeaseManager.release`'s own
`takeOrFail("release", ttl)` — about **30 s** at ADR-0007's ~10 s TTL. This
project has already cut a wait from 30 s to 5 s once, and recorded why in
`DefaultIngest`: the old wait "exceeded Kubernetes' default grace period, so a
slow subscriber turned a graceful drain into a SIGKILL". A wait that outlives
the grace period does not buy the release it is spent on; it loses it.

**Have `close` skip the wait when the lock is contended.** Rejected as the worst
of both: it reintroduces the reconciliation it was meant to remove, and keeps a
bound whose only remaining effect is to make an uncontended shutdown slower to
reason about.

## Consequences

- The hand-back stays **best-effort in ordering**: a term won during a close is
  given back by the electing thread rather than by `close`.
- ⚠️ **And best-effort in reporting.** When the electing thread gives the term
  back, `giveBack` logs a failed release rather than throwing — a caller asked
  whether it leads, and failing a flush because the goodbye did not land is the
  defect `retire` already records. So `close` can return normally on a shutdown
  where the lease was not released. ⚠️ That is a BEHAVIOUR gap rather than an
  unpinned property, so it is **M5.6i** and not a bullet on M5.6h, whose items
  are all about tests that do not exist. It is already DRIVEN, by
  `LeadershipTest`'s stub whose release PUT fails; what is missing is a way to
  report it without failing the flush.
- The CAS's losing arm is reachable only when `close` lands between the publish
  and the re-read — two adjacent statements — so it is driven through an
  `afterPublish` seam that puts the close exactly where the interleaving would.
  Racing two real threads for that window would assert a schedule rather than
  the rule.
