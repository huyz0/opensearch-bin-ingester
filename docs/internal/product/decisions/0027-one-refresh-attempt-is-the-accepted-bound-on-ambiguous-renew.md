# 0027. One refresh attempt is the accepted bound on an ambiguous renew

Status: accepted
Date: 2026-09-03
Requirements: FR-11, NFR-9

## Context

M4.3d closed a defect in which an ambiguous renew self-fenced a healthy leader.
A `putIfMatch` that returns empty definitively lost; one that **throws** may
have landed and lost only its response, leaving the cached `Version` stale. The
next renew then loses to this instance's own bytes, and the node concludes it
was fenced while the lease it just wrote runs to the full TTL.

The fix records the outcome as ambiguous and re-reads before trusting the
cached version. That read gets **one attempt**, and M4.3d's own round-1 review
found the residual — filing M4.3h for it in that same commit, `c655de8`: if the
client's timeout fires while the write is still *queued inside the object
store*, it can take effect after the refresh's `stat`. The refresh
then sees the version unmoved, adopts it, clears the flag, and the conditional
write that follows loses to the node's own earlier bytes with nothing left to
say it might have been its own.

⚠️ **The same interleaving reaches `release`**, because the one-attempt refresh
lives in `refreshed()` and `releaseLocked` calls it too. An ambiguous renew
leaves a write queued; a `SIGTERM` arrives; `release` refreshes, sees the old
version, and the delayed renew lands underneath it. The release's conditional
write then loses — and its return value is **discarded**, so nothing observes
the loss at all. The pod exits believing it gave the lease back, having written
nothing. That is the harm M4.3d widened its own scope to close, reached through
the residual this ADR accepts, and it is inside the decision rather than beside
it.

Both paths are now demonstrated rather than argued, each by its own
characterisation test driving `DelayedLandingStore`:

- `LeaseManagerAmbiguityTest.aWriteThatLandsAfterTheRefreshReadStillSelfFencesOnce`
  — the renew path. `renew()` returns empty, `held()` goes empty, and the object
  on the store is the *late renew*, asserted by its exact expiry so that a plain
  lost CAS could not satisfy it. ⚠️ It also pins the unshortenability argued for
  in *The quantities*: it calls `release()` after the fencing and asserts no
  successor can acquire with no clock advance. That last assertion is the one a
  reader is most likely to see fail, because retaining any version in the fenced
  branch — even an unverified one, which keeps `held()` empty and so preserves
  M4.3e — lets `release` refresh onto the late renew and free the lease at once.
- `LeaseManagerAmbiguityTest.aLateWriteAlsoDefeatsReleaseAndNothingObservesTheLoss`
  — the release path. The pod believes it handed the lease back, and a `podB`
  manager cannot acquire with no clock advance, because what the store holds is
  the late renew, still live.

**The quantities, stated more carefully than "up to one TTL".** The cost is one
failover, and the bound is the remainder of the TTL — but not because the node
"stops renewing and the lease expires on schedule". It is because **nothing in
the design can shorten it**. `renewLocked`'s fencing branch sets `belief = null`,
which destroys the version, and from that point `release()` returns at its
`belief == null` guard — so the node cannot hand back a lease it still holds on
the store, on `SIGTERM` or otherwise — while `acquireLocked()` reads the
unexpired lease it wrote itself and returns empty until expiry.

⚠️ And this failover class can never use the fast path. ADR-0007 meets NFR-9's
`< 5 s` "by early challenge on evidence of death; ~10 s worst case on TTL
expiry", and here there is **no evidence of death**: the holder is alive,
healthy, and answering. So this window always consumes ADR-0007's worst case in
full, with no voluntary exit. The honest quantity is *exactly* the remainder of
the TTL, unshortenable — which is a materially different thing to accept than
"up to". No write is lost —
I1's write-once chain and the epoch in the object path are what carry safety
(ADR-0002), and they do not depend on the lease at all. ADR-0002 puts it more
strongly than "held by the right node": two processes that both believe they
lead cannot both win a sequence number, so an undetected zombie is wasteful
rather than incorrect. That matters here because the window this ADR accepts is
one in which **zero** nodes hold the lease for up to a TTL, and safety is
unaffected by that too. The window itself requires a write to land after a
subsequent read of the same key, which is a store-side delay exceeding the
client timeout; it is not the ordinary timeout case, which the M4.3d refresh
already settles on its first attempt.

## Decision

**One refresh attempt is the bound.** An ambiguous conditional write earns
exactly one re-read before the next write is trusted. If a delayed write lands
underneath that read, the resulting lost CAS is treated as definitive fencing,
the belief is dropped, and the node stands down for exactly the remainder of the
TTL — unshortenable, per *The quantities* above. On the `release` path the loss
is not observed at all, and the successor waits out that same remainder.

The residual is recorded in `LeaseManager`'s class javadoc and pinned by the two
characterisation tests named above, which assert the accepted cost. **If either
test ever fails, this ADR is what should be reconsidered** — it is not a
regression to be papered over.

## Alternatives considered

**Let the ambiguity flag survive a refresh that finds the version unmoved.**
Rejected because it does not work on its own, which is the substantive finding
here rather than a matter of taste. Trace it: the refresh reads `V0`, keeps the
flag, and the conditional write still goes out against `V0`; the delayed write
lands in between; the CAS still loses; `renew` still reads an empty `Optional`
as fencing and still drops the belief. The flag surviving changes nothing
unless a **lost CAS while ambiguous is also made inconclusive** — and that is a
much larger change than the row proposing it assumed.

**Make a lost CAS inconclusive while ambiguous, and re-refresh.** This would
genuinely close the window, and it is safe in principle: the `(epoch, podId)`
identity check governs adoption, so a genuinely superseded node re-reads, fails
that check, and correctly stands down. Rejected on where the complexity lands.
A lost conditional write is *the* fencing signal in this design; making it
conditional means the one branch that decides whether a node may keep
sequencing acquires a state-dependent exception. The cost of a bug there is
two nodes sequencing at once — unbounded, and the thing the lease exists to
make unlikely — against a saving of one bounded failover in a window that
requires a store-side delay outlasting the client timeout. That trade is bad in
the direction that matters. It also cannot be bought back cheaply: `renew`
would have to retry internally or invent a third return value, because empty
already means "stop sequencing now" to every caller.

**Retain enough state to `release` voluntarily after fencing.** The cheapest of
the three, and the one that attacks the quantity rather than the window: it
would leave the fencing rule unconditional — the ADR's whole "Easy" consequence
— and cut the stall from a full TTL to the successor's detection time. Rejected
here only because it is not free either, and saying why is what stops it being
rediscovered as an obvious win: `held()` gates on `verified()` and would report
a term after fencing, breaking M4.3e's invariant that `held()` never names a
term this instance does not hold. Closing it properly means a fourth belief
state — "fenced, but still able to give back what I wrote" — and that state has
to be kept out of `held()`, out of `renew`, and inside `release` only. Worth
doing if the frequency ever justifies it; not worth inventing alongside the
decision that accepts the window.

**Do nothing and leave it undocumented.** Rejected: the review that found it
noted this is the shape a future incident actually presents as, and an accepted
cost nobody wrote down is indistinguishable from a defect nobody found.

## Consequences

Easy: the fencing decision stays a single unconditional rule — a lost
conditional write means stop — which is the property the simulation harness
(M4.12, M4.13) will assert I1–I5 against without a special case.

Hard: an operator seeing a lease failover with no corresponding node failure
has to know this window exists. The class javadoc and this ADR are where they
will find it, and **M4.3q** owns making an ambiguous renew countable so the
frequency is measurable rather than theoretical. ⚠️ An earlier draft pointed at
M4.14, which is the compaction-trigger histogram and never touches the lease —
the same misattribution M4.3d's row had already had to correct once.

Foreclosed: nothing. If M8's or M9's measurements show this firing often enough
to matter, the second alternative above is still available — and the ADR that
supersedes this one will have the frequency number that this one cannot.
