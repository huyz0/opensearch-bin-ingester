# 0028. Three shapes in the commit chain, behind a sealed interface

Status: accepted
Date: 2026-09-03
Requirements: FR-11, NFR-11
Research: docs/research/30-design-space/03-metadata-and-cas.md

## Context

Through M3 the commit chain carried one shape: a `CommitDelta`, written with
`putIfAbsent` at a sequential slot. M4's seal protocol needs two more at the
same key. A `SEAL` ends an epoch's chain, so a fenced leader's next write
collides with it rather than slipping past. A `CONTINUE` opens the next epoch's
chain with a link back across the boundary, so a reader can follow the history
without listing epochs.

Both commit **no runs**, and that collides with a guard `CommitDelta` has
carried since M1: it refuses an empty run list, because *"an empty delta would
consume a sequence number and commit nothing, so a replay would see a gap it
cannot explain"*.

The constraint that shapes everything else: **every delta already in a bucket
is v0**, and object-store bytes outlive the process that wrote them by the whole
retention window. A reader must handle the old shape until every possible writer
of it has aged out.

## Decision

**A sealed interface, `ChainEntry`, permitting `CommitDelta`, `Seal` and
`Continue`** — java-style.md rule 11's "records for data, sealed interfaces for
closed sets". A reader that switches over three permitted types is exhaustive by
compilation; a reader that switches over a kind byte is exhaustive by hope, and
`CommitLog.apply` is now the former.

**The version distinguishes layouts, not releases.** Version 0 *is* "a delta" —
it carries no kind field, so a v0 object can only ever be one. Version 1 *is* "a
kinded entry": the 8-byte header is followed by a `KIND_*` uvarint. **A delta
still writes v0**, because nothing about a delta changed; re-encoding one would
churn every byte in the bucket and both golden files to say exactly what v0
already says.

**An unknown kind or version stops; it never skips.** The
`wire-format-change` skill requires this answer be stated rather than left to
whichever branch happens to run, and the safe direction here is not symmetric:
silently ignoring an entry a reader does not understand is precisely how a chain
loses a `SEAL` and a reader goes on applying a discarded suffix — invariant I3.

**`SEAL` carries `continuedAt`, `CONTINUE` carries `{prevEpoch, prevSeq}`** —
the boundary is linked in both directions, per the corpus. Both are written by
the same term within one failover, so neither is a guess about the other.

**`CONTINUE` cannot express an absent previous chain, by construction.** M4.4b
reserved epoch 0 for the unleased chain, so 0 names a *live* epoch and cannot
double as "none". The resolution is not a different sentinel: every chain above
epoch 0 has a predecessor, and epoch 0 is the unleased chain that conceptually
always exists. A first leader at epoch 1 writes `{prevEpoch=0, prevSeq=0}`,
which truthfully says the unleased chain was empty rather than that there was
none.

**Read side first.** This commit teaches every reader all three shapes and adds
the encoders; **nothing writes a `SEAL` or a `CONTINUE` yet** — M4.6 does. That
is the skill's rollout rule: readers lag writers by at least one release.

## Alternatives considered

**Make `SEAL` a `CommitDelta` variant and relax the empty-runs guard.** The
SPEC offered this. Rejected because the guard cannot be relaxed for one case
only: it fires on a *property of the value* (`runs.isEmpty()`), not on the
caller's intent, so relaxing it for the seal relaxes it for the accidental empty
delta the guard was written to catch. Separate types keep the guard exactly as
strict as it was, and make "commits no runs on purpose" a thing the type system
says rather than a comment.

**One record with a `kind` field and mostly-absent columns.** Rejected on
java-style rule 11 and on the M4.3g experience: a record whose fields are only
meaningful in some combinations needs a compact constructor to keep the
combinations honest, and the review of `Belief` had to add exactly that after a
state that meant nothing reached a store call. Three records have no invalid
combinations to police.

**Omit `SEAL`'s forward link and let `CONTINUE`'s backward link carry the
boundary alone.** A first draft of this ADR chose that, on the reasoning that a
forward link would be a second copy of the same fact "written by a different
term at a different moment". **That reasoning is wrong, and the corpus already
had the answer** — `03-metadata-and-cas.md` §7 specifies
`SEAL{ continuedAt: e+1 }`. The seal is written by the NEW leader: it recovers
the old chain through `seq = N`, races for `N+1` *in that chain*, and then opens
its own. So `continuedAt` is the epoch the writer already holds, and the same
process writes both links moments apart — they cannot disagree. The draft was
overturned before it shipped; it is recorded here because the failure was
skipping the corpus and re-deriving a decision it had already made with the
protocol in front of it, which is exactly what the `research` skill exists to
prevent.

## Consequences

**Cost: unchanged, and this is the section a wire-format change most often gets
wrong.** No request count moves. A `SEAL` is 11 bytes and a `CONTINUE` 13, both
far inside any speculative read; deltas are byte-identical to before, so nothing
that coalesced stops coalescing. The chain still costs one `putIfAbsent` per
entry, and recovery still costs one LIST plus one GET per object — the two new
shapes add objects only at epoch boundaries, which scale with **failovers**, not
with records, shards, partitions or indices (non-negotiable 6).

Easy: adding a fourth shape later fails to compile at every reader, which is the
whole point of sealing the interface.

Hard: `CommitDelta.decode` now narrows a `ChainEntry` and throws if handed a
seal, so a caller that can only handle deltas must say so. That is deliberate —
the alternative is a cast that succeeds until the day a seal appears.

Hard, and a protocol decision rather than a detail: **losing a slot to a `SEAL`
is now terminal for the writer.** Every other lost race in this design is
routine — re-read, fold in, retry the next slot — and this one is not, because
a seal is proof the writer is fenced. Making it fatal is what keeps I5 ("no
acknowledged commit beyond a `SEAL` in its own chain"), and getting there was
not free: teaching the reader to understand a seal is precisely what let the
writer fold one in and carry on, where before M4.5 an unreadable version had
stopped it by accident.

⚠️ **That is deliberately only the FENCED half.** A new leader holding a valid
lease must redrive to `N+2` rather than stop, or it surrenders sequencing
cluster-wide; that branch needs the lease and is M4.6's. ⚠️ And it covers the
lost race only: `recover()` still folds a seal in as inert, so a writer whose
recovered state already sits beyond one wins its slot outright and never reaches
this check. M4.6 owns that, because the right behaviour there is not "stop" — a
legitimate cross-boundary reader must read the sealed prefix and follow the
`CONTINUE`.

Foreclosed: nothing. A fifth version can add fields to the kinded layout, and v0
keeps parsing regardless.
