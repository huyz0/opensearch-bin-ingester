# 0029. Seal the chain you inherit from, before you read it

Status: accepted
Date: 2026-09-04
Requirements: FR-11, NFR-11
Research: docs/research/30-design-space/03-metadata-and-cas.md §12 — answers the
  open question *"How do we bound the seal race in the pathological case of a
  leader paused by a long GC pause that resumes after several epochs? (Believed
  safe by I3; needs proof.)"* The proof went the other way.
  docs/research/30-design-space/08-failure-domains-and-resilience.md:12 — *"an
  undetected zombie is safe, merely wasteful"* is **overturned**.

## Context

Found by the commit-protocol simulation, which is what it was built for. The
shape, from a seed dump:

```
epoch  1: [CONT prev=0@0] [D 0..2] [D 3..3] [D 4..4] [D 5..7] [D 8..8] [D 9..9] [D 10..11]
epoch  3: [SEAL]
epoch  4: [CONT prev=3@0] [D 3..3]        <-- reassigns 3
```

**Neither writer ever lost a race.** Epoch 4 acquired the lease, followed its
`CONTINUE` one hop into epoch 3 — sealed at slot 0, never opened, carrying no
offsets — walked back to epoch 1, and inherited its high-water mark *as it stood
at that moment*: offset 2, so it resumed at 3. Epoch 1's leader had by then been
abandoned but **not sealed**, and it went on committing 3..11 into its own chain.
Each chain is individually well-formed. The union violates I2.

⚠️ **The defect is not that a zombie's bytes exist.** I3 already handles that:
the epoch is in the object path, so a fenced leader's writes land where no reader
of a later epoch looks. The defect is that a successor **inherited a moving
high-water mark** — it read the predecessor's offsets from a chain that was
still growing.

⚠️ **Why the gap is wide.** `tryAcquire` mints the epoch *before* `start` does
its work — stat, get, conditional put, seal the predecessor, recover, open — so
under injected faults that work usually fails somewhere, releases, and burns the
epoch it already minted. Measured on the same run: **73 epochs for 3 takeovers**.
Ancestors are therefore skipped in *runs*, not singly, which is why
`LocalSequencer.start`'s "seal `epoch - 1`" covers a vanishing fraction of them.
M4.17 (nothing renews the lease) compounds it: every lapse of a *healthy* leader
burns another epoch.

⚠️ **The seeds have moved twice** as the fault streams were corrected (one shared
`Random`, then a phase-locked scrambler, then per-class scrambled streams), so
this ADR names the **mechanism** and not a seed number. M4.13's sweep pins
whichever seeds reproduce it on the day. One seed reproduced it with **zero
successful zombie writes**, which is what ruled out "a fenced writer's write
lands" as the whole story and forced the reading above.

## Decision

**A successor seals the chain it is about to inherit from, and reads it only
after the seal lands.**

Concretely, in the crossing path: walk back — the same arithmetic
`ChainReplay.replayAncestry` already performs over a never-opened ancestor,
`ChainReplay.java:175-190` — to the first ancestor that was genuinely opened,
the one whose offsets the new chain will inherit; probe its end with
`recoverChainEnd` (a boundary read, not an offset one — it leaves `nextOffsets`
empty by design, `CommitLog.java:164-166`) and `seal` it there; *only then* let
the new chain's `open` cross into it, which is where the offsets actually get
folded, via the existing `ChainReplay.inherited`/`crossFrom` walk
(`CommitLog.java:206-209`) — now reading a chain that is provably closed. Not
the other way round, and not a new offset-reading mechanism: `open` already
walks back through unopened ancestors to find them: what it does not do today
is see any of them sealed.

Three properties follow:

- **The inherited mark stops moving.** After the seal at slot `N`, the zombie's
  next append to `N` loses `putIfAbsent` and it learns it is fenced — M4.5's
  existing losing branch, no new mechanism.
- **The work is O(1) per takeover**, not O(epochs skipped): the walk-back stops
  at the *first* chain with a `CONTINUE`, and that is the only one whose offsets
  are read, so it is the only one that needs sealing.
- **Order is load-bearing.** If the zombie's in-flight write wins the race at
  `N`, the seal redrives to `N+1` (M4.6b's redrive branch) and the *subsequent*
  read picks up the extra delta. Reading first and sealing second re-opens the
  defect exactly, because the read would predate the write it is meant to
  exclude.

## Alternatives considered

**(a) Seal every unsealed ancestor on takeover.** Rejected — but **not on cost**,
which an earlier draft of this record got wrong. Seals are failover-only, so
70 extra conditional writes per takeover is a few cents a month. It is rejected
on **failover latency**, which NFR-9 bounds and M4.9 exists to bound: the work
grows without limit in the number of skipped epochs, on the one path that must
stay quick. It also races the same zombies it is trying to stop, so each seal may
redrive. The decision above is this option restricted to the one ancestor that
matters, which is what makes it bounded.

**(b) Make a zombie's write fail on the LEASE rather than on the chain** — check
the lease as part of committing. **Costed, and it is affordable, which an earlier
draft also got wrong by confusing request *count* with *cost*.** From
`00-problem/02-cost-model.md`: a PUT is `$5.0 × 10⁻⁶`, a GET `$4.0 × 10⁻⁷` — a
12.5× ratio. The commit chain at the default 250 ms interval is 4 PUT/s, which
is `4 × 2.592 × 10⁶ × 5 × 10⁻⁶ =` **$51.84/month**, matching the SPEC's $52
exactly. One lease GET per commit *batch* adds 4 GET/s =
`4 × 2.592 × 10⁶ × 4 × 10⁻⁷ =` **$4.15/month, i.e. +8.0%** — not the doubling
"twice the requests" suggests. It scales with batches, never with records,
shards, partitions or indices, so no rule in cost.md § R1/R5/R6 moves.
⚠️ Rejected anyway, on two grounds that are not price: it buys a *narrower*
guarantee than the decision above (a check-then-write window of one round trip
remains, where sealing closes the mechanism outright), and it puts a recurring
cost on the steady-state hot path to fix a failover-only defect. 8% of the number
this project exists to control is not worth spending on a problem that has an
O(1) failover-side answer.

**(c) A RANGE SEAL — one record meaning "every epoch below E is sealed".** O(1)
on the failover path and cheaper than (a). Rejected because it stops a stale
write from being *believed* rather than from *happening*, so it needs the reader
side to consult it — and reader semantics are M5's, so choosing this here would
decide something M5 owns. Worth revisiting if the crossing ever needs to skip
more than one inheritable ancestor.

**(d) Mint the epoch only after `start` succeeds**, so a failed start burns
nothing. Narrows the gap by ~95% on the measured numbers and closes **nothing** —
one skipped ancestor is all the defect needs. Worth doing on its own merits (an
epoch consumed by work that failed is a leak with a cost) but it is a separate
row, and offering it as the fix would be treating a symptom. ⚠️ It may also be
impossible as stated: the epoch is what makes the acquisition's conditional write
safe, so it must exist before the write it guards.

## Consequences

**A SIMPLER `CONTINUE` FALLS OUT, and this was found by reading the production
crossing rather than by design.** `LocalSequencer.start` today seals `epoch - 1`
blindly and writes `open(epoch - 1, prevSeq)`, so when `epoch - 1` is a burned
chain the successor's `CONTINUE` names a chain with no offsets in it — and
`ChainReplay.replayAncestry` then reconstructs the real link by arithmetic,
stepping back over never-opened chains one epoch at a time
(`ChainReplay.java:175-190`). Under this decision `start` must already locate the
inheritable ancestor in order to seal it, so it can name **that** chain in the
`CONTINUE` instead. The forward link then points where the offsets actually are,
and the reader's arithmetic walk-back stops being needed for this case at all.

⚠️ The walk-back cannot simply be deleted: chains already written by today's
code carry `CONTINUE`s naming burned epochs, and a reader must keep following
them. So the arithmetic path stays as a compatibility route and stops being the
mechanism the crossing depends on — which is worth having, because it is the
path whose deletion `Invariants` measured as silently un-finding every violation
the sweep reports.

**Easier.** The crossing becomes self-sufficient: a reader following a `CONTINUE`
inherits from a chain that is provably closed, so I2 across a failover stops
depending on how many epochs were burned in between.

**Harder.** `LocalSequencer.start` gains an ordering constraint that is easy to
get backwards and whose violation is invisible in a single-leader test — it needs
the simulation to catch it, which is why M4.13's sweep is the gate on this
change rather than a unit test.

**Foreclosed.** Nothing, but (c) becomes the natural extension if a future
crossing must skip more than one inheritable ancestor.

⚠️ **What this does NOT cover**, stated because "believed safe" is exactly the
phrasing that produced this record: it does not stop a zombie *writing*, only
being inherited from; it does not bound how many epochs a flapping cluster burns
(that is (d), and M4.17's missing renew tick); and it says nothing about what a
reader does with a chain sealed while it is mid-walk, which is M5's.
