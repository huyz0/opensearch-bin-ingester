# 0037. Seal the burned run, not just where the walk stops

Status: accepted — amends 0029, whose decision holds and whose implementation
  does not reach the case below
Date: 2026-09-07
Requirements: FR-11, NFR-11
Research: docs/research/30-design-space/08-failure-domains-and-resilience.md:12 —
  already overturned by 0029; this record narrows *how far* the overturning goes,
  and adds nothing the corpus needs to retract a second time.

## Context

ADR-0029 decided that a successor seals the chain it is about to inherit from,
and reads it only after the seal lands. That decision is right and is not
reopened here. Its **implementation does not reach a case the same simulation
now produces**, and I2 is violated in production as a result.

⚠️ **Found by the M4.13 sweep, at a seed count no existing test reaches.**
Measured over 300 seeds at 120 rounds and 3 pods:

| profile | seeds violating | violations |
|---|---|---|
| no faults | **0** | 0 |
| `duplicatePut` only | **0** | 0 |
| `unreachable` only | 45 | 50 |
| `ambiguousPut` only | 159 | 230 |
| ROUGH (all three) | 56 | 60 |

Clean runs are clean. The witness is seed 153 under `ambiguousPut` at 0.05, with
**zero successful zombie writes** — so this is not a fenced writer's bytes being
counted:

```
epoch 3: [0:CONT<-2/8] [1:D 17+1]     commits offset 17, chain NEVER SEALED
epoch 4: [0:SEAL@5]                   leader fenced at slot 0, sealed by epoch 5
epoch 5: [0:CONT<-4/0] [1:D 17+2]     REASSIGNS offset 17
```

An `ambiguousPut` fired on exactly
`ctl/log/0/0000000000000003/0000000000000001.delta` — the write **landed** and
the response was **lost**, so the producer was never acked.

⚠️ **Two readers of one cluster then disagree about what lives at offset 17**,
which is what makes this corruption rather than a tolerable discard:

```
reader@3 = 18      offset 17 holds record A
reader@4 = 18      offset 17 holds record A
reader@5 = 23      offset 17 holds record B
```

architecture.md:92 states why that is fatal: *"with `all_active` replicas, two
copies converge only because record R is at offset N on both."*

⚠️ **THE MECHANISM IS 0029's OWN WALK STOPPING ONE EPOCH TOO EARLY.** 0029's
decision says the walk-back reaches *"the first ancestor that was genuinely
opened"*. `ChainReplay.neverOpened` cannot deliver that:

```java
private static boolean neverOpened(ChainEntry first, boolean atOrigin) {
    if (first == null) {
        return !atOrigin;      // an EMPTY chain at the START is NOT stepped over
    }
    return first instanceof Seal sealed && sealed.sequence() == 0;
}
```

`firstInheritableAncestor` starts with `atOrigin = true`, so a `prevEpoch` that
is **empty at that instant** — the lease acquired, nothing written yet — stops
the walk. The successor then seals the burned epoch, inherits nothing from it,
and leaves the genuine ancestor unsealed and still growing:

```
epoch 5 starts -> firstInheritableAncestor(4) -> epoch 4 is EMPTY
               -> caveat: not walked past    -> returns 4
               -> seals 4, inherits nothing, epoch 3 LEFT UNSEALED
```

`ChainReplay` predicts this seed in its own javadoc — *"a `prevEpoch` that is
completely empty ... is not walked past here ... That gap is M4.23's"* — so the
caveat was known; what was not known is that it defeats 0029 and produces I2.

⚠️ **THE RUN IS SHORT, and that number is the whole reason this record can
decide differently from 0029's rejected (a).** Burned-epoch run lengths,
measured over 100 ROUGH seeds — 2,566 runs, 4,014 burned epochs:

| run length | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
|---|---|---|---|---|---|---|---|
| occurrences | 1610 | 615 | 237 | 67 | 28 | 8 | 1 |

**mean 1.56, max 7.**

## Decision

**A successor seals every burned epoch it steps over, and keeps walking to the
first genuinely opened ancestor, which it also seals — then reads.**

Concretely: `neverOpened` steps over an **empty** chain as well as one sealed at
slot 0, so `firstInheritableAncestor` reaches the chain whose offsets are
actually inherited; and every epoch the walk crosses is sealed on the way, not
only the one it lands on.

Both halves are load-bearing and neither works alone:

- **Walking further** is what makes the inherited mark stop moving, which is
  0029's property, now actually reached.
- **Sealing the run** is what fences the burned epochs' own leaders. A burned
  epoch is empty because its leader acquired the lease and had not written
  *yet* — not because it is dead. Stepping over it without sealing it leaves it
  free to open and write beside the successor.

Order is 0029's and is unchanged: seal before reading, or the read predates the
write it is meant to exclude.

## Alternatives considered

**(a) Make the writer's crossing fold what a reader folds.** ⚠️ **IMPOSSIBLE,
and it was the first hypothesis.** MEASURED: the identical call
`ChainReplay.inherited(4, 0)` answers **18** against the final store and
answered **17** at `open`. The divergence is **timing, not logic** — a writer
cannot fold bytes that do not exist when it reads. Recorded because it is the
obvious fix and the measurement is the only thing that rules it out.

**(b) Let an unsealed ancestor contribute nothing.** MEASURED and rejected: it
converts every I2 into a `gap` — 230 of them under `ambiguousPut` — because the
inherited base is genuinely needed. It trades a wrong name for a wrong answer.

**(c) Step over an empty chain without sealing it.** The one-line version of
this decision, and it opens the **mirror hazard**: the burned epoch is left
unsealed and its leader, which holds the lease and merely has not written, can
open it and write beside the successor. This is presumably why the caveat exists.
Rejected — it exchanges one I2 for another.

**(d) Seal every unsealed ancestor on takeover.** This is 0029's rejected (a),
and it stays rejected on the ground 0029 gave: failover latency, unbounded in
epochs skipped, on the path NFR-9 bounds. ⚠️ **This record is not that option.**
0029 measured ~70 epochs per takeover; the *burned run* measured here is **mean
1.56, max 7** — the walk stops at the first opened ancestor, so the bound is the
length of one gap, not the history. Two orders of magnitude is the difference
between the two proposals, and it is why the same latency argument does not
carry over.

**(e) A range seal — one record meaning "every epoch below E is sealed".** Still
0029's (c), still rejected for the reason given there: it stops a stale write
from being *believed* rather than from *happening*, so it needs reader
semantics, and those are M5's. ⚠️ 0029 said it would be *"worth revisiting if
the crossing ever needs to skip more than one inheritable ancestor"* — it does
not. The run measured here is a run of **burned** epochs before a single
inheritable ancestor, so that trigger has not fired.

**(f) Mint the epoch only after `start` succeeds.** 0029's (d), unchanged: it
narrows the gap and closes nothing, since one skipped ancestor is all the defect
needs.

## Consequences

**Failover does up to 7 extra conditional writes, mean 1.56.** At the cost model's
`$5.0 × 10⁻⁶` per PUT this is unmeasurable in money; the cost that matters is
latency, and it is bounded by the burned run rather than by history. ⚠️ **The
bound is measured on the simulation's fault rates, not derived** — a store outage
spanning many acquisitions would lengthen the run, and nothing here caps it. If
that becomes real, (e) is the escape and its trigger is now written down.

**Each seal may redrive**, exactly as 0029 records for its single seal, because
sealing races the zombie it is fencing. The redrive budget is per seal, so the
worst case multiplies — the argument for keeping the run short rather than
sealing everything.

**M4.23 is closed by this on BOTH its sides**, not merely referenced. The row
framed it as a checker-visibility concern; the caveat is also the production
defect. And the two halves cannot ship apart: once the walk skips a burned run,
the successor's `CONTINUE` names an ancestor whose seal was written for an
EARLIER successor that then burned, and `checkLink`'s `continuedAt != epoch`
test fails every such takeover. MEASURED: the production fix alone turns 0 I2
violations into **63,081 `link` violations** over 300 `unreachable`-only seeds.
So `checkLink` now accepts a seal naming any epoch at or below this one,
PROVIDED every epoch in between is itself burned — and it READS each of them
rather than trusting the walk that chose the ancestor, which is M4.19's lesson.

**A fork is only a fork when the sibling exists.** `InvariantsAcrossChainsTest`'s
fork fixture left the named successor absent from the store, which is
byte-identical to a burned epoch — so it asserted a fork and built a burned run.
Under this decision that fixture stopped discriminating and had to build the
sibling chain it names. ⚠️ That is a test getting STRONGER, not an assertion
weakened: the shape it claims to test is now the shape it builds.

**The reader arithmetic still exists and is still needed.** 0029 forecast that
naming the genuine ancestor in the `CONTINUE` would make the walk-back
unnecessary for this case; that remains true for the crossing, and the walk stays
for chains written before this decision.

⚠️ **What this does NOT fix: the landed-but-unacked delta itself.** Epoch 3's
records at 17 were written and never acknowledged. This decision stops a
successor from *reassigning* their offsets; it does not decide whether those
records are ever delivered. That is I4's permitted half — uncommitted records may
be dropped — and it stays permitted.
