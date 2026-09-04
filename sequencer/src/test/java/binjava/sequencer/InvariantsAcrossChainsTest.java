// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static binjava.sequencer.InvariantFixtures.PREFIX;
import static binjava.sequencer.InvariantFixtures.counts;
import static binjava.sequencer.InvariantFixtures.delta;
import static binjava.sequencer.InvariantFixtures.put;
import static binjava.sequencer.InvariantFixtures.start;
import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.backend.MemoryBinStore;
import binjava.format.Continue;
import binjava.format.Seal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * What the checker sees ACROSS the join between two chains.
 *
 * <p>⚠️ SPLIT FROM {@code InvariantsTest} at 502 lines against the 500-line
 * limit -- code-structure.md rule 1: split it, never raise it. The seam is the
 * one the milestone keeps turning on: everything here concerns the BOUNDARY --
 * which chain a successor continues from, whether that chain was sealed for it,
 * where its barrier really is, and which of its offsets may be inherited. The
 * sibling class asserts what holds within a single chain.
 *
 * <p>⚠️ THIS IS WHERE EVERY DEEP FINDING OF M4 HAS LANDED. The checker could not
 * disagree with production because it took the boundary from the successor's own
 * CONTINUE; then it believed a chain that DISOWNED its link; then it took the
 * LAST seal where the barrier is the FIRST, which made a successor inheriting a
 * fenced writer's offsets report clean. Each was found by measurement, and each
 * arm below is falsifiable against its own mutation.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class InvariantsAcrossChainsTest {

    @Test
    void aTakeoversTWOChainsAreCheckedACROSSTheirJoin() throws Exception {
        // ⚠️ THIS TEST INVERTED WHEN M4.6e LANDED, and the inversion is the
        // point. Before it, per-chain checking could not see a boundary: both
        // chains were internally consistent while the successor RESTARTED the
        // stream at 0, and the checker could not see across the join at all and was
        // asserted to, because reporting "no violations" about an unexamined
        // property is false comfort.
        // ⚠️ It was written NOT to assert the successor's restarted offset,
        // deliberately, so that M4.6e would not have to invert an assertion
        // ABOUT THE BROKEN BEHAVIOUR alongside its fix -- the call M4.5 made
        // about the post-seal commit. What flipped is a capability flag, which
        // is what a capability flag is for.
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer first = start(store, "pod1");
        first.commit(new CommitRequest("pod1", 1, "seg/a", counts(6)));
        // ⚠️ A SECOND DELTA, and it is the only reason the fold's AGGREGATION is
        // exercised anywhere. Every predecessor chain in this file used to carry
        // exactly one, so `merge(..., Math::max)` could become `Math::min` with
        // all 18 tests green -- a real false negative, not just an untested line:
        // with two deltas the checker reports the rewind as I2 and the mutant
        // reports it as a `gap`, so the KIND is wrong and M4.13 pins exact counts.
        first.commit(new CommitRequest("pod1", 2, "seg/a2", counts(2)));
        first.close();
        LocalSequencer second = start(store, "pod2");
        var resumed = second.commit(new CommitRequest("pod2", 1, "seg/b", counts(2)));

        assertThat(Invariants.checkChain(store, PREFIX, 1))
                .as("the sealed predecessor is clean").isEmpty();
        assertThat(Invariants.checkChain(store, PREFIX, 2))
                .as("and so is the successor, checked against what it INHERITED "
                        + "rather than against 0")
                .isEmpty();
        assertThat(resumed.runs().getFirst().firstOffset())
                .as("because it resumes the stream where its predecessor stopped")
                .isEqualTo(8);
    }

    @Test
    void aCONTINUENamingAChainNobodySEALEDIsReportedAsABrokenLINK() throws Exception {
        // ⚠️ THE CHECKER MUST BE ABLE TO DISAGREE WITH PRODUCTION, and round-1
        // review proved it could not: the boundary was taken from the
        // successor's own CONTINUE, which is byte-for-byte what production hands
        // to `open`. Mutating production to `open(prevEpoch, 0)` -- every stream
        // restarting at 0 on every failover -- left the checker SILENT.
        // ⚠️ So the link is now read from the PREDECESSOR's end, and this pins
        // the case M4.16 exists for: a chain continued from one nobody sealed,
        // whose writer therefore met no barrier and may still be assigning.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog first = new CommitLog(store, PREFIX, 1);
        put(store, first, 0, new Continue(0, 0, 0).encode());
        put(store, first, 1, delta(1, 3, 0));
        // ⚠️ NO SEAL on epoch 1, deliberately.

        CommitLog second = new CommitLog(store, PREFIX, 2);
        put(store, second, 0, new Continue(0, 1, 1).encode());
        put(store, second, 1, delta(1, 2, 3));

        assertThat(Invariants.checkChain(store, PREFIX, 2))
                .extracting(Invariants.Violation::invariant)
                .as("an unsealed predecessor is a broken link, however tidy the offsets look")
                .contains("link");
    }

    @Test
    void aCONTINUEThatMISSTATESWhereItsPredecessorEndedIsReported() throws Exception {
        // ⚠️ THE EXACT MUTATION round-1 review used, as a fixture: a leader that
        // seals its predecessor correctly but links to the wrong slot.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog first = new CommitLog(store, PREFIX, 1);
        put(store, first, 0, new Continue(0, 0, 0).encode());
        put(store, first, 1, delta(1, 3, 0));
        put(store, first, 2, new Seal(2, 2).encode());

        CommitLog second = new CommitLog(store, PREFIX, 2);
        // ⚠️ Claims the predecessor ended at 0; its SEAL sits at 2.
        put(store, second, 0, new Continue(0, 1, 0).encode());
        put(store, second, 1, delta(1, 2, 3));

        assertThat(Invariants.checkChain(store, PREFIX, 2))
                .extracting(Invariants.Violation::invariant)
                .as("the successor's claim is checked against the predecessor's own SEAL")
                .contains("link");
    }

    @Test
    void TWOChainsCONTINUINGFromONEPredecessorIsAFORKAndIsReported() throws Exception {
        // ⚠️ THE `continuedAt` ARM was the only thing that detects two chains
        // claiming the same predecessor, and deleting it left the suite green.
        // ⚠️ A FORK IS THE WORST CASE THIS CHECKER EXISTS FOR: both successors
        // inherit the same history and both hand out the same offsets to
        // different records, which is I2 at its most damaging -- and each chain
        // examined ALONE looks perfectly well formed.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog first = new CommitLog(store, PREFIX, 1);
        put(store, first, 0, new Continue(0, 0, 0).encode());
        put(store, first, 1, delta(1, 3, 0));
        // ⚠️ The SEAL names epoch 2, so epoch 3's claim on it is false.
        put(store, first, 2, new Seal(2, 2).encode());

        CommitLog fork = new CommitLog(store, PREFIX, 3);
        put(store, fork, 0, new Continue(0, 1, 2).encode());
        put(store, fork, 1, delta(1, 2, 3));

        assertThat(Invariants.checkChain(store, PREFIX, 3))
                .extracting(Invariants.Violation::invariant)
                .as("epoch 3 continues from a chain sealed for epoch 2 -- the predecessor's "
                        + "own SEAL is the authority, and it names somebody else")
                .contains("link");
    }

    @Test
    void aChainCarryingDELTASWithNoCONTINUEAtAllIsReportedAsABrokenLINK() throws Exception {
        // ⚠️ THIS ARM WAS SHIPPED UNFALSIFIABLE and review measured it: deleting
        // it left the whole suite green. It is one half of the fix for a BLOCKING
        // finding -- "a chain that omits or DISOWNS its link was still believed,
        // so `open(0, 0)` reported 0 violating seeds over 240 chains" -- so
        // shipping it deletable is the very shape this commit deletes
        // `crossEpochOffsetsChecked()` for. The class javadoc leans on it by name.
        // ⚠️ WHAT IT CATCHES: a chain with committed work and no opening link at
        // all. Its offsets rest on nothing, so every one of them is a fresh start
        // that no predecessor authorised.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 2);
        // ⚠️ NO Continue at slot 0 -- the chain begins with committed work.
        put(store, log, 0, delta(0, 3, 0));
        put(store, log, 1, delta(1, 2, 3));

        assertThat(Invariants.checkChain(store, PREFIX, 2))
                .extracting(Invariants.Violation::invariant)
                .as("committed deltas with no CONTINUE is a broken link, however tidy the "
                        + "offsets inside the chain look")
                .contains("link");
    }

    @Test
    void aLATERChainDISOWNINGAPredecessorThatCarriesDELTASIsReportedAsABrokenLINK()
            throws Exception {
        // ⚠️ THE OTHER HALF, and the one that was the actual fix for round 2's
        // blocking finding -- also shipped deletable, measured. `open(0, 0)` makes
        // every successor claim it had no predecessor; without this arm the
        // checker believed it and reported zero violating seeds over 240 chains
        // while every stream restarted at 0 on every failover.
        // ⚠️ EPOCH 1 MAY SAY IT: epoch 0 is the reserved unleased chain, so a
        // FIRST leader naming it is telling the truth. Anyone else is not.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog first = new CommitLog(store, PREFIX, 1);
        put(store, first, 0, new Continue(0, 0, 0).encode());
        put(store, first, 1, delta(1, 4, 0));
        put(store, first, 2, new Seal(2, 2).encode());

        CommitLog second = new CommitLog(store, PREFIX, 2);
        // ⚠️ Claims prevEpoch 0 -- "there was no predecessor" -- while epoch 1
        // demonstrably carries four assigned offsets.
        put(store, second, 0, new Continue(0, 0, 0).encode());
        put(store, second, 1, delta(1, 2, 0));

        assertThat(Invariants.checkChain(store, PREFIX, 2))
                .extracting(Invariants.Violation::invariant)
                .as("disowning a predecessor that carries committed deltas is a broken link -- "
                        + "this is the arm that catches every stream restarting at 0")
                .contains("link");
    }

    @Test
    void aSECONDSealPastAFencedWritersDeltaDoesNOTMoveTheBarrier() throws Exception {
        // ⚠️ THE MASKING BUG THIS PINS was measured by review: the seal scans kept
        // the LAST seal while the I5 arm keeps the FIRST, so the file held two
        // definitions of "the seal" and the successor inherited a fenced writer's
        // offsets with the boundary reported CLEAN.
        // ⚠️ PRODUCTION-REACHABLE, not hypothetical: `ChainReplay.chainEnd`
        // inspects only the LAST key of a chain, so when the highest slot is a
        // post-seal delta it reports `seal == null`, `CommitLog.seal`'s
        // `sealedAt != null` guard does not fire, and the next taker writes a
        // SECOND seal beyond the real barrier.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog first = new CommitLog(store, PREFIX, 1);
        put(store, first, 0, new Continue(0, 0, 0).encode());
        put(store, first, 1, delta(1, 3, 0));
        // ⚠️ THE REAL BARRIER, at slot 2.
        put(store, first, 2, new Seal(2, 2).encode());
        // ⚠️ A fenced writer's delta AFTER it, then a second seal past that.
        put(store, first, 3, delta(3, 5, 3));
        put(store, first, 4, new Seal(4, 3).encode());

        CommitLog third = new CommitLog(store, PREFIX, 3);
        // ⚠️ Claims the predecessor ended at the SECOND seal and resumes at 8 --
        // inheriting the fenced writer's five offsets.
        put(store, third, 0, new Continue(0, 1, 4).encode());
        put(store, third, 1, delta(1, 2, 8));

        // ⚠️ TWO ARMS, ASSERTED SEPARATELY, because `isNotEmpty()` alone
        // constrained only their CONJUNCTION -- review measured that reverting
        // either scan on its own left the suite green while reverting both failed.
        // A test that needs two bugs at once to notice either is a test that
        // notices neither.
        var found = Invariants.checkChain(store, PREFIX, 3);
        assertThat(found).extracting(Invariants.Violation::invariant)
                .as("the LINK arm: the CONTINUE names the SECOND seal, and the predecessor's "
                        + "barrier is its FIRST -- taking the last one makes this claim look true")
                .contains("link");
        assertThat(found).extracting(Invariants.Violation::detail)
                .as("and the FOLD arm, which is the one that decides whether a fenced writer's "
                        + "offsets are INHERITED: resuming at 8 must be seen as beyond what the "
                        + "chain assigned up to its barrier, not as continuing correctly")
                .anyMatch(d -> d.contains("resumes at 8"));
    }

    @Test
    void theFOLDStopsAtTheFIRSTSealEvenWhenTheLINKIsWellFormed() throws Exception {
        // ⚠️ THE FOLD ARM IN ISOLATION. The fixture above needs the link to be
        // wrong too; this one keeps the link PERFECT -- the CONTINUE names the
        // first seal, which is where the chain really ended -- so the only thing
        // that can go wrong is the fold reaching past it. That isolates the arm
        // review measured as surviving on its own, and it is the arm that matters:
        // it decides whether a fenced writer's offsets are inherited.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog first = new CommitLog(store, PREFIX, 1);
        put(store, first, 0, new Continue(0, 0, 0).encode());
        put(store, first, 1, delta(1, 3, 0));
        put(store, first, 2, new Seal(2, 2).encode());
        // ⚠️ A fenced writer's delta past the barrier, and a second seal after it.
        put(store, first, 3, delta(3, 5, 3));
        put(store, first, 4, new Seal(4, 2).encode());

        CommitLog second = new CommitLog(store, PREFIX, 2);
        // ⚠️ A CORRECT link: names epoch 1 at slot 2, its real barrier.
        put(store, second, 0, new Continue(0, 1, 2).encode());
        // ⚠️ And resumes at 3, which is right IF the fold stopped at the barrier.
        put(store, second, 1, delta(1, 2, 3));

        assertThat(Invariants.checkChain(store, PREFIX, 2))
                .as("with the link well formed, resuming at 3 is correct -- a fold that ran on "
                        + "to the second seal would inherit the fenced writer's five offsets, "
                        + "demand 8, and report a phantom gap against a chain that is right")
                .isEmpty();
    }

    @Test
    void offsetsAreINHERITEDAcrossAChainThatWasNeverOPENED() throws Exception {
        // ⚠️ THE NEVER-OPENED WALK-BACK IS WHAT PRODUCES the only violations the
        // sweep finds, and deleting it left the suite green -- so the mechanism
        // the checker's own javadoc calls load-bearing was unfalsified.
        // ⚠️ THE SHAPE: epoch 3 was acquired and abandoned before its CONTINUE,
        // leaving a SEAL at slot 0 and no offsets. Epoch 4 continues from it, so
        // it must walk BACK to epoch 1 for the history -- and if it does not, its
        // first delta looks like a fresh start and the reassignment goes unseen.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog first = new CommitLog(store, PREFIX, 1);
        put(store, first, 0, new Continue(0, 0, 0).encode());
        put(store, first, 1, delta(1, 5, 0));
        put(store, first, 2, new Seal(2, 4).encode());

        // ⚠️ Epoch 3: never opened. A SEAL at slot 0 and nothing else.
        put(store, new CommitLog(store, PREFIX, 3), 0, new Seal(0, 4).encode());

        CommitLog fourth = new CommitLog(store, PREFIX, 4);
        put(store, fourth, 0, new Continue(0, 3, 0).encode());
        // ⚠️ Epoch 1 assigned 0..4, so 5 is next. Resuming at 2 REASSIGNS three.
        put(store, fourth, 1, delta(1, 2, 2));

        assertThat(Invariants.checkChain(store, PREFIX, 4))
                .extracting(Invariants.Violation::invariant)
                .as("the history is inherited THROUGH the never-opened chain, so the "
                        + "reassignment against epoch 1 is seen -- without the walk-back this "
                        + "chain looks like it started from nothing")
                .contains("I2");
    }

    @Test
    void offsetsBEYONDThePredecessorsSEALAreNOTInherited() throws Exception {
        // ⚠️ THE SEAL-BOUNDED FOLD is what the checker's javadoc calls the
        // independence mechanism, and it was unconstrained in BOTH directions:
        // `upTo = Long.MAX_VALUE` survived, and deleting the `break` survived.
        // ⚠️ WHY IT MATTERS: entries after a SEAL in the predecessor were written
        // by a FENCED writer. Inheriting them would make the successor start
        // above the offsets any reader will ever apply, turning a zombie's write
        // into a permanent hole -- and would MASK the I2 violation that zombie
        // caused, because the successor would appear to continue from it
        // correctly.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog first = new CommitLog(store, PREFIX, 1);
        put(store, first, 0, new Continue(0, 0, 0).encode());
        put(store, first, 1, delta(1, 3, 0));
        put(store, first, 2, new Seal(2, 2).encode());
        // ⚠️ BEYOND THE SEAL: a fenced writer's delta at 3..7, which no reader
        // applies and no successor may inherit.
        put(store, first, 3, delta(3, 5, 3));

        CommitLog second = new CommitLog(store, PREFIX, 2);
        put(store, second, 0, new Continue(0, 1, 2).encode());
        // ⚠️ Correct: epoch 1 assigned 0..2 UP TO ITS SEAL, so 3 is next.
        put(store, second, 1, delta(1, 2, 3));

        assertThat(Invariants.checkChain(store, PREFIX, 2))
                .as("resuming at 3 is correct because the fold stops at the SEAL -- folding "
                        + "the fenced writer's delta in would demand 8 and report a phantom gap")
                .isEmpty();
    }

    @Test
    void offsetsAreInheritedTRANSITIVELYAcrossMORETHANONEHop() throws Exception {
        // ⚠️ THE TRANSITIVE HOP WAS UNFALSIFIED, measured: discarding the
        // recursion's RESULT -- so the ancestry walk still runs and still reports
        // ancestor violations, but no ancestor's OFFSETS travel past one hop --
        // left both invariant test classes entirely green. The only thing in the
        // tree that noticed was an UNTRACKED M4.20 file, which is no coverage at
        // all: a fresh clone has it not at all, and M4.19 ships the arm.
        // ⚠️ THE SHAPE NEEDS TWO HOPS TO BITE. Epoch 2 is a chain that was opened
        // and sealed without ever committing -- it carries a CONTINUE and a SEAL
        // and no deltas -- so epoch 3's history lives TWO links back, in epoch 1.
        // One hop reaches epoch 2 and finds nothing; only the recursion reaches
        // the offsets that were actually assigned.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog first = new CommitLog(store, PREFIX, 1);
        put(store, first, 0, new Continue(0, 0, 0).encode());
        put(store, first, 1, delta(1, 6, 0));
        put(store, first, 2, new Seal(2, 2).encode());

        CommitLog middle = new CommitLog(store, PREFIX, 2);
        put(store, middle, 0, new Continue(0, 1, 2).encode());
        put(store, middle, 1, new Seal(1, 3).encode());

        CommitLog third = new CommitLog(store, PREFIX, 3);
        put(store, third, 0, new Continue(0, 2, 1).encode());
        // ⚠️ Epoch 1 assigned 0..5, so 6 is next. Resuming at 2 REASSIGNS four
        // offsets -- invisible unless the inheritance crossed BOTH hops.
        put(store, third, 1, delta(1, 3, 2));

        var found = Invariants.checkChain(store, PREFIX, 3);
        assertThat(found).extracting(Invariants.Violation::invariant)
                .as("the rewind against a high-water mark two links back is reported -- without "
                        + "the transitive hop this chain looks like it inherited nothing and "
                        + "started cleanly")
                .contains("I2");
        assertThat(found).extracting(Invariants.Violation::detail)
                .as("and it names epoch 1's mark, 6, so the assertion cannot be satisfied by a "
                        + "violation raised for some other reason")
                .anyMatch(d -> d.contains("assigned up to 6"));
    }
}
