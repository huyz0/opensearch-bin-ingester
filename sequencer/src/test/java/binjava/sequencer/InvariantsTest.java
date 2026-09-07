// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static binjava.sequencer.InvariantFixtures.start;
import static binjava.sequencer.InvariantFixtures.PREFIX;
import static binjava.sequencer.InvariantFixtures.FIXED_CLOCK;
import static binjava.sequencer.InvariantFixtures.put;
import static binjava.sequencer.InvariantFixtures.delta;
import static binjava.sequencer.InvariantFixtures.counts;
import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.backend.MemoryBinStore;
import binjava.format.Continue;
import binjava.format.Seal;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * ⚠️ A CHECKER THAT NEVER FAILS IS WORSE THAN NO CHECKER, because it reports
 * safety it never established. `return List.of()` satisfies every happy-path
 * test, so the weight here is on the chains that MUST be reported — one per
 * invariant, each hand-built to violate exactly one thing.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class InvariantsTest {

    @Test
    void aChainAREALSequencerProducedHasNoViolations() throws Exception {
        // ⚠️ The positive control, and deliberately driven through the PRODUCTION
        // path rather than hand-built: a checker tuned to hand-made fixtures
        // would pass here and say nothing about what the system writes.
        // ⚠️ THROUGH THE SHARED FIXTURE, not a re-inlined copy of it. This test's
        // whole job is to be the PRODUCTION baseline, so a `start` that drifted
        // from the one every other fixture uses would make the baseline and the
        // hand-built chains disagree about what the system writes -- with both
        // green. The inlined copy this replaces also left the fixture's own
        // `start` dead, so its seal-redrive budget was unconstrained.
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer seq = start(store, "pod1");
        seq.commit(new CommitRequest("pod1", "i1", 1, "seg/a", counts(3)));
        seq.commit(new CommitRequest("pod1", "i1", 2, "seg/b", counts(4)));

        assertThat(Invariants.checkChain(store, PREFIX, 1))
                .as("a chain opened by a CONTINUE and extended by two commits is clean")
                .isEmpty();
    }

    @Test
    void anEntryBEYONDASealIsReportedAsI5() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        put(store, log, 0, new Continue(0, 0, 0).encode());
        put(store, log, 1, delta(1, 3, 0));
        put(store, log, 2, new Seal(2, 2).encode());
        put(store, log, 3, delta(3, 2, 3));   // the discarded suffix

        assertThat(Invariants.checkChain(store, PREFIX, 1))
                .extracting(Invariants.Violation::invariant)
                .as("the entry past the seal is named as I5")
                .containsExactly("I5");
    }

    @Test
    void anOffsetThatOVERLAPSOneAlreadyAssignedIsReportedAsI2() throws Exception {
        // ⚠️ I2 is "offsets are never REASSIGNED", so the violation to catch is a
        // second delta claiming an offset the chain already gave out -- the shape
        // a failover produces when a new leader does not know where the old one
        // stopped. That is exactly what M4.6e must prevent across a boundary.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        put(store, log, 0, new Continue(0, 0, 0).encode());
        put(store, log, 1, delta(1, 5, 0));   // assigns 0..4
        put(store, log, 2, delta(2, 3, 3));   // claims 3..5 -- 3 and 4 twice

        assertThat(Invariants.checkChain(store, PREFIX, 1))
                .extracting(Invariants.Violation::invariant)
                .as("reassigning an offset is I2, and is named as such")
                .containsExactly("I2");
    }

    @Test
    void aHOLEInTheChainIsReported() throws Exception {
        // ⚠️ Not I1, but a broken chain, and it matters for the seal protocol:
        // consecutiveness is what guarantees a fenced leader COLLIDES with the
        // seal rather than writing past it into an empty slot.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        put(store, log, 0, new Continue(0, 0, 0).encode());
        put(store, log, 2, delta(2, 3, 0));   // slot 1 never claimed

        assertThat(Invariants.checkChain(store, PREFIX, 1))
                .extracting(Invariants.Violation::invariant)
                .as("a gap is reported rather than silently tolerated")
                .contains("chain");
    }

    @Test
    void aSequenceAPPEARINGTWICEInOneChainIsReportedAsI1() throws Exception {
        // ⚠️ THE I1 ARM WAS UNFALSIFIED, and round-1 review measured it: deleting
        // the duplicate-sequence predicate outright left the ENTIRE sequencer
        // suite green. There were fixtures for I5, I2 and chain gaps and none
        // for I1, so the class advertised an invariant nothing could break.
        // ⚠️ WHAT THIS ACTUALLY PINS, stated because the class javadoc used to
        // overclaim: a backend whose write-once primitive slipped would
        // OVERWRITE one key, not create two, so this predicate cannot see that.
        // What it does see is the KEY and the CONTENT disagreeing -- an entry
        // stored at slot 2 that decodes as sequence 1 -- which is the failure a
        // reader has no other way to notice, since it addresses by key and then
        // trusts the decoded sequence.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        put(store, log, 0, new Continue(0, 0, 0).encode());
        put(store, log, 1, delta(1, 3, 0));
        put(store, log, 2, delta(1, 3, 3));

        assertThat(Invariants.checkChain(store, PREFIX, 1))
                .extracting(Invariants.Violation::invariant)
                .as("the repeated sequence is reported, and as I1 specifically")
                .contains("I1");
    }

    @Test
    void aDeltaResumingABOVETheHighWaterMarkIsAGAPAndNOTAnI2Violation() throws Exception {
        // ⚠️ THE `gap` ARM HAD NO FIXTURE, measured: deleting it left the whole
        // suite green. It was introduced when I2 was SPLIT -- the predicate used
        // to fire on `firstOffset != expected` in both directions, and only a
        // REWIND reassigns an offset. Resuming ABOVE loses no data and duplicates
        // nothing; it is a hole, which readers and NFR-11 care about for other
        // reasons. Without this fixture the half of the split that was newly
        // introduced constrained nothing.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        put(store, log, 0, new Continue(0, 0, 0).encode());
        put(store, log, 1, delta(1, 3, 0));
        // ⚠️ Resumes at 9 where 3 was expected: SIX offsets never assigned.
        put(store, log, 2, delta(2, 2, 9));

        var found = Invariants.checkChain(store, PREFIX, 1);
        assertThat(found).extracting(Invariants.Violation::invariant)
                .as("a forward jump is reported as a `gap`")
                .contains("gap");
        assertThat(found).extracting(Invariants.Violation::invariant)
                .as("and NOT as I2 -- nothing was reassigned, and conflating the two made "
                        + "every severed CONTINUE read as data corruption")
                .doesNotContain("I2");
    }

    @Test
    void anOverlapOfEXACTLYONERecordIsStillReportedAsI2() throws Exception {
        // ⚠️ THE I2 BOUNDARY WAS UNPINNED BY ONE RECORD: the only overlap fixture
        // overlapped by TWO, so mutating the comparison from `< from` to
        // `< from - 1` survived. A one-record reassignment is the smallest real
        // instance of the defect and the likeliest -- an off-by-one in
        // `lastOffset() + 1` produces exactly this.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        put(store, log, 0, new Continue(0, 0, 0).encode());
        put(store, log, 1, delta(1, 3, 0));
        // ⚠️ 0..2 assigned, so 3 is next; resuming at 2 REASSIGNS exactly one.
        put(store, log, 2, delta(2, 2, 2));

        assertThat(Invariants.checkChain(store, PREFIX, 1))
                .extracting(Invariants.Violation::invariant)
                .as("an overlap of ONE record is a reassignment and must be reported")
                .contains("I2");
    }

    @Test
    void theHIGHWATERMARKNEVERGOESBACKWARDSAfterARewind() throws Exception {
        // ⚠️ THE DEFENSIVE `Math.max` WAS UNCONSTRAINED across all 181 tests,
        // measured -- no fixture had a THIRD delta after a rewind, so the running
        // high-water mark's monotonicity was never exercised.
        // ⚠️ WHY IT MATTERS RATHER THAN BEING TIDY: without it, a rewind DROPS the
        // mark to the rewound value, so every delta after the first violation is
        // judged against a mark that went backwards. The count and the KIND of
        // each subsequent violation are then wrong -- and M4.13's sweep pins exact
        // counts, so one reassignment early in a chain would silently change what
        // every seed reports.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        put(store, log, 0, new Continue(0, 0, 0).encode());
        put(store, log, 1, delta(1, 6, 0));
        // ⚠️ THE REWIND: 0..5 assigned, this reassigns 2..3.
        put(store, log, 2, delta(2, 2, 2));
        // ⚠️ AND THE DELTA AFTER IT, which is the one the mark decides. 6 is
        // correct against the true mark; against a mark that fell back to 4 it
        // would read as a two-offset GAP that never happened.
        put(store, log, 3, delta(3, 2, 6));

        var found = Invariants.checkChain(store, PREFIX, 1);
        assertThat(found).extracting(Invariants.Violation::invariant)
                .as("exactly one violation, the rewind itself -- the delta after it resumes "
                        + "correctly against a mark that did NOT go backwards")
                .containsExactly("I2");
    }
}
