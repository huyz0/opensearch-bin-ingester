// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static binjava.sequencer.InvariantFixtures.delta;
import static binjava.sequencer.InvariantFixtures.put;
import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.backend.MemoryBinStore;
import binjava.format.Continue;
import binjava.format.RunKey;
import binjava.format.Seal;
import binjava.sequencer.Invariants.Violation;
import binjava.sequencer.ReaderInvariants.ReaderView;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Where {@link ReaderInvariants#checkReader} gets its EXPECTED base from, for
 * the chain shapes that do not simply carry a CONTINUE (M4.13a, M4.13i).
 *
 * <p>⚠️ SPLIT FROM {@link ReaderInvariantsTest} BECAUSE code-structure.md rule 1
 * made it one, and the seam it follows is a real one: that file asks what the
 * checker REPORTS given a base -- which invariant, naming which stream and which
 * offsets -- and this one asks where the base came from. The failure modes
 * differ. A wrong report is loud; a base derived behind production's back is
 * silent, and it shows up as a violation against a reader that did nothing
 * wrong.
 *
 * <p>⚠️ SO THE CASES HERE PIN PRODUCTION AGREEMENT WITH {@code ReaderView.of}
 * WHEREVER THEY CAN. M4.13i's blocking finding was a false I4 against
 * production's own reader, hidden by a hand-fed view -- which asserts the
 * checker against a reader the test invented rather than the one that exists.
 */
class ReaderInvariantsScopeTest {

    private static final RunKey RA = new RunKey(InvariantFixtures.A, 0);
    private static final String PREFIX = InvariantFixtures.PREFIX;

    /**
     * ⚠️ AN EMPTY OWN CHAIN INHERITS NOTHING, AND THAT IS NOT THE SAME AS BEING
     * SEALED AT SLOT 0. An earlier version of this commit treated both as "never
     * opened" and walked back for both. MEASURED, and it was wrong in the
     * direction that matters: production's reader on an empty own chain derives
     * {@code {}} -- {@code LocalSequencer.start} calls {@code recover()} before
     * {@code open()}, so it has not crossed -- while on a slot-0 seal it derives
     * the predecessor's offsets. Walking back for the empty case made the
     * checker inherit three records the reader correctly does not have and call
     * the difference a DROP: a false I4 against production itself, on the state
     * M4.13's fault injection produces when the opening PUT fails.
     *
     * <p>⚠️ SO THIS PINS PRODUCTION AGREEMENT WITH `ReaderView.of`, not a
     * hand-fed map. A hand-fed map is what hid the disagreement: it asserted the
     * checker against a reader this test invented rather than against the one
     * that exists.
     */
    @Test
    void anEMPTYOwnChainInheritsNOTHINGAndProductionAgrees() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        put(store, log, 0, new Continue(0, 0, 0).encode());
        put(store, log, 1, delta(1, 3, 0));
        put(store, log, 2, new Seal(2, 2).encode());

        CommitLog reader = new CommitLog(store, PREFIX, 9);
        reader.recover();
        assertThat(reader.offsets())
                .as("production has not crossed, so it carries nothing")
                .isEmpty();

        assertThat(ReaderInvariants.checkReader(store, PREFIX, ReaderView.of(reader)))
                .as("and the checker must agree with it rather than inherit behind its back")
                .isEmpty();

        // ⚠️ STILL JUDGED, in both broken directions -- otherwise a checker that
        // simply DECLINED on an empty chain satisfies the assertion above, which
        // is verbatim the behaviour this row exists to remove.
        assertThat(ReaderInvariants.checkReader(store, PREFIX, new ReaderView(9, Map.of(RA, 3L))))
                .extracting(Violation::invariant)
                .as("a reader claiming offsets its own chain never gave it is I3")
                .containsExactly("I3");
    }

    /**
     * ⚠️ M4.13i CLOSED THIS, AND THE TEST THAT PINNED THE HOLE IS THIS ONE
     * INVERTED. It used to assert the DECLINE for all three reader shapes, and
     * its note said closing M4.13i must make it fail; it did. A leader fenced
     * before it wrote its CONTINUE leaves a SEAL at slot 0 and nothing else --
     * the shape M4.13's fault injection produces on purpose -- and the base its
     * reader legitimately carries comes from the backward walk, because the
     * protocol advances epochs by exactly one per acquisition.
     *
     * <p>⚠️ ALL THREE SHAPES ARE STILL HERE, now with the verdicts they should
     * always have had: correct is clean, dropped-everything is I4, and a wild
     * over-apply is I3. Those two were the measured cost of the hole -- both
     * came back NOT-JUDGED, and the first is the exact defect
     * `inheritedOffsets`' javadoc was rewritten to catch.
     */
    @Test
    void anEpochSEALEDBeforeItsCONTINUEIsNOWJudgedAgainstItsInheritedBase() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog first = new CommitLog(store, PREFIX, 1);
        put(store, first, 0, new Continue(0, 0, 0).encode());
        put(store, first, 1, delta(1, 3, 0));
        put(store, first, 2, new Seal(2, 2).encode());

        CommitLog abandoned = new CommitLog(store, PREFIX, 2);
        put(store, abandoned, 0, new Seal(0, 3).encode());

        CommitLog reader = new CommitLog(store, PREFIX, 2);
        reader.recover();
        assertThat(reader.offsets()).containsEntry(RA, 3L);

        List<Violation> correct = ReaderInvariants.checkReader(store, PREFIX, ReaderView.of(reader));
        assertThat(correct).isEmpty();

        List<Violation> dropped = ReaderInvariants.checkReader(
                store, PREFIX, new ReaderView(2, Map.of()));
        assertThat(dropped).extracting(Violation::invariant)
                .as("a reader that restarted every stream at 0 across a failover is I4")
                .containsExactly("I4");

        List<Violation> wild = ReaderInvariants.checkReader(
                store, PREFIX, new ReaderView(2, Map.of(RA, 500L)));
        assertThat(wild).extracting(Violation::invariant).containsExactly("I3");
    }

    /**
     * ⚠️ THE WALK STEPS OVER MORE THAN ONE ABANDONED EPOCH, which is what makes
     * it a walk rather than a decrement. Three leaders acquiring and dying in a
     * row is a lease fight, not a pathology, and a base taken from `epoch - 1`
     * alone would be empty for all three -- putting the hole back one epoch
     * further out where no test looks.
     */
    @Test
    void theWalkSTEPSOVERSeveralAbandonedEpochsNotJustOne() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog first = new CommitLog(store, PREFIX, 1);
        put(store, first, 0, new Continue(0, 0, 0).encode());
        put(store, first, 1, delta(1, 3, 0));
        put(store, first, 2, new Seal(2, 2).encode());
        for (long dead = 2; dead <= 4; dead++) {
            put(store, new CommitLog(store, PREFIX, dead), 0, new Seal(0, 3).encode());
        }

        List<Violation> v = ReaderInvariants.checkReader(
                store, PREFIX, new ReaderView(4, Map.of(RA, 3L)));
        assertThat(v)
                .as("three dead epochs back to the one that actually committed")
                .isEmpty();
    }

    /**
     * ⚠️ THE WRITER'S SIDE OF THAT SAME HISTORY STAYS VISIBLE, which bounds how
     * ⚠️ BOTH SIDES OF THE SAME BYTES. `checkChain` reports the zombie a fenced
     * leader appended past the slot-0 seal -- the WRITER's side, that such bytes
     * exist -- and since M4.13i `checkReader` reports the reader that applied
     * it.
     * Before M4.13i only the writer's side was visible, which is the half
     * acceptance criterion 3(a) does NOT ask for: it asks that no entry after
     * fencing is ever applied BY A READER.
     */
    @Test
    void theZOMBIEOnANeverOpenedChainIsReportedOnBOTHSides() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog fenced = new CommitLog(store, PREFIX, 2);
        put(store, fenced, 0, new Seal(0, 3).encode());
        put(store, fenced, 1, delta(1, 4, 3));

        assertThat(Invariants.checkChain(store, PREFIX, 2))
                .extracting(Violation::invariant).contains("I5");
        assertThat(ReaderInvariants.checkReader(store, PREFIX, new ReaderView(2, Map.of(RA, 7L))))
                .extracting(Violation::invariant)
                .as("a reader that applied the zombie is I3 -- the reader's side, which is "
                        + "what criterion 3(a) actually asks for")
                .containsExactly("I3");
    }

    /**
     * ⚠️ A CHAIN THAT LEGITIMATELY OPENS WITH A DELTA IS STILL JUDGED. Epoch
     * 1's cold start writes no CONTINUE, so a guard phrased as "the first entry
     * is not a CONTINUE" would refuse to judge the commonest chain in the suite
     * -- turning the fix above into a checker that judges almost nothing.
     */
    @Test
    void aColdStartChainOpeningWithADELTAIsSTILLJudged() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        put(store, log, 0, delta(0, 3, 0));
        put(store, log, 1, new Seal(1, 2).encode());

        List<Violation> verdict = ReaderInvariants.checkReader(store, PREFIX, new ReaderView(1, Map.of(RA, 9L)));
        assertThat(verdict).extracting(Violation::invariant).containsExactly("I3");
    }
}
