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
 * What {@link ReaderInvariants#checkReader} will and will NOT judge (M4.13a).
 *
 * <p>⚠️ SPLIT FROM {@link ReaderInvariantsTest} BECAUSE code-structure.md rule 1
 * made it one, and the seam it follows is a real one: that file asks what the
 * checker REPORTS -- which invariant, naming which stream and which offsets --
 * and this one asks whether it examined the chain at all. The two questions have
 * different failure modes. A wrong report is loud; an unexamined chain that
 * returns an empty violation list is silent, and a sweep counting it as a seed
 * that passed is how I3 and I4 go back to NOT-RUN with the invariant list still
 * claiming CHECKED.
 */
class ReaderInvariantsScopeTest {

    private static final RunKey RA = new RunKey(InvariantFixtures.A, 0);
    private static final String PREFIX = InvariantFixtures.PREFIX;

    /**
     * ⚠️ THE OTHER ARM OF THE SAME HOLE -- **M4.13i**, not a law. An epoch with
     * no objects and one sealed at slot 0 are the two shapes the protocol gives
     * a leader that acquired and wrote nothing, and the base is derivable for
     * both. Pinning only one arm is how half a hole survives its own fix.
     */
    @Test
    void anEpochWithNOObjectsAtAllIsNOTJUDGED() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, PREFIX, 1);
        put(store, log, 0, new Continue(0, 0, 0).encode());
        put(store, log, 1, delta(1, 3, 0));

        ReaderInvariants.Verdict verdict =
                ReaderInvariants.checkReader(store, PREFIX, new ReaderView(9, Map.of(RA, 3L)));

        assertThat(verdict.judged()).isFalse();
        assertThat(verdict.violations()).isEmpty();
    }

    /**
     * ⚠️ THE HOLE ITSELF, PINNED SO IT CANNOT CLOSE BY ACCIDENT OR WIDEN
     * UNNOTICED -- **M4.13i**, whose note holds the reasoning. This asserts the
     * DECLINING rather than pretending it is a correctness property, for all
     * three reader shapes, and asserts first that production's own reader DOES
     * derive the base -- so the decline is a checker gap, not protocol truth.
     * Closing M4.13i MUST make this test fail.
     */
    @Test
    void anEpochSEALEDBeforeItsCONTINUEIsNOTJUDGEDAndTHATIsAKnownHole() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog first = new CommitLog(store, PREFIX, 1);
        put(store, first, 0, new Continue(0, 0, 0).encode());
        put(store, first, 1, delta(1, 3, 0));
        put(store, first, 2, new Seal(2, 2).encode());

        CommitLog abandoned = new CommitLog(store, PREFIX, 2);
        put(store, abandoned, 0, new Seal(0, 3).encode());

        CommitLog reader = new CommitLog(store, PREFIX, 2);
        reader.recover();
        assertThat(reader.offsets())
                .as("production DOES derive the base -- so the checker's decline is a gap, not a law")
                .containsEntry(RA, 3L);

        for (Map<RunKey, Long> view : List.of(
                reader.offsets(),               // correct
                Map.<RunKey, Long>of(),         // dropped everything: I4
                Map.of(RA, 500L))) {            // wild over-apply: I3
            ReaderInvariants.Verdict verdict =
                    ReaderInvariants.checkReader(store, PREFIX, new ReaderView(2, view));
            assertThat(verdict.judged()).as("view %s", view).isFalse();
            assertThat(verdict.violations()).as("view %s", view).isEmpty();
        }
    }

    /**
     * ⚠️ THE WRITER'S SIDE OF THAT SAME HISTORY STAYS VISIBLE, which bounds how
     * bad M4.13i is: `checkChain` still reports the zombie a fenced leader
     * appended past the slot-0 seal. Only the READER's side goes unexamined.
     */
    @Test
    void theZOMBIEOnANeverOpenedChainIsStillReportedByCheckChain() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog fenced = new CommitLog(store, PREFIX, 2);
        put(store, fenced, 0, new Seal(0, 3).encode());
        put(store, fenced, 1, delta(1, 4, 3));

        assertThat(Invariants.checkChain(store, PREFIX, 2))
                .extracting(Violation::invariant).contains("I5");
        assertThat(ReaderInvariants.checkReader(store, PREFIX, new ReaderView(2, Map.of(RA, 7L)))
                .judged())
                .as("while the reader's side of the same bytes is M4.13i's hole")
                .isFalse();
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

        ReaderInvariants.Verdict verdict =
                ReaderInvariants.checkReader(store, PREFIX, new ReaderView(1, Map.of(RA, 9L)));

        assertThat(verdict.judged()).isTrue();
        assertThat(verdict.violations()).extracting(Violation::invariant).containsExactly("I3");
    }
}
