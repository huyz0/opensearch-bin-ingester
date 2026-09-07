// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.format.ChainEntry;
import binjava.format.CommitDelta;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import binjava.format.Seal;
import binjava.sequencer.Invariants.Violation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * I3 and I4, the invariants about what a READER applied (M4.13a).
 *
 * <p>⚠️ A SEPARATE FILE BECAUSE code-structure.md rule 1 made it one:
 * {@link Invariants} reached 571 lines, and the seam the split follows is a
 * real one rather than a line count -- everything left there is a predicate
 * over the STORED BYTES, while this judges an OBSERVATION that leaves no trace
 * in them. {@link AckOrderInvariants} is on this side of that seam too, and
 * M4.13g moved it here rather than leaving the split half done.
 */
final class ReaderInvariants {

    private ReaderInvariants() {
    }

    /**
     * What one reader derived: its epoch and its next-offset per stream.
     *
     * <p>⚠️ THE TWO TRAVEL TOGETHER BECAUSE APART THEY DISAGREE. An earlier
     * shape took the epoch as a separate argument, and the misuse it invited
     * was measured on a CLEAN four-epoch history with production's own reader:
     * judged at an epoch below its own, the fold stops early and a reader that
     * legitimately advanced into a later chain reads as having applied a
     * discarded suffix -- spurious I3s on three of four epochs. The natural
     * caller cannot make that mistake now, because {@link #of} takes both from
     * one object.
     *
     * <p>⚠️ AND IT MUST BE NO OLDER THAN THE CHAIN IT IS JUDGED AGAINST. The
     * checker folds the bytes as they are NOW, so a view taken before a commit
     * that has since landed reads as a reader that dropped records. MEASURED on
     * a clean history: recover at epoch 1, take the view, land one more delta,
     * and the verdict is "I4 ... up to 3, dropping records the chain committed
     * up to 7". Nothing was dropped. In M4.13's sweep the natural way to make
     * this mistake is to hand it the live writer's log while commits are still
     * in flight; take the view after the workload quiesces.
     *
     * <p>⚠️ AND A BROKEN VIEW IS STILL EXPRESSIBLE, which is the point of the
     * whole seam: the constructor takes any epoch and any map, so a test can
     * hand the checker a reader that over-applied. What the record removes is
     * an epoch and a view that came from DIFFERENT readers, which is not a
     * violation of anything -- it is a caller bug wearing an invariant's name.
     */
    record ReaderView(long epoch, Map<RunKey, Long> offsets) {

        /** The view production's own reader reports, epoch and offsets in step. */
        static ReaderView of(CommitLog reader) {
            return new ReaderView(reader.epoch(), reader.offsets());
        }
    }

    /**
     * I3 and I4, judged against the view a READER actually derived (M4.13a).
     *
     * <p>⚠️ THE READER'S VIEW IS AN INPUT, NOT SOMETHING RECOMPUTED HERE, and
     * that is the entire point of the signature. M4.19 records what the other
     * shape costs: a checker that re-derived its expectation from production's
     * own input agreed with production's own mistake, and a mutation breaking
     * every failover left it returning EMPTY. So the EXPECTED side below is
     * folded from the stored bytes, stopping at the first {@code SEAL}, while
     * the ACTUAL side is whatever the reader says it got. The two can disagree,
     * which is what makes this an assertion rather than a restatement.
     *
     * <p><b>I3</b> — a reader applies only deltas in a sealed prefix or a later
     * epoch's chain. An offset ABOVE the fold means the reader applied a
     * discarded suffix. ⚠️ "OR A LATER EPOCH'S CHAIN" IS WHY THE FOLD STARTS AT
     * THE INHERITED BASE: a reader at epoch N legitimately carries everything
     * its ancestors committed, and {@code inheritedOffsets} walks them, so a
     * successor's reader is judged against the whole history behind it rather
     * than against one chain's deltas. ⚠️ {@link Invariants#checkChain}'s I5 arm reports that
     * such bytes EXIST, which is the writer's side; whether a reader APPLIED
     * them cannot be read off the store at all.
     *
     * <p><b>I4</b> — committed records are never dropped. An offset BELOW the
     * fold, or a stream the reader never mentions, means the reader lost
     * records the chain had committed. ⚠️ THE REORDERING HALF OF I4 IS NOT
     * VISIBLE HERE, and saying so is the same honesty the I5 entry above owes:
     * a fold is a high-water mark, so runs applied in the wrong ORDER end at
     * the same number. What catches reordering is {@link Invariants#checkChain}'s I2 arm
     * over the bytes -- a run resuming below the mark -- plus the fact that a
     * reader walks the listing in sequence order by construction. A reader that
     * emitted records out of order WITHIN a correct fold would need a
     * per-record consumer trace, which nothing produces yet.
     *
     * <p>⚠️ ONE CALL PER READER, NOT ONE PER EPOCH. {@link Invariants#checkChain}
     * is safe swept over a range of epochs and this is not: it is judged at the
     * reader's OWN epoch, which is why {@link ReaderView} carries it. At any
     * other epoch the fold stops early or is empty, and a reader that
     * legitimately advanced reads as having applied a discarded suffix. MEASURED
     * on a CLEAN four-epoch history with production's own reader: spurious I3s
     * on THREE of the four epochs. A checker that cries I3 at correct behaviour
     * gets silenced, and silencing it is how I3 goes back to unchecked with the
     * invariant list still claiming otherwise.
     *
     * <p>⚠️ EVERY CHAIN HAS A DERIVABLE EXPECTATION, including one that was
     * never opened -- see the branch below and M4.13i for where its base comes
     * from when no CONTINUE names a predecessor.
     *
     * @param view the reader's own epoch and next-offset per stream --
     *     {@link ReaderView#of} for production's reader, or a hand-made one for
     *     a test that needs a broken reader
     */
    static List<Violation> checkReader(BinStore store, String prefix, ReaderView view)
            throws IOException {
        List<Violation> found = new ArrayList<>();
        List<ChainEntry> entries = Invariants.readChain(store, prefix, view.epoch());
        // ⚠️ THREE CASES, AND AN EMPTY CHAIN IS NOT THE SAME AS ONE SEALED AT
        // SLOT 0. An earlier version treated both as "never opened" and walked
        // back for both, on the reasoning that they are the two shapes the
        // protocol gives a leader that acquired and wrote nothing. MEASURED, and
        // the reasoning was wrong: production's reader on an EMPTY own chain
        // derives {} -- `LocalSequencer.start` calls `recover()` before `open()`,
        // so it has not crossed yet -- while on a chain SEALED AT SLOT 0 it
        // derives the predecessor's offsets, because it has. `ChainReplay`
        // encodes exactly that distinction as its `atOrigin` flag, which
        // `Invariants.neverOpened(List)` does not carry.
        // ⚠️ SO WALKING BACK FOR AN EMPTY CHAIN REPORTS A FALSE I4 against
        // production's own reader: the checker inherits three records the reader
        // correctly does not have, and calls the difference a drop.
        // ⚠️ THE VIOLATION LISTS BELOW ARE DISCARDED. Every violation the two
        // derivations can raise is already raised by `checkChain` over the same
        // chain, and a caller running both would see each one twice -- which
        // makes a report of two violations ambiguous between two defects and one
        // counted twice.
        Map<RunKey, Long> expected;
        if (entries.isEmpty()) {
            // Nothing crossed yet, so nothing is inherited -- and this is still
            // JUDGED: a reader claiming offsets its chain never gave it is I3.
            expected = new HashMap<>();
        } else if (Invariants.neverOpened(entries)) {
            // ⚠️ SEALED AT SLOT 0: NO CONTINUE, SO NO LINK, BUT IT DID CROSS.
            // Its own sealed prefix is empty by construction, and the protocol
            // advances epochs by exactly one per acquisition, so the base is the
            // backward walk from `epoch - 1` -- which is what production's
            // reader derives here, measured. This used to return NOT-JUDGED, and
            // the cost was that a reader restarting every stream at 0 across a
            // failover -- the defect `inheritedOffsets`' own javadoc was
            // rewritten to catch -- came back unexamined rather than as I4.
            // ⚠️ `epoch - 1`, AND PASSING `epoch` IS AN EQUIVALENT MUTANT
            // rather than a defect -- measured. This branch runs only when the
            // chain at `epoch` is never-opened, so the walk would step over it
            // on its first iteration and reach the same base. What it would
            // cost is one extra `readChain` per call, which is a LIST plus a GET
            // per entry, so the difference is request count and not correctness.
            expected = CrossEpochInvariants.offsetsSealedInto(
                    store, prefix, view.epoch() - 1, new ArrayList<>(), 0);
        } else {
            expected = CrossEpochInvariants.inheritedOffsets(
                    store, prefix, entries, view.epoch(), new ArrayList<>(), 0);
        }
        Map<RunKey, Long> readerView = view.offsets();

        for (ChainEntry e : entries) {
            if (e instanceof Seal) {
                // ⚠️ THE FIRST SEAL ENDS THE APPLIED PREFIX, and nothing after
                // it counts even if it decodes cleanly. That is the boundary
                // I3 is about, derived here from the bytes rather than asked
                // of the reader whose behaviour is under test.
                break;
            }
            if (e instanceof CommitDelta delta) {
                for (RunCommit run : delta.allRuns()) {
                    expected.merge(run.key(), run.firstOffset() + run.recordCount(), Math::max);
                }
            }
        }

        for (Map.Entry<RunKey, Long> want : expected.entrySet()) {
            long applied = readerView.getOrDefault(want.getKey(), 0L);
            if (applied > want.getValue()) {
                found.add(new Violation("I3",
                        "reader applied stream " + want.getKey() + " up to " + applied
                                + ", beyond the sealed prefix which ends at " + want.getValue()));
            } else if (applied < want.getValue()) {
                found.add(new Violation("I4",
                        "reader applied stream " + want.getKey() + " up to " + applied
                                + ", dropping records the chain committed up to "
                                + want.getValue()));
            }
        }
        // ⚠️ A STREAM THE CHAIN NEVER COMMITTED BUT THE READER REPORTS is I3
        // too, and the loop above cannot see it -- it iterates the EXPECTED
        // keys. A reader inventing a stream has applied something outside the
        // sealed prefix by definition.
        for (Map.Entry<RunKey, Long> got : readerView.entrySet()) {
            if (!expected.containsKey(got.getKey()) && got.getValue() > 0) {
                found.add(new Violation("I3",
                        "reader applied stream " + got.getKey() + " up to " + got.getValue()
                                + ", which the sealed prefix never committed"));
            }
        }
        return found;
    }
}
