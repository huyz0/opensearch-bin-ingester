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
import java.util.List;
import java.util.Map;

/**
 * I3 and I4, the invariants about what a READER applied (M4.13a).
 *
 * <p>⚠️ A SEPARATE FILE BECAUSE code-structure.md rule 1 made it one:
 * {@link Invariants} reached 571 lines, and the seam the split follows is a
 * real one rather than a line count -- everything left there is a predicate
 * over the STORED BYTES, while this judges an OBSERVATION that leaves no trace
 * in them. {@code Invariants.checkAckOrder} is on this side of that seam too
 * and has not moved; it is the next extraction when the file next grows.
 */
final class ReaderInvariants {

    private ReaderInvariants() {
    }

    /**
     * What {@link #checkReader} concluded, including that it could conclude
     * NOTHING.
     *
     * <p>⚠️ "NO VIOLATIONS" AND "COULD NOT JUDGE" ARE DIFFERENT ANSWERS, and
     * collapsing them is how a sweep counts a seed it never examined as a seed
     * that passed. The checker derives its expectation from a chain, and this
     * one declines for a chain that was never opened -- an epoch with no
     * objects at all, or one sealed at slot 0 before its CONTINUE was written,
     * a leader fenced mid-acquisition that M4.13's fault injection produces on
     * purpose.
     *
     * <p>⚠️ THAT IS A KNOWN HOLE, NOT A LIMIT OF THE PROTOCOL: the base IS
     * derivable, and both arms of it are **M4.13i**, whose backlog note holds
     * the measurement and the reason the fix is blocked. ⚠️ IT COSTS I4'S DROP
     * CLAUSE AS WELL AS I3, because this returns before evaluating either.
     *
     * <p>⚠️ THROWING WOULD BE WRONG HERE, and an earlier version did. Neither
     * state is a caller error -- the reader is at its own epoch and its offsets
     * are correct -- so a throw turns a silent hole into a crash on a
     * legitimate state and the sweep fails on clean seeds instead.
     */
    record Verdict(boolean judged, List<Violation> violations) {

        static Verdict notJudged() {
            return new Verdict(false, List.of());
        }
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
     * <p>⚠️ AND WHERE NO EXPECTATION CAN BE DERIVED IT RETURNS
     * {@link Verdict#notJudged()} rather than an empty list -- see that record
     * for why the two are different answers.
     *
     * @param view the reader's own epoch and next-offset per stream --
     *     {@link ReaderView#of} for production's reader, or a hand-made one for
     *     a test that needs a broken reader
     */
    static Verdict checkReader(BinStore store, String prefix, ReaderView view)
            throws IOException {
        List<Violation> found = new ArrayList<>();
        List<ChainEntry> entries = Invariants.readChain(store, prefix, view.epoch());
        // ⚠️ `neverOpened`, NOT `!(first instanceof Continue)`. A chain may
        // legitimately begin with a delta -- epoch 1's cold start writes no
        // CONTINUE -- and refusing to judge those would drop the commonest
        // chain in the suite.
        // ⚠️ AND IT COVERS THE EMPTY CHAIN TOO, which is why there is no
        // `entries.isEmpty() ||` in front of it. There was, and it was dead
        // code: mutation measured that dropping the emptiness clause changed
        // nothing, because `neverOpened` returns true for an empty list. A
        // condition no test can distinguish from its absence is one a reader
        // has to check the callee to understand.
        // ⚠️ THIS IS WIDER THAN IT SHOULD BE -- see the Verdict javadoc and
        // M4.13i. It declines on a class this checker could judge.
        if (Invariants.neverOpened(entries)) {
            return Verdict.notJudged();
        }
        Map<RunKey, Long> readerView = view.offsets();

        // ⚠️ DISCARDED. Every violation `inheritedOffsets` can raise is already
        // raised by `checkChain` over the same chain, and a caller running both
        // would see each one twice -- which makes a report of two violations
        // ambiguous between two defects and one counted twice.
        Map<RunKey, Long> expected =
                Invariants.inheritedOffsets(
                        store, prefix, entries, view.epoch(), new ArrayList<>(), 0);
        for (ChainEntry e : entries) {
            if (e instanceof Seal) {
                // ⚠️ THE FIRST SEAL ENDS THE APPLIED PREFIX, and nothing after
                // it counts even if it decodes cleanly. That is the boundary
                // I3 is about, derived here from the bytes rather than asked
                // of the reader whose behaviour is under test.
                break;
            }
            if (e instanceof CommitDelta delta) {
                for (RunCommit run : delta.runs()) {
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
        return new Verdict(true, found);
    }
}
