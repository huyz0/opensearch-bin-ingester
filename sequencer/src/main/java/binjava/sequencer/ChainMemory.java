// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.format.CommitDelta;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;

/**
 * The commit chain this term has written, in memory, so a GC pass costs no
 * object-store request (M8.3, M7.25, NFR-3).
 *
 * <p>⚠️ **THREE CLASSES CONSUMED A {@code List<CommitDelta>} AND NOTHING
 * PRODUCED ONE.** {@code RetentionPass}, {@code SegmentGc} and
 * {@code RetainedOffsets} each take one; no {@code src/main} method could build
 * one, not even expensively — {@code CommitLog.recover()} returns {@code void}
 * and {@code ChainReplay.Result} discards the deltas it reads. So M7's
 * "0 LIST, 0 GET" was true of {@code SegmentGc} and UNPROVEN of a pass, and the
 * cheap wrong answer — calling the recovery walk once per pass — costs one LIST
 * per 1,000 deltas plus one GET per delta, every pass, for ever. ADR-0052 § 3
 * puts this row before any GC loop precisely because that answer is available
 * the moment the loop exists.
 *
 * <p>⚠️ **BOUNDED, AND WHAT IT DROPS IS VISIBLE.** A chain grows for the life of
 * a term — one delta per commit interval, for days — so an unbounded copy is a
 * leak with a slow fuse. Past the cap the OLDEST delta goes and
 * {@link Snapshot#complete()} turns false for ever after. A pass over an
 * incomplete chain UNDER-deletes, which is the safe direction; what it must not
 * do is move a retained-offset boundary over segments it never saw, and that is
 * what the flag is for.
 *
 * <p>⚠️ **THE ORDINARY WAY TO STAY SMALL IS {@link #forgetThrough}, NOT THE
 * CAP.** A pass says how far it collected and the chain below that is dead
 * weight: those deltas cannot be condemned twice. The cap is the backstop for a
 * deployment whose GC loop has stopped, which is exactly when an unbounded
 * chain would take the heap.
 *
 * <p>⚠️ **SYNCHRONIZED, BECAUSE TWO THREADS REACH IT AND NEITHER HOLDS A LOCK
 * THE OTHER DOES.** A commit records from the writer thread; a retention pass
 * calls {@link #snapshot()} and {@link #forgetThrough} from its own (M8.5).
 * {@code CommitLog} has no lock of its own — an earlier version of this
 * paragraph claimed it did, which review measured as false — so the mutual
 * exclusion has to be here. A reader walking the live deque while a writer
 * appended would get a {@code ConcurrentModificationException} at best and, at
 * worst, a pass that condemned a segment committed after it started.
 */
public final class ChainMemory {

    /**
     * What the chain holds right now.
     *
     * @param deltas an immutable copy, **in the order the walk read them**:
     *     oldest ancestor chain first, ascending sequence within each chain.
     *     ⚠️ NOT "in sequence order" — an earlier version of this line said so
     *     and it stops being true the moment a chain crosses a takeover, where
     *     the predecessor's delta 7 precedes this term's delta 1
     * @param firstEpoch the chain the first held delta belongs to, so a
     *     snapshot spanning a takeover can be read at all — a sequence number
     *     is only meaningful beside its epoch
     * @param complete whether every delta since {@link #firstSequence} is here.
     *     ⚠️ False means the CAP dropped something, never that a pass forgot
     *     what it had already collected — forgetting is not losing.
     * @param firstSequence the sequence this snapshot starts at, so a truncated
     *     chain cannot be mistaken for a complete one that begins at 0
     * @param lastEpoch the chain the LAST held delta belongs to
     * @param lastSequence its sequence. ⚠️ **THIS PAIR IS WHAT A PASS HANDS
     *     BACK TO {@link ChainMemory#forgetThrough}**, and without it a pass
     *     could not name what it had just consumed: the deltas themselves carry
     *     no epoch, so a caller holding only {@code firstEpoch} can forget the
     *     predecessor's half of a crossed chain and nothing else — and it then
     *     re-condemns this term's segments on every later pass. ⚠️ Both are 0
     *     on an empty snapshot, which a caller must not forget through: there
     *     is nothing to forget, and {@code deltas.isEmpty()} is the check.
     */
    public record Snapshot(List<CommitDelta> deltas, boolean complete, long firstEpoch,
            long firstSequence, long lastEpoch, long lastSequence) {

        public Snapshot {
            deltas = List.copyOf(Objects.requireNonNull(deltas, "deltas"));
        }
    }

    /**
     * ⚠️ 100,000 deltas. At M7's 250 ms commit interval that is about seven
     * hours of a term with the GC loop stopped, which is long enough that
     * reaching it means something else is already wrong.
     *
     * <p>⚠️ **THE BOUND IS IN DELTAS AND THE REQUIREMENT IS IN BYTES**, and
     * saying so beats a number that looks safe. One delta carries a
     * {@code RunCommit} per stream in its batch, so a fleet delta over a
     * thousand streams is not the same object as a single-stream one — the
     * heap this bounds is therefore "100,000 × whatever the deployment's
     * batches look like" rather than a figure that can be written here. It is
     * the same shape as M8.36, one layer down, and it is a backstop rather than
     * the mechanism: {@link #forgetThrough} is what keeps a healthy chain
     * small.
     */
    public static final int DEFAULT_MAX_DELTAS = 100_000;

    private final int maxDeltas;
    private final Deque<EpochDelta> deltas = new ArrayDeque<>();
    private boolean complete = true;

    /**
     * ⚠️ **ONLY MEANINGFUL WHILE THE DEQUE IS EMPTY.** Where it holds anything,
     * every boundary is read off the deque itself — an earlier version kept
     * these updated in three places as well, and review measured all three
     * removable with the suite green, because {@code snapshot()} recomputed
     * them anyway. What is left is the one case the deque cannot answer: what a
     * chain that has been fully forgotten should report as its start.
     */
    private long emptyEpoch;
    private long emptySequence;

    private long lastEpoch = -1;
    private long lastSequence = -1;

    public ChainMemory(int maxDeltas) {
        if (maxDeltas <= 0) {
            // ⚠️ A CAP OF ZERO IS A CHAIN THAT IS ALWAYS EMPTY AND ALWAYS SAYS
            // IT IS INCOMPLETE -- a GC that silently never collects, which is
            // the failure this whole row exists to make impossible.
            throw new IllegalArgumentException("a chain holds at least one delta: " + maxDeltas);
        }
        this.maxDeltas = maxDeltas;
    }

    /**
     * Takes one delta, from a live commit or from a replay.
     *
     * <p>⚠️ **ONLY {@code CommitDelta}.** A seal and a continue consume a
     * sequence number and carry no runs and no segment key, so a pass has
     * nothing to do with them — and {@code SegmentGc} counts what it examined,
     * so an empty entry would be reported as a delta it looked at.
     *
     * <p>⚠️ **THE REPEAT TEST IS A MONOTONIC-TAIL TEST, NOT SET MEMBERSHIP**,
     * and the difference is worth stating because the weaker property is what
     * this actually has: anything at or before the LAST recorded (epoch,
     * sequence) is dropped, so a replay that re-reads a chain this already
     * holds adds nothing, while a caller that re-ran {@code open()} after
     * committing would append the ancestor's deltas a second time. The callers
     * in this class do neither; a set would cost a hash per delta held, for a
     * case no caller produces.
     */
    synchronized void record(long epoch, CommitDelta delta) {
        Objects.requireNonNull(delta, "delta");
        if (epoch == lastEpoch && delta.sequence() <= lastSequence) {
            // ⚠️ A REPLAY OF WHAT THIS ALREADY HOLDS IS NOT AN APPEND, and the
            // identity is (EPOCH, SEQUENCE) rather than the sequence alone.
            // Offsets cross a takeover and sequence numbers do not, so epoch
            // 2's delta 1 is a different object from epoch 1's -- and a
            // crossing replay reads the ANCESTOR first. Review MEASURED the
            // sequence-only version discarding this term's own deltas as
            // repeats of the predecessor's, while reporting itself complete.
            return;
        }
        deltas.addLast(new EpochDelta(epoch, delta));
        lastEpoch = epoch;
        lastSequence = delta.sequence();
        while (deltas.size() > maxDeltas) {
            deltas.removeFirst();
            complete = false;
        }
    }

    /**
     * Empties the chain before a replay refills it.
     *
     * <p>⚠️ **SO A RECOVERY DESCRIBES THE LOG AND NOT THE PROCESS.** A log that
     * recovers a DIFFERENT epoch than it last held would otherwise carry both
     * terms' deltas in one list, and a pass would condemn the predecessor's
     * segments against this term's retained offsets.
     */
    synchronized void reset() {
        deltas.clear();
        complete = true;
        emptyEpoch = 0;
        emptySequence = 0;
        lastEpoch = -1;
        lastSequence = -1;
    }

    /** What a pass reads, copied so the writer can keep committing. */
    public synchronized Snapshot snapshot() {
        List<CommitDelta> copy = new ArrayList<>(deltas.size());
        for (EpochDelta held : deltas) {
            copy.add(held.delta());
        }
        if (deltas.isEmpty()) {
            return new Snapshot(copy, complete, emptyEpoch, emptySequence, emptyEpoch,
                    emptySequence);
        }
        return new Snapshot(copy, complete, deltas.peekFirst().epoch(),
                deltas.peekFirst().delta().sequence(), deltas.peekLast().epoch(),
                deltas.peekLast().delta().sequence());
    }

    /**
     * Drops everything up to and INCLUDING {@code sequence}.
     *
     * <p>⚠️ **THE BOUNDARY IS (EPOCH, SEQUENCE)**, since a chain that crossed a
     * takeover holds two numbering spaces and a bare sequence would cut the
     * wrong one.
     *
     * <p>⚠️ **INCLUSIVE, AND THE BOUNDARY IS THE PASS'S.** What a pass has
     * already collected cannot be collected again, so holding it is dead weight
     * — and this is the mechanism that keeps the chain small in a healthy
     * deployment, rather than the cap.
     *
     * <p>⚠️ **IT DOES NOT MAKE THE CHAIN INCOMPLETE.** Forgetting what was
     * consumed is not losing it, and a snapshot afterwards says where it now
     * starts.
     */
    public synchronized void forgetThrough(long epoch, long sequence) {
        while (!deltas.isEmpty() && atOrBefore(deltas.peekFirst(), epoch, sequence)) {
            deltas.removeFirst();
        }
        if (deltas.isEmpty()) {
            emptyEpoch = epoch;
            emptySequence = sequence + 1;
        }
    }

    /**
     * ⚠️ **(EPOCH, SEQUENCE) IN THAT ORDER**, because a chain held across a
     * takeover holds two numbering spaces at once: every delta of an older
     * epoch is before every delta of a newer one, whatever the sequences say.
     */
    private static boolean atOrBefore(EpochDelta held, long epoch, long sequence) {
        return held.epoch() < epoch
                || (held.epoch() == epoch && held.delta().sequence() <= sequence);
    }
}
