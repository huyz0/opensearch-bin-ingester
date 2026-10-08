// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.CommitDelta;
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
     * @param fromFloor whether this chain holds every surviving delta back to
     *     the retention floor (M8.42) -- the one condition under which a keep
     *     list taken from it is safe for hours before this term began
     */
    public record Snapshot(List<CommitDelta> deltas, boolean complete, long firstEpoch,
            long firstSequence, long lastEpoch, long lastSequence, boolean fromFloor,
            List<SequencedVoid> voids, boolean voidsComplete) {

        public Snapshot {
            deltas = List.copyOf(Objects.requireNonNull(deltas, "deltas"));
            voids = List.copyOf(Objects.requireNonNull(voids, "voids"));
        }

        /** A snapshot of deltas alone, as every snapshot was before voids (M13.25h). */
        public Snapshot(List<CommitDelta> deltas, boolean complete, long firstEpoch,
                long firstSequence, long lastEpoch, long lastSequence, boolean fromFloor) {
            this(deltas, complete, firstEpoch, firstSequence, lastEpoch, lastSequence, fromFloor,
                    List.of(), true);
        }
    }

    /**
     * A recovery entry's void, with the slot that committed it (M13.25h,
     * ADR-0082 §5).
     *
     * <p>⚠️ **BESIDE THE DELTAS, NOT AMONG THEM**: retention, GC and the orphan
     * keep list read only {@link Snapshot#deltas}, and a void holds no
     * segment, so none of them changes. Catch-up is to read both, a stream's
     * runs and voids merged by offset (M13.25i).
     *
     * <p>⚠️ **{@link Snapshot#voidsComplete}, NOT {@link Snapshot#complete}**,
     * says whether a void was evicted (M13.25h review P1): the deltas'
     * flag stops the orphan sweep and catch-up, and an evicted void must
     * stop neither while every delta is still held.
     */
    public record SequencedVoid(long epoch, long sequence,
            io.github.huyz0.os.biningester.format.Recovery.VoidRange range) {
        public SequencedVoid {
            Objects.requireNonNull(range, "range");
        }
    }

    private final Deque<SequencedVoid> voids = new ArrayDeque<>();
    private long lastVoidEpoch = -1;
    private long lastVoidSequence = -1;
    private boolean voidsComplete = true;

    /**
     * Takes a recovery entry's voids, from a live apply or a replay, by the
     * same monotonic-tail rule as {@link #record}: a slot at or before the last
     * one taken IN THE SAME EPOCH is a replay of what this holds -- a
     * crossing reads the ancestor's slots first, and epoch 2's slot 3 is not
     * a repeat of epoch 1's slot 5. ⚠️ BOUNDED BY {@code maxDeltas} as the
     * deltas are; a void dropped clears {@link Snapshot#voidsComplete} only.
     */
    synchronized void recordVoids(long epoch, long sequence,
            List<io.github.huyz0.os.biningester.format.Recovery.VoidRange> taken) {
        Objects.requireNonNull(taken, "taken");
        if (epoch == lastVoidEpoch && sequence <= lastVoidSequence) {
            return;
        }
        for (var range : taken) {
            voids.addLast(new SequencedVoid(epoch, sequence, range));
        }
        lastVoidEpoch = epoch;
        lastVoidSequence = sequence;
        while (voids.size() > maxDeltas) {
            voids.removeFirst();
            voidsComplete = false;
        }
    }

    private void forgetVoidsThrough(long epoch, long sequence) {
        while (!voids.isEmpty() && (voids.peekFirst().epoch() < epoch
                || (voids.peekFirst().epoch() == epoch
                        && voids.peekFirst().sequence() <= sequence))) {
            voids.removeFirst();
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

    /** Whether a backfill has reached the retention floor (M8.42); see {@link #backfill}. */
    private boolean fromFloor;

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

    /**
     * Deltas forgotten because their segments are gone, whose OWN objects
     * chain GC has not yet collected (M8.39).
     *
     * <p>⚠️ **SEPARATE FROM THE CHAIN, AND THAT IS THE COST ARGUMENT.** Kept in
     * the chain, a delta chain GC must keep -- pinned by a pod's pointer, or
     * newer than the newest checkpoint -- would be handed to the segment pass
     * on every tick, whose DELETEs for segments already gone would then run on
     * an idle leader for ever. Here they cost nothing until chain GC runs.
     * ⚠️ BOUNDED BY {@code maxDeltas} like the chain: past it the oldest is
     * dropped, and its object stays in the bucket, which is storage and the
     * safe direction.
     */
    private final Deque<ChainGc.DeltaAt> awaitingChainGc = new ArrayDeque<>();

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
        voids.clear();
        lastVoidEpoch = -1;
        lastVoidSequence = -1;
        voidsComplete = true;
        complete = true;
        fromFloor = false;
        emptyEpoch = 0;
        emptySequence = 0;
        lastEpoch = -1;
        lastSequence = -1;
    }

    /**
     * Where an EMPTY chain begins, when a replay began at a checkpoint (M8.38).
     *
     * <p>⚠️ **ONLY WHILE EMPTY**, like every use of the empty boundary: a chain
     * holding deltas reports its boundary off the deque. A checkpoint that
     * collapsed the whole chain leaves nothing to read, and (0, 0) would say
     * nothing had ever been collected.
     */
    synchronized void startsAt(long epoch, long sequence) {
        if (deltas.isEmpty()) {
            emptyEpoch = epoch;
            emptySequence = sequence;
        }
    }

    /** What a pass reads, copied so the writer can keep committing. */
    public synchronized Snapshot snapshot() {
        List<CommitDelta> copy = new ArrayList<>(deltas.size());
        for (EpochDelta held : deltas) {
            copy.add(held.delta());
        }
        List<SequencedVoid> heldVoids = List.copyOf(voids);
        if (deltas.isEmpty()) {
            return new Snapshot(copy, complete, emptyEpoch, emptySequence, emptyEpoch,
                    emptySequence, fromFloor && complete, heldVoids, voidsComplete);
        }
        return new Snapshot(copy, complete, deltas.peekFirst().epoch(),
                deltas.peekFirst().delta().sequence(), deltas.peekLast().epoch(),
                deltas.peekLast().delta().sequence(), fromFloor && complete, heldVoids,
                voidsComplete);
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
        forgetVoidsThrough(epoch, sequence);
        if (deltas.isEmpty()) {
            emptyEpoch = epoch;
            emptySequence = sequence + 1;
        }
    }

    /**
     * Drops the longest PREFIX whose every segment has been deleted, and
     * nothing after it (M8.5).
     *
     * <p>⚠️ **A PREFIX, AND ONLY OF WHAT WAS ACTUALLY DELETED.** The tempting
     * call is {@link #forgetThrough} with the snapshot's LAST position, "because
     * the pass has seen all of it". That forgets every delta whose segments the
     * pass KEPT -- the young ones, and the ones a slow consumer is still behind
     * -- and they are then never judged again: a storage leak on the retention
     * side, and on the orphan side something worse. The orphan sweep's keep
     * list is this chain's segment keys and it FAILS OPEN, so a committed
     * segment whose delta was forgotten here would be LISTED, found in no
     * delta, and deleted as an orphan. Forgetting only what is gone is what
     * keeps the keep list equal to "every committed segment still in the
     * bucket".
     *
     * <p>⚠️ **STOPS AT THE FIRST DELTA WITH A SURVIVING SEGMENT**, even if
     * later ones are fully deleted: the chain is a deque and its identity is
     * its order, so it cannot hold holes. A later pass that collects the
     * blocker forgets the whole run at once; the only cost is re-issuing
     * DELETEs for keys already gone, which a store answers as success and a
     * batch carries a thousand of.
     *
     * @return how many deltas were dropped
     */
    public synchronized int forgetCollected(java.util.Set<String> deletedKeys) {
        Objects.requireNonNull(deletedKeys, "deletedKeys");
        int dropped = 0;
        EpochDelta last = null;
        while (!deltas.isEmpty() && everySegmentIn(deltas.peekFirst(), deletedKeys)) {
            last = deltas.removeFirst();
            dropped++;
            awaitingChainGc.addLast(new ChainGc.DeltaAt(last.epoch(), last.delta().sequence(),
                    last.delta()));
            if (awaitingChainGc.size() > maxDeltas) {
                awaitingChainGc.removeFirst();
            }
        }
        if (last != null) {
            // ⚠️ A VOID BEFORE A COLLECTED DELTA HAS BEEN CONSUMED WITH IT (M13.25h).
            forgetVoidsThrough(last.epoch(), last.delta().sequence());
        }
        if (last != null && deltas.isEmpty()) {
            // ⚠️ THE SAME BOUNDARY `forgetThrough` WOULD HAVE LEFT, so a
            // snapshot of a fully-collected chain says where it resumes rather
            // than falling back to (0, 0) -- which is M8.38's defect arriving
            // through a second door.
            emptyEpoch = last.epoch();
            emptySequence = last.delta().sequence() + 1;
        }
        return dropped;
    }

    /**
     * Prepends what a takeover read BELOW its replay, and marks the chain as
     * reaching the retention floor (M8.42).
     *
     * <p>⚠️ **ONLY WHAT IS OLDER THAN THE FIRST HELD DELTA IS TAKEN**, so a
     * backfill racing a commit cannot duplicate or reorder anything; the
     * caller's boundary and this one agree by construction.
     *
     * <p>⚠️ **{@link Snapshot#fromFloor} IS WHAT LETS THE SWEEP ENTER HOURS
     * BEFORE THIS TERM**, and it is only true while nothing has been dropped:
     * the cap evicting a backfilled delta takes the claim with it.
     */
    public synchronized void backfill(List<ChainGc.DeltaAt> older) {
        Objects.requireNonNull(older, "older");
        EpochDelta first = deltas.peekFirst();
        List<ChainGc.DeltaAt> taken = new ArrayList<>();
        for (ChainGc.DeltaAt at : older) {
            if (first == null || at.epoch() < first.epoch()
                    || (at.epoch() == first.epoch()
                            && at.sequence() < first.delta().sequence())) {
                taken.add(at);
            }
        }
        for (int i = taken.size() - 1; i >= 0; i--) {
            deltas.addFirst(new EpochDelta(taken.get(i).epoch(), taken.get(i).delta()));
        }
        while (deltas.size() > maxDeltas) {
            deltas.removeFirst();
            complete = false;
        }
        fromFloor = true;
    }

    /** What chain GC may judge: deltas forgotten here whose objects are still there. */
    public synchronized List<ChainGc.DeltaAt> awaitingChainGc() {
        return List.copyOf(awaitingChainGc);
    }

    /** Forgets what chain GC collected. */
    public synchronized void chainCollected(java.util.Collection<ChainGc.DeltaAt> gone) {
        awaitingChainGc.removeAll(gone);
    }

    private static boolean everySegmentIn(EpochDelta held, java.util.Set<String> deletedKeys) {
        for (io.github.huyz0.os.biningester.format.SegmentCommit segment : held.delta().segments()) {
            if (!deletedKeys.contains(segment.segmentKey())) {
                return false;
            }
        }
        return true;
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
