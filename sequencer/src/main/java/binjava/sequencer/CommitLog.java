// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.ListPage;
import binjava.binstore.ObjectStat;
import binjava.format.ChainEntry;
import binjava.format.CommitDelta;
import binjava.format.Continue;
import binjava.format.Seal;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The commit log v0 (M1.10): a chain of sequentially numbered deltas, each
 * written with {@code putIfAbsent}.
 *
 * <p>⚠️ THE STORE IS THE ONLY COORDINATOR (ADR-0002). Two writers may both try
 * slot N; the store decides, the loser re-reads and retries at N+1. Invariant I1
 * — no seq is ever written twice — holds with no lock, no lease and no consensus
 * round, and that is the entire reason this system can be cheap.
 *
 * <p>⚠️ OFFSETS ARE ASSIGNED HERE, NOT IN THE SEGMENT (ADR-0001). The segment is
 * already durable when this runs, so an abandoned PUT leaves no hole: it is
 * simply never committed and is garbage-collected later. Pre-assigning offsets
 * before the PUT would create holes that stall every consumer of the partition.
 */
public final class CommitLog {

    private final BinStore store;
    private final String prefix;
    private final long epoch;
    private final Map<RunKey, Long> nextOffsets = new HashMap<>();
    private long nextSequence;
    /**
     * The SEAL this chain ended at, if recovery reached one.
     *
     * <p>⚠️ NOT reset by {@link #recover()}, unlike the two fields above, and
     * deliberately: a chain is sealed permanently, so this only ever goes from
     * null to set. A reset would be a line no test could ever falsify.
     */
    private Seal sealedAt;

    /**
     * The chain for epoch 0 — what M1 wrote, and what every call site that has
     * no lease yet still writes.
     *
     * <p>⚠️ THIS CHAIN IS A FORK, and the wiring commit must END it rather
     * than leave it running. Once a leader writes at epoch 1, a reader of that
     * epoch never lists this prefix — so records committed here are acked and
     * never become visible, and the leader re-issues offsets this chain already
     * assigned. Nor can this chain be SEALED: the seal depends on a losing
     * writer treating its loss as proof it is fenced, and {@code commit} does
     * the opposite by construction, folding the winner's offsets in and
     * retrying at the next sequence forever. There is no leader here to fence.
     * ⚠️ So M4.5/M4.6 must REMOVE the unleased production commit path, not
     * merely pass an epoch to it — and {@code CONTINUE} may not use
     * {@code prevEpoch=0} as a "no previous chain" sentinel, because 0 now
     * names a live one.
     *
     * <p>⚠️ EPOCH 0 IS RESERVED FOR EXACTLY THIS — "no lease" — and
     * {@code LeaseManager} starts its FIRST term at 1 so that no leased chain
     * can ever collide with this one (M4.4b). That reservation is what keeps
     * I3 true for the first term: were a first leader also at epoch 0, its
     * chain would be byte-identical to what every no-lease caller here writes,
     * {@code putIfAbsent} would still buy I1, but "readers of the new epoch
     * never look there" would be void — it would not be a different epoch.
     */
    public CommitLog(BinStore store, String prefix) {
        this(store, prefix, 0);
    }

    /**
     * The chain for one term of leadership.
     *
     * <p>⚠️ THE EPOCH IS IN THE PATH, and that is what makes fencing not depend
     * on catching a write in time (ADR-0002). A fenced leader's in-flight PUT
     * lands under its OWN epoch, where readers of the new epoch never look — so
     * it does not have to be stopped before it writes, only before anyone
     * believes it.
     */
    public CommitLog(BinStore store, String prefix, long epoch) {
        this.store = Objects.requireNonNull(store, "store");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        if (epoch < 0) {
            // ⚠️ Epochs count up from 0. A negative one could only come from a
            // corrupt lease, and it would order BEFORE every real epoch.
            throw new IllegalArgumentException("epoch is never negative: " + epoch);
        }
        this.epoch = epoch;
    }

    /**
     * ⚠️ Slot 0 because ADR-0007 fixes S = 1 — but the slot stays IN THE KEY so
     * raising it is configuration, not a key-grammar change. M1 wrote slot 0 /
     * epoch 0 for exactly this reason, and M4.4 is that promise being kept:
     * the dimension was always there, and this only fills it in.
     *
     * <p>⚠️ The epoch is zero-padded to 16 hex digits, like the sequence beside
     * it, so LEXICOGRAPHIC ORDER IS NUMERIC ORDER. That matters because
     * ADR-0022 removed {@code lastModifiedMillis}, leaving key order as the
     * only ordering a reader has; unpadded, epoch 10 would sort before epoch 9.
     */
    String keyFor(long sequence) {
        // ⚠️ Locale.ROOT: an object key is a wire value and must not depend on
        // the process's locale.
        return String.format(java.util.Locale.ROOT, "%s%016x.delta", logPrefix(), sequence);
    }

    String logPrefix() {
        return String.format(java.util.Locale.ROOT, "%s/ctl/log/0/%016x/", prefix, epoch);
    }

    /** Which term of leadership this chain belongs to. */
    public long epoch() {
        return epoch;
    }

    /**
     * Rebuilds the offset state by replaying the log.
     *
     * <p>⚠️ A LIST, and R2 permits it because this is RECOVERY. It runs once at
     * startup, never on a hot path.
     */
    public void recover() throws IOException {
        nextOffsets.clear();
        nextSequence = 0;
        String startAfter = null;
        while (true) {
            ListPage page = store.list(logPrefix(), startAfter, 1000);
            for (ObjectStat stat : page.objects()) {
                ChainEntry entry = readEntry(stat.key());
                apply(entry);
                if (entry instanceof Seal seal) {
                    // ⚠️ A BARRIER, not an entry to count and move past. M4.5
                    // closed this in `commit` for a writer that LOSES a slot to
                    // a seal; it could not close it for one that never loses
                    // one. Recovery used to advance `nextSequence` PAST the
                    // seal, so the next commit picked a FREE slot, won it
                    // outright, and never reached that check -- I5 with no lost
                    // race anywhere in it. Measured before this fix: the commit
                    // succeeded, and a recovery over [delta, SEAL, delta]
                    // reported offset 9 where the sealed prefix ended at 3,
                    // which is I3.
                    // ⚠️ Returning here also STOPS the LIST. Everything beyond
                    // a seal is a discarded suffix, so paging on would spend
                    // requests reading bytes that must not be applied.
                    sealedAt = seal;
                    return;
                }
            }
            if (page.nextStartAfter().isEmpty()) {
                return;
            }
            startAfter = page.nextStartAfter().get();
        }
    }

    private ChainEntry readEntry(String key) throws IOException {
        try (InputStream in = store.get(key)) {
            return ChainEntry.decode(in.readAllBytes());
        }
    }

    /**
     * Folds one chain entry into this reader's state.
     *
     * <p>⚠️ EXHAUSTIVE BY COMPILATION (M4.5). {@link ChainEntry} is sealed, so
     * a fourth shape cannot be added without every switch like this one failing
     * to compile — which is the point of the sealed interface rather than a
     * kind byte and a default branch that silently ignores what it does not
     * know.
     *
     * <p>⚠️ A {@code SEAL} and a {@code CONTINUE} carry no runs, so they move
     * no offsets — but they DO consume a sequence number, deliberately, and
     * {@code nextSequence} must advance past them or the next commit would race
     * for a slot that is already taken and spin.
     */
    private void apply(ChainEntry entry) {
        switch (entry) {
            case CommitDelta delta -> {
                for (RunCommit run : delta.runs()) {
                    nextOffsets.merge(run.key(), run.lastOffset() + 1, Math::max);
                }
            }
            case Seal ignored -> { }
            case Continue ignored -> { }
        }
        nextSequence = Math.max(nextSequence, entry.sequence() + 1);
    }

    /** The offset the next record of this stream will get. */
    public long nextOffset(RunKey key) {
        return nextOffsets.getOrDefault(key, 0L);
    }

    public long nextSequence() {
        return nextSequence;
    }

    /**
     * Commits one segment, assigning offsets to each of its runs.
     *
     * <p>⚠️ RETRIES ON A LOST RACE rather than failing. Losing slot N is normal:
     * another writer got there first, so this one re-reads what landed, folds it
     * into its own offset state and tries N+1. Treating a lost race as an error
     * would turn ordinary contention into an outage.
     *
     * @return the delta as committed, with the offsets that were actually assigned
     */
    public CommitDelta commit(String segmentKey, Map<RunKey, Integer> recordCounts)
            throws IOException {
        if (recordCounts.isEmpty()) {
            throw new IllegalArgumentException("a commit with no runs commits nothing");
        }
        if (sealedAt != null) {
            // ⚠️ The half `commit`'s lost-race branch below cannot reach. That
            // branch needs a race to lose; this writer recovered a sealed chain
            // and would win an empty slot beyond the barrier without ever
            // racing anyone.
            throw fenced(sealedAt);
        }
        while (true) {
            List<RunCommit> runs = new ArrayList<>(recordCounts.size());
            // ⚠️ Sorted, so a delta's runs are in the same order the segment's
            // directory uses and a replay is deterministic.
            recordCounts.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(e -> runs.add(
                            new RunCommit(e.getKey(), e.getValue(), nextOffset(e.getKey()))));
            CommitDelta delta = new CommitDelta(nextSequence, segmentKey, runs);
            Optional<binjava.binstore.Version> written = store.putIfAbsent(keyFor(nextSequence),
                    new Body(delta.encode().length, () -> new ByteArrayInputStream(delta.encode())));
            if (written.isPresent()) {
                apply(delta);
                return delta;
            }
            // ⚠️ Somebody else took this slot. Read what they wrote, fold their
            // offsets in, and try the next one -- which is why offsets must be
            // recomputed inside the loop rather than before it.
            ChainEntry taken = readEntry(keyFor(nextSequence));
            if (taken instanceof Seal seal) {
                // ⚠️ FAIL CLOSED. Losing a slot to a SEAL is not ordinary
                // contention -- it is proof this writer is fenced. Folding the
                // seal in and retrying at the next slot would write PAST the
                // barrier and acknowledge it, which is invariant I5 ("no
                // acknowledged commit exists beyond a SEAL in its own chain"),
                // and recovery would then apply a discarded suffix, which is
                // I3.
                // ⚠️ Teaching this reader to UNDERSTAND a seal is what made
                // that reachable: before M4.5 an unknown version threw here, so
                // the writer failed closed by accident. A read side that ships
                // first must be no less safe than the one it replaces.
                // ⚠️ Stopping is only the FENCED branch. A new leader holding a
                // valid lease must redrive to N+2 instead of stopping, or it
                // surrenders sequencing cluster-wide -- that split needs the
                // lease, so it is M4.6's. This refusal is the safe half, and
                // the half that must not wait.
                throw fenced(seal);
            }
            apply(taken);
        }
    }

    /**
     * ⚠️ Stopping is only the FENCED branch. A new leader holding a valid lease
     * must redrive to N+2 rather than stop, or it surrenders sequencing
     * cluster-wide; that split needs the lease and is not this task's.
     */
    private static IOException fenced(Seal seal) {
        return new IOException("chain sealed at seq " + seal.sequence()
                + ", continued at epoch " + seal.continuedAt()
                + "; this writer is fenced and must stop");
    }
}
