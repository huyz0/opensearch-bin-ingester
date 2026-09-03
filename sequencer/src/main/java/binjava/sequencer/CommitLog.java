// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.ListPage;
import binjava.binstore.ObjectStat;
import binjava.format.CommitDelta;
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
                CommitDelta delta = readDelta(stat.key());
                apply(delta);
            }
            if (page.nextStartAfter().isEmpty()) {
                return;
            }
            startAfter = page.nextStartAfter().get();
        }
    }

    private CommitDelta readDelta(String key) throws IOException {
        try (InputStream in = store.get(key)) {
            return CommitDelta.decode(in.readAllBytes());
        }
    }

    private void apply(CommitDelta delta) {
        for (RunCommit run : delta.runs()) {
            nextOffsets.merge(run.key(), run.lastOffset() + 1, Math::max);
        }
        nextSequence = Math.max(nextSequence, delta.sequence() + 1);
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
            apply(readDelta(keyFor(nextSequence)));
        }
    }
}
