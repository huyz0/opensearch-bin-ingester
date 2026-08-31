// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

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
    private final Map<RunKey, Long> nextOffsets = new HashMap<>();
    private long nextSequence;

    public CommitLog(BinStore store, String prefix) {
        this.store = Objects.requireNonNull(store, "store");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
    }

    /**
     * ⚠️ slot 0 / epoch 0 in M1. Their SLOTS are in the grammar from the first
     * object so that M4's leases and epochs are not a key-grammar change.
     */
    String keyFor(long sequence) {
        // ⚠️ Locale.ROOT: an object key is a wire value and must not depend on
        // the process's locale.
        return String.format(java.util.Locale.ROOT, "%s/ctl/log/0/0/%016x.delta",
                prefix, sequence);
    }

    String logPrefix() {
        return String.format(java.util.Locale.ROOT, "%s/ctl/log/0/0/", prefix);
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
