// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.GovernorScope;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentReader;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.InboxDrain;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * The inbox drain's fast write at the leader (M13.34m; ADR-0081 §8): a
 * deferred intent's runs of streams now fast, each run's records read back
 * from the intent's uploaded segment and appended through this pod's fast
 * path, as a writer's redirected run is re-sent (M13.34l).
 *
 * <p>⚠️ ONE GET PER SUCH INTENT, DECLARED RECOVERY as the drain's LIST is
 * (ADR-0075): paid only for an intent deferred across a switch to fast mode,
 * never per record.
 */
public final class FastDrainRedirect implements InboxDrain.FastRedirect {

    private final BinStore store;
    private final Supplier<FastPath> path;

    /**
     * @param path this pod's fast path when the drain runs: set by the
     *     composition root after the term starts
     */
    public FastDrainRedirect(BinStore store, Supplier<FastPath> path) {
        this.store = Objects.requireNonNull(store, "store");
        this.path = Objects.requireNonNull(path, "path");
    }

    /**
     * Through {@code ingest}'s fast path, none while it is unbuilt: a term's
     * first drain runs as the node starts, its catalog empty -- nothing fast.
     */
    public static FastDrainRedirect of(BinStore store, Supplier<DefaultIngest> ingest) {
        Objects.requireNonNull(ingest, "ingest");
        return new FastDrainRedirect(store, () -> {
            DefaultIngest built = ingest.get();
            return built == null ? FastPath.NONE : built.fastPath();
        });
    }

    @Override
    public void write(CommitRequest intent, Set<RunKey> runs) throws IOException {
        byte[] segment = GovernorScope.recovery(() -> {
            try (var in = store.get(intent.segmentKey())) {
                return in.readAllBytes();
            }
        });
        SegmentReader reader = SegmentReader.open(segment);
        for (RunKey run : runs) {
            List<SegmentRecord> records = reader.read(reader.find(run).orElseThrow(
                    () -> new IOException("the intent's segment " + intent.segmentKey()
                            + " holds no run of " + run)));
            if (path.get().append(run, records).isEmpty()) {
                throw new IOException("stream " + run + " of an inbox intent was answered"
                        + " NOT_FAST by the fast path: it stays in the inbox");
            }
        }
    }
}
