// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.format.Checkpoint;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * Reading a chain's newest checkpoint from outside this package (M8.6,
 * ADR-0056).
 *
 * <p>⚠️ **A STAT AND A GET, AND THAT IS ALL IT MAY EVER COST.** The one caller
 * outside this module is the serving pod's retained-floor cache, which ADR-0056
 * budgets at exactly these two requests per refresh: the epoch it names is the
 * one the pod last committed under, known without a request. ⚠️ An earlier
 * version of this paragraph budgeted a third, a lease read, which nothing
 * makes. A version of this that walked back through ancestor epochs
 * looking for a checkpoint would be an unbounded read on the subscription
 * path, which is exactly what the checkpoint exists to remove.
 *
 * <p>⚠️ **EMPTY MEANS "NOT YET", NOT "NOTHING RETAINED".** A term that has not
 * checkpointed yet has no pointer; the caller treats the floor as unknown, and
 * unknown refuses nothing.
 */
public final class Checkpoints {

    private Checkpoints() {
    }

    /**
     * Where {@code epoch}'s newest checkpoint lives.
     *
     * <p>⚠️ **SO NOTHING OUTSIDE THIS MODULE SPELLS THE KEY GRAMMAR.** A caller
     * that needs to put a checkpoint where {@link #newest} will look -- a test
     * of the floor's wiring, today -- would otherwise copy the layout, and the
     * copy is what goes stale.
     */
    public static String newestKey(String prefix, long epoch) {
        Objects.requireNonNull(prefix, "prefix");
        return new LogKeys(prefix, epoch).latestCheckpointKey();
    }

    /** The newest checkpoint of {@code epoch}'s chain, or empty if it has none yet. */
    public static Optional<Checkpoint> newest(BinStore store, String prefix, long epoch)
            throws IOException {
        Objects.requireNonNull(store, "store");
        Objects.requireNonNull(prefix, "prefix");
        return CheckpointCursor.newest(store, prefix, epoch);
    }
}
