// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.format.Checkpoint;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Chain GC for the term this node holds, from memory alone (M8.39, FR-9,
 * M7.24).
 *
 * <p>⚠️ **IT ISSUES NO GET AND NO LIST.** {@link ChainGc} takes the newest
 * checkpoint and every checkpoint of the chain, and every other source of
 * those is a read -- a request per pass on an idle leader, which M8's
 * criterion 4 forbids. Here both come from what this term's
 * {@link CheckpointWriter} wrote, and the deltas from what its
 * {@link ChainMemory} forgot once their segments were gone. With nothing to
 * judge it returns before touching the store.
 *
 * <p>⚠️ **WHAT IT DOES NOT COLLECT**: a predecessor's checkpoints, which this
 * term never wrote and does not know without a LIST. They stay in the bucket.
 */
public final class ChainCollector {

    /** How many of the newest checkpoints are kept; {@link ChainGc} refuses fewer than 2. */
    public static final int KEEP_CHECKPOINTS = 2;

    private final LocalSequencer term;
    private final String prefix;

    public ChainCollector(LocalSequencer term, String prefix) {
        this.term = Objects.requireNonNull(term, "term");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
    }

    /**
     * One pass, deleting through {@code fenced}.
     *
     * <p>⚠️ **{@code fenced} IS THE GC LEASE's STORE**, like every other
     * deletion the retention loop makes: a node that lost the lease must not
     * delete.
     */
    public ChainGc.Result collect(BinStore fenced, int deleteBatch) {
        Objects.requireNonNull(fenced, "fenced");
        CheckpointWriter writer = term.checkpoints;
        if (writer == null) {
            return ChainGc.Result.none();
        }
        Optional<Checkpoint> newest = writer.newestWritten();
        List<ChainGc.DeltaAt> awaiting = term.chain().awaitingChainGc();
        List<ChainGc.CheckpointAt> checkpoints = writer.written();
        if (newest.isEmpty()
                || (awaiting.isEmpty() && checkpoints.size() <= KEEP_CHECKPOINTS)) {
            return ChainGc.Result.none();
        }
        ChainGc.Result result = new ChainGc(fenced, prefix, deleteBatch, KEEP_CHECKPOINTS)
                .collect(newest.get(), new ChainGc.CheckpointAt(term.epoch(),
                        newest.get().sequence()), awaiting, Set.of(), checkpoints);
        term.chain().chainCollected(result.collected());
        writer.collected(result.collectedCheckpoints());
        return result;
    }
}
