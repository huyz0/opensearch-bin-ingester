// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.format.ChainEntry;
import binjava.format.CommitDelta;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

/**
 * Reads ONE delta out of any chain, by {@code (epoch, sequence)}.
 *
 * <p>⚠️ Split out of {@link CommitLog} when M4.10d pushed that file past the
 * 500-line limit — code-structure.md rule 1, split rather than raise. The seam
 * is real rather than convenient: answering a replay reads a single addressed
 * object, possibly from a PREDECESSOR's chain, and needs none of the offset
 * state, sequencing or sealing that {@code CommitLog} exists for. {@link
 * CheckpointCursor} was split off the same way.
 *
 * <p>⚠️ ONE GET, AND NO LIST. The key is computed, so this costs one request
 * whatever the chain holds and however long its term ran.
 */
final class DeltaReader {

    private DeltaReader() {
    }

    /**
     * The delta at {@code sequence} of {@code epoch}, or empty if that slot
     * holds something else.
     *
     * <p>⚠️ A slot holding a {@code Seal} or a {@code Continue} returns empty
     * rather than throwing: those are legitimate chain contents, not
     * corruption, and a caller asking for a delta wants to know it is not one.
     */
    static Optional<CommitDelta> at(BinStore store, String prefix, long epoch, long sequence)
            throws IOException {
        String key = new LogKeys(prefix, epoch).keyFor(sequence);
        try (InputStream in = store.get(key)) {
            ChainEntry entry = ChainEntry.decode(in.readAllBytes());
            return entry instanceof CommitDelta delta ? Optional.of(delta) : Optional.empty();
        }
    }
}
