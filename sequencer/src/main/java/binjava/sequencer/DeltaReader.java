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
 * <p>⚠️ NO LIST, EVER. The key is computed, so a read costs the same whatever
 * the chain holds and however long its term ran: {@link #at} is one GET, and
 * {@link #ifWritten} is a stat plus that GET, because it must tell an unwritten
 * slot from an unreachable store and {@code BinStore.get} reports both the same
 * way.
 */
final class DeltaReader {

    private DeltaReader() {
    }

    /**
     * The delta at {@code sequence} of {@code epoch}, or empty if that slot
     * holds no delta -- including because nothing was ever written there
     * (M5.23).
     *
     * <p>⚠️ EMPTY MEANS "NOT A DELTA", NOT "ABSENT", and the two are folded
     * deliberately rather than by omission. A slot holding a {@code Seal} or a
     * {@code Continue} answers empty here exactly as an unwritten one does, and
     * the one caller that reads empty as "my append never landed" is safe under
     * the lease: a chain has one writer per epoch, so the only entry that could
     * occupy the slot this writer aimed at is that writer's own append or a
     * successor's SEAL -- and a seal there proves the append did NOT land,
     * which is the same conclusion. What follows either way is a retry, which
     * meets the seal through the fencing path that already exists.
     *
     * <p>⚠️ THE STAT IS NOT A DUPLICATE OF THE GET, and removing it turns one
     * failure mode into another. {@code BinStore.get} reports "no such key" and
     * "store unreachable" as the same {@link IOException}, so a caller
     * reconciling an ambiguous append could not tell an append that never
     * landed from a store that is still down -- and refusing on both wedges the
     * writer permanently: the slot stays empty precisely because nothing may
     * commit until the slot is read.
     *
     * <p>⚠️ TWO REQUESTS PER AMBIGUOUS APPEND, and neither scales with records,
     * shards, partitions or indices -- cost.md's invariant, not its numbered
     * R1, which is the bundling rule. The rate is a FAULT rate and is nil on a
     * healthy store. No LIST (R2): the key is computed.
     */
    static Optional<CommitDelta> ifWritten(BinStore store, String prefix, long epoch,
            long sequence) throws IOException {
        if (store.stat(new LogKeys(prefix, epoch).keyFor(sequence)).isEmpty()) {
            return Optional.empty();
        }
        return at(store, prefix, epoch, sequence);
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
