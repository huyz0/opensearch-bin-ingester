// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import java.io.IOException;
import java.util.Optional;

/**
 * Reads one delta by {@code (epoch, sequence)} for a caller outside this
 * package: an AZ's relay, turning a hint into the delta it names (M10.20a,
 * ADR-0075 decision 4).
 *
 * <p>⚠️ **ONE GET ON A HEALTHY STORE; A STAT ONLY WHEN THAT GET FAILS.**
 * {@code BinStore.get} reports an absent key and an unreachable store the same
 * way, and the relay must tell a stale or forged hint (advance nothing) from a
 * store failure (retry). Asking the stat first -- {@code DeltaReader.ifWritten}'s
 * order, right for its fault-path caller -- would double the relay's steady
 * request rate. Both requests are per (delta, remote AZ); neither scales with
 * records, shards, partitions or indices.
 */
public final class DeltaReads {

    private DeltaReads() {
    }

    /**
     * @return the delta, or empty when that slot holds none -- never written,
     *     or a seal or continue
     * @throws IOException when the store could not answer
     */
    public static Optional<CommitDelta> read(BinStore store, String prefix, long epoch,
            long sequence) throws IOException {
        try {
            return DeltaReader.at(store, prefix, epoch, sequence);
        } catch (IOException failed) {
            if (store.stat(new LogKeys(prefix, epoch).keyFor(sequence)).isEmpty()) {
                return Optional.empty();
            }
            throw failed;
        }
    }
}
