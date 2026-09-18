// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.format.CommitDelta;
import java.util.Objects;

/**
 * One delta and the chain it belongs to (M8.3).
 *
 * <p>⚠️ **A SEQUENCE ALONE IS NOT AN IDENTITY.** Offsets cross a takeover and
 * sequence numbers do not: every chain numbers from its own start, so epoch 2's
 * delta 1 and epoch 1's delta 1 are different objects with the same number. A
 * crossing replay reads the ANCESTOR first, so anything that deduplicated or
 * ordered on the sequence alone would discard this term's own deltas as repeats
 * of the predecessor's — review MEASURED that, with a recovered chain holding
 * only the predecessor's segments while reporting itself COMPLETE, which is the
 * one state {@link ChainMemory} promises cannot happen.
 */
public record EpochDelta(long epoch, CommitDelta delta) {

    public EpochDelta {
        Objects.requireNonNull(delta, "delta");
        if (epoch < 0) {
            throw new IllegalArgumentException("epoch is never negative: " + epoch);
        }
    }
}
