// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import java.util.Objects;
import java.util.UUID;

/**
 * Which stream a run belongs to: one index, one partition.
 *
 * <p>⚠️ Runs are sorted by {@code (indexId, partitionId)} so that ONE consumer's
 * runs are contiguous in the segment — its fetch is then one coalesced range
 * rather than hundreds of range GETs. That ordering is cost rules R4 and R5, not
 * a tidiness preference.
 */
public record RunKey(UUID indexId, int partitionId) implements Comparable<RunKey> {

    public RunKey {
        Objects.requireNonNull(indexId, "indexId");
        if (partitionId < 0) {
            throw new IllegalArgumentException("partitionId is never negative: " + partitionId);
        }
    }

    @Override
    public int compareTo(RunKey other) {
        int byIndex = indexId.compareTo(other.indexId);
        return byIndex != 0 ? byIndex : Integer.compare(partitionId, other.partitionId);
    }
}
