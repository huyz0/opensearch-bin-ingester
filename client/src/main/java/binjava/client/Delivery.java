// SPDX-License-Identifier: Apache-2.0
package binjava.client;

import binjava.format.RunKey;
import java.util.Objects;

/**
 * One stream's share of a commit, as the transport hands it over.
 *
 * <p>WARNING: the segment bytes travel WITH the delivery. M1 ships `inline`
 * only (ADR-0004), so a consumer issues NO object-store request to read what it
 * was just told about -- which is what makes the zero-idle-cost property hold
 * under load and not merely at rest. `proxy` and `direct` are M5.
 */
public record Delivery(RunKey key, String segmentKey, int recordCount, long firstOffset,
        byte[] segment) {

    public Delivery {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(segmentKey, "segmentKey");
        Objects.requireNonNull(segment, "segment");
        if (recordCount <= 0) {
            throw new IllegalArgumentException("a delivery of nothing is not a delivery");
        }
        if (firstOffset < 0) {
            throw new IllegalArgumentException("offsets are never negative");
        }
    }
}
