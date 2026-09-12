// SPDX-License-Identifier: Apache-2.0
package binjava.client;

import binjava.format.FetchMode;
import binjava.format.RunKey;
import java.util.Objects;

/**
 * One stream's share of a commit, as the transport hands it over.
 *
 * <p>WARNING: the segment bytes are IN HAND by the time a delivery exists. How
 * they got here is what {@code via} records: `inline` means they travelled with
 * the push, `proxy` that the ingester streamed them through. Either way the
 * consumer issues NO object-store request to read what it was just told about,
 * which is what makes the zero-idle-cost property hold under load rather than
 * merely at rest. `direct` -- where the consumer reads the store itself -- is
 * M5.45b and cannot reach this type yet.
 *
 * <p>⚠️ {@code via} IS TOLD TO THE CONSUMER, NOT ASKED OF IT (FR-6). It is a
 * record component with no setter and nothing on {@code SubscriptionTransport}
 * carries a preference, so there is no expression a consumer could write to
 * demand a mode. That is rung 1 of the gate-design ladder rather than a
 * documented rule: a consumer that could demand `direct` at fan-out 300 would
 * reproduce the $3,732/month design ADR-0004 rejected.
 */
public record Delivery(RunKey key, String segmentKey, int recordCount, long firstOffset,
        FetchMode via, byte[] segment) {

    public Delivery {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(segmentKey, "segmentKey");
        Objects.requireNonNull(via, "via");
        Objects.requireNonNull(segment, "segment");
        if (recordCount <= 0) {
            throw new IllegalArgumentException("a delivery of nothing is not a delivery");
        }
        if (firstOffset < 0) {
            throw new IllegalArgumentException("offsets are never negative");
        }
    }
}
