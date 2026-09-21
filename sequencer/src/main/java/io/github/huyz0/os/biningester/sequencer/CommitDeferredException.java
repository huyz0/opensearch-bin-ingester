// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import java.io.IOException;
import java.util.Objects;

/**
 * The commit could not reach a sequencer, and its INTENT is durable in the
 * inbox instead (M8.14a, ADR-0058).
 *
 * <p>⚠️ **NOT A FAILURE TO THE PRODUCER.** The segment is in the store and so
 * is the request that commits it; the leaseholder applies it when it drains
 * the inbox, deduplicated by the request's triple. FR-4, as ADR-0058 amends
 * it, lets the append complete on this -- offsets unknown until the drain.
 *
 * <p>⚠️ **AN {@code IOException}, SO A CALLER THAT DOES NOT KNOW IT FAILS
 * SAFE**: an older caller treats it as a failed commit and the producer
 * retries, which the triple also deduplicates.
 */
public final class CommitDeferredException extends IOException {

    private static final long serialVersionUID = 1L;

    private final String intentKey;

    public CommitDeferredException(String intentKey, IOException because) {
        super("no sequencer was reachable; the commit's intent is durable at " + intentKey
                + " and is applied when the leaseholder drains the inbox", because);
        this.intentKey = Objects.requireNonNull(intentKey, "intentKey");
    }

    /** Where the intent is. */
    public String intentKey() {
        return intentKey;
    }
}
