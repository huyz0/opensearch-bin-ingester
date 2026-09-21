// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;

/**
 * What a consumer is told when the records it would resume from have been
 * collected (M7.16, FR-9, FR-10).
 *
 * <p>⚠️ IT IS DISTINGUISHABLE FROM BOTH ITS NEIGHBOURS, and that is the whole
 * point. An empty read means "nothing new"; a {@link DeliveryGapException}
 * means "records were dropped between two deliveries and here is the range",
 * and a consumer can carry on. THIS means "your own starting point is gone and
 * data was lost" — an operator incident, not a retry, because carrying on would
 * silently skip everything below the floor.
 *
 * <p>⚠️ SILENCE HERE IS ADR-0020's FAILURE MODE ONE LEVEL UP: a shard that
 * starts, reports healthy and indexes nothing. The 404 a consumer would
 * otherwise meet on the segment it was about to fetch is the same fact with no
 * name on it and no range attached.
 *
 * <p>⚠️ IT CARRIES THE SIZE OF THE LOSS. 380 records is a different incident
 * from 3, and an operator deciding whether to reindex needs the number rather
 * than the fact.
 */
public final class PositionCollectedException extends IOException {

    private static final long serialVersionUID = 1L;

    private final RunKey key;
    private final long requestedOffset;
    private final long oldestRetainedOffset;

    public PositionCollectedException(RunKey key, long requestedOffset,
            long oldestRetainedOffset) {
        super("stream " + key + ": offset " + requestedOffset + " has been collected -- the "
                + "oldest retained offset is " + oldestRetainedOffset + ", so "
                + (oldestRetainedOffset - requestedOffset) + " records between them are GONE "
                + "and this consumer has lost data");
        this.key = java.util.Objects.requireNonNull(key, "key");
        this.requestedOffset = requestedOffset;
        this.oldestRetainedOffset = oldestRetainedOffset;
    }

    /** The stream whose records are gone. */
    public RunKey key() {
        return key;
    }

    /** Where this consumer would have resumed. */
    public long requestedOffset() {
        return requestedOffset;
    }

    /** The oldest offset still readable, INCLUSIVE. */
    public long oldestRetainedOffset() {
        return oldestRetainedOffset;
    }

    /** How many records are gone — the size of the incident. */
    public long lostRecords() {
        return oldestRetainedOffset - requestedOffset;
    }
}
