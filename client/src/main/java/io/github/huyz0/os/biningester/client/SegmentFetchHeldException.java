// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;

/**
 * A segment fetch answered from a node's held FAILURE, not by the source
 * (M10.28): the node fetched the segment and failed, and holds that failure
 * for a backoff so its runs of the segment do not fetch it once each.
 *
 * <p>⚠️ NOT AN ATTEMPT. A consumer's retry policy counts fetches that reached
 * the source; counting this one too would spend a run's attempts on answers
 * nothing was asked for, and pause a shard whose store had already recovered
 * (M10.28 review F1). {@code SegmentFetcher} waits out {@link #remaining()} and
 * asks again, and surfaces the failure once {@link #nodeAttempts()} -- the
 * node's own consecutive fetches of the segment -- has risen by its attempt
 * budget SINCE THE RUN BEGAN WAITING, so the runs share one schedule, still
 * pause where an operator sees it, and a resumed run gets a round of its own.
 */
public final class SegmentFetchHeldException extends IOException {

    private static final long serialVersionUID = 1L;

    private final transient Duration remaining;
    private final int nodeAttempts;

    public SegmentFetchHeldException(String message, IOException cause, Duration remaining,
            int nodeAttempts) {
        super(message, Objects.requireNonNull(cause, "cause"));
        this.remaining = Objects.requireNonNull(remaining, "remaining");
        if (remaining.isNegative() || nodeAttempts < 1) {
            throw new IllegalArgumentException("a hold has time left and at least one fetch "
                    + "behind it: " + remaining + ", " + nodeAttempts);
        }
        this.nodeAttempts = nodeAttempts;
    }

    /** How long the node holds the failure yet. */
    public Duration remaining() {
        return remaining;
    }

    /** How many times the node has fetched the segment and failed, consecutively. */
    public int nodeAttempts() {
        return nodeAttempts;
    }
}
