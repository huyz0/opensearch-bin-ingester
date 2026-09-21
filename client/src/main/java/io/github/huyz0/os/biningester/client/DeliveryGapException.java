// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;

/**
 * Records this consumer was never handed (M6.1, FR-10).
 *
 * <p>⚠️ THE ALTERNATIVE IS SILENT DATA LOSS, which is what happened before this
 * type existed. {@code ConsumerClient.deliver} offers into a bounded queue and
 * DROPS when it is full — correct, because blocking would stall the ingester's
 * commit path for every stream on the node, and the queue's own javadoc says
 * so. What was missing is that nothing noticed: the shard read offset 41 after
 * offset 12 and indexed it, so the records between were gone with no exception
 * on any path, no counter, and a shard that looked healthy.
 *
 * <p>⚠️ IT SAYS WHETHER THIS CONSUMER DROPPED THEM, and the two cases need
 * different answers from an operator. A LOCAL drop means this node fell behind
 * — the queue is a capacity to raise or a consumer to speed up. A gap with no
 * local drop means the records never arrived at all, which is the ingester's
 * side of the channel, and raising the queue would do nothing.
 *
 * <p>⚠️ IT DOES NOT RECOVER, and M6 does not pretend to. Re-reading the missing
 * window is the fallback ladder's tier 2 or 3, which read the store; M8 executes
 * tiers 0 and 1 and ADR-0057 leaves 2, 3 and the re-read to M9, with the
 * catch-up read path they need. What this type buys is that the loss
 * is VISIBLE at the moment it happens rather than inferred weeks later from a
 * document count.
 */
public final class DeliveryGapException extends IOException {

    private static final long serialVersionUID = 1L;

    private final RunKey key;
    private final long expectedOffset;
    private final long receivedOffset;
    private final boolean droppedLocally;

    DeliveryGapException(RunKey key, long expectedOffset, long receivedOffset,
            boolean droppedLocally) {
        super("stream " + key + " skipped offsets [" + expectedOffset + ", " + receivedOffset
                + ") -- " + (droppedLocally
                        ? "this consumer's queue was full and dropped them, so it is behind and "
                                + "the records still exist upstream"
                        : "this consumer dropped nothing, so they never arrived and the gap is "
                                + "upstream of it"));
        this.key = key;
        this.expectedOffset = expectedOffset;
        this.receivedOffset = receivedOffset;
        this.droppedLocally = droppedLocally;
    }

    /** The stream the gap is in. */
    public RunKey key() {
        return key;
    }

    /** The first offset that was not delivered. */
    public long expectedOffset() {
        return expectedOffset;
    }

    /** The first offset that WAS delivered after the gap -- exclusive end of the gap. */
    public long receivedOffset() {
        return receivedOffset;
    }

    /** How many records the gap covers. */
    public long missingRecords() {
        return receivedOffset - expectedOffset;
    }

    /**
     * Whether THIS consumer dropped the missing deliveries because its queue was
     * full.
     *
     * <p>⚠️ FALSE MEANS THE GAP IS UPSTREAM, which is the more serious of the
     * two: nothing on this node could have prevented it, and raising the queue
     * capacity would change nothing.
     */
    public boolean droppedLocally() {
        return droppedLocally;
    }
}
