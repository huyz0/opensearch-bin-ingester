// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.Objects;

/**
 * One stream's share of a commit, as the transport hands it over.
 *
 * <p>WARNING: under `inline` and `proxy` the segment bytes are IN HAND by the
 * time a delivery exists. How they got here is what {@code via} records:
 * `inline` means they travelled with the push, `proxy` that the ingester
 * streamed them through. On both the consumer issues NO object-store request to
 * read what it was just told about, which is what makes the zero-idle-cost
 * property hold under load rather than merely at rest.
 *
 * <p>⚠️ UNDER `direct` THE BYTES ARE NOT IN HAND AND THE CONSUMER DOES ISSUE A
 * REQUEST -- {@link #segment()} is EMPTY and {@link #grant()} is what
 * {@code ConsumerClient} fetches with, through {@link SegmentSource} (M5.45g).
 * An earlier version of this paragraph said the bytes are always in hand, that
 * the consumer issues no request either way, and that `direct` "cannot reach
 * this type yet"; the commit that added {@link #grant()} falsified all three
 * and left them standing.
 *
 * <p>⚠️ AND THAT REQUEST IS PER RUN, WHICH IS WHERE THE COALESCING HAS TO GO.
 * One delivery is one run, so a node holding K runs of a segment fetches K
 * times unless something above this type merges them -- ~400 whole-object GETs
 * for one 8 MiB segment on a catch-up node, shards-per-node, which
 * non-negotiable 6 forbids by name and which nothing in the tree can measure,
 * since consumer-side GETs are invisible to the ingester's
 * {@code CountingBinStore}. **M5.45h** owns the merge, and the production
 * {@link SegmentSource} (M8.31) is only ever handed out behind it (M5.45g
 * criterion 6, ADR-0044).
 * A reader looking for the coalescing point should look at
 * {@code ConsumerClient.decodeInto}, not at the hub.
 *
 * <p>⚠️ {@code sequencerEpoch} IS THE CHAIN'S TERM, NOT THE SESSION'S
 * (M5.15d, SPEC criterion 12). A consumer holds both and they answer different
 * questions: this one moves when the leaseholder moves and says nothing about
 * the consumer, while the session epoch orders that consumer's own requests and
 * says nothing about the chain. Conflating them makes every failover look like
 * a lost session, and every session reset look like a failover.
 *
 * <p>⚠️ {@code via} IS TOLD TO THE CONSUMER, NOT ASKED OF IT (FR-6). It is a
 * record component with no setter and nothing on {@code SubscriptionTransport}
 * carries a preference, so there is no expression a consumer could write to
 * demand a mode. That is rung 1 of the gate-design ladder rather than a
 * documented rule: a consumer that could demand `direct` at fan-out 300 would
 * reproduce the $3,732/month design ADR-0004 rejected.
 */
public record Delivery(RunKey key, String segmentKey, int recordCount, long firstOffset,
        FetchMode via, byte[] segment, io.github.huyz0.os.biningester.format.Grant grant, long sequencerEpoch) {

    /**
     * What {@link #sequencerEpoch()} carries when the publisher models no
     * chain. ⚠️ Not 0, which M4.4b reserves for "no lease", and the same value
     * {@code SubscriptionHub.EPOCH_UNKNOWN} and {@code Sequencer.EPOCH_UNKNOWN}
     * carry -- restated rather than imported, because `client` depends on
     * neither module (ADR-0023, architecture.md).
     */
    public static final long EPOCH_UNKNOWN = -1L;

    /** A delivery with no grant, which is every {@code inline} and {@code proxy} one. */
    public Delivery(RunKey key, String segmentKey, int recordCount, long firstOffset,
            FetchMode via, byte[] segment) {
        this(key, segmentKey, recordCount, firstOffset, via, segment, null, EPOCH_UNKNOWN);
    }

    /**
     * A delivery with a grant and no chain.
     *
     * <p>⚠️ IT EXISTS SO THE EPOCH COST NO CALL SITES (M5.15d), which is the
     * same licence the no-grant form above took. Every caller that omits it
     * models no chain -- a fixture, or a transport under test -- and saying
     * {@code EPOCH_UNKNOWN} out loud at seventeen sites would bury the two
     * that carry a real one.
     */
    public Delivery(RunKey key, String segmentKey, int recordCount, long firstOffset,
            FetchMode via, byte[] segment, io.github.huyz0.os.biningester.format.Grant grant) {
        this(key, segmentKey, recordCount, firstOffset, via, segment, grant, EPOCH_UNKNOWN);
    }

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
        // ⚠️ REFUSED WHERE IT IS BUILT, which is the asymmetry M5.44 closed
        // three times over on the wire and M5.45d closed one layer up on
        // `SubscriptionHub.Push`. A `direct` delivery without a grant tells a
        // consumer to fetch and not how; it would reach `decodeInto`, find an
        // EMPTY segment, and throw somewhere far from the mistake.
        if (grant == null && via == FetchMode.DIRECT) {
            throw new IllegalArgumentException(
                    "a `direct` delivery without a grant tells a consumer to fetch and not how");
        }
        // ⚠️ AND THE OTHER DIRECTION, because security.md rule 4 makes an
        // unnecessary secret a COST rather than waste: a signed URL minted for
        // a consumer that will never fetch with it is one more place it leaks
        // from, and every such place is one a future reader has to check.
        if (grant != null && via != FetchMode.DIRECT) {
            throw new IllegalArgumentException(
                    "a grant is for `direct`; this delivery is " + via);
        }
    }
}
