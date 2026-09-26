// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * The {@code assembling} adapter, split out of {@link SubscriptionHub}.
 *
 * <p>⚠️ SPLIT FOR SIZE, AND THIS IS THE RIGHT PIECE TO MOVE. code-structure.md
 * rule 1 caps a file at 700 lines and {@code SubscriptionHub} reached 730 when
 * {@code direct} landed. Everything else there is about SERVING -- the fan-out,
 * the modes, the grant, the sinks -- and this is the one part belonging to a
 * CONSUMER: it exists so a caller can say "give me the whole segment" without
 * implementing a sink.
 */
final class AssemblingSubscriber {

    private AssemblingSubscriber() {
    }

    /**
     * A subscriber that assembles the whole segment and hands it over at once.
     *
     * <p>⚠️ ONE ARRAY, SHARED BY REFERENCE ACROSS A CONSUMER'S RUNS. A
     * subscriber holding K runs of a segment is handed K {@link SubscriptionHub.Push} records
     * whose {@code segment()} is the SAME array -- not K copies. A callback
     * that decodes in place, or zeroes the buffer it was given, corrupts the
     * other K-1; {@link SegmentSink}'s contract already says a consumer that
     * needs to retain bytes copies them itself, and this is where that bites.
     *
     * <p>⚠️ IT PAYS O(SEGMENT) PER SUBSCRIBER ON PURPOSE, and that cost belongs
     * to the subscriber rather than to the serving path: in production this
     * side is a socket, and {@code SegmentSink}'s contract already says a sink
     * that needs to retain bytes copies them itself. ⚠️ SO IT MUST NOT BE USED
     * BY A TEST ASSERTING CRITERION 6 -- the bound that must be flat in K is the
     * SERVICE's, and this adapter is the consumer.
     */
    static SubscriptionHub.Subscriber of(Consumer<SubscriptionHub.Push> onSegment) {
        Objects.requireNonNull(onSegment, "onSegment");
        return new SubscriptionHub.Subscriber() {
            @Override
            public SegmentSink open(List<SubscriptionHub.Push> pushes) {
                return new AssemblingSink();
            }

            @Override
            public void complete(List<SubscriptionHub.Push> pushes, SegmentSink sink) {
                // ⚠️ ONE COPY, HANDED OUT K TIMES. The array is built
                // once and shared by reference across the K pushes; it is not
                // copied per run, which is the cost this adapter's javadoc
                // warns is O(SEGMENT) PER SUBSCRIBER and not per subscription.
                byte[] whole = ((AssemblingSink) sink).out.toByteArray();
                for (SubscriptionHub.Push push : pushes) {
                    // ⚠️ THE GRANT TRAVELS, and dropping it was a LIVE DEFECT
                    // review measured: the six-argument constructor leaves it
                    // null, so a `direct` push rebuilt here hits `Push`'s own
                    // "a direct push without a grant" guard, that throw lands
                    // inside `complete()`, and `deliver` swallows it as a slow
                    // subscriber -- the consumer loses the whole window with no
                    // log and no counter. An invariant added in one place and
                    // not honoured in another is worse than no invariant.
                    onSegment.accept(new SubscriptionHub.Push(push.key(), push.segmentKey(), push.recordCount(),
                            push.firstOffset(), push.via(), whole, push.grant(), push.sequencerEpoch(),
                            push.chainSequence()));
                }
            }
        };
    }

    /**
     * Assembles only {@code inline} segments; every other push is handed on
     * with an empty segment and nothing copied (M10.3, NFR-6, ADR-0073).
     *
     * <p>⚠️ **A {@code proxy} OR {@code direct} PUSH CARRIES NO BYTES ON THE
     * WIRE**, so assembling one per session bought a whole-segment copy -- up to
     * {@code maxSegmentBytes} (8 MiB by default) per session for every segment
     * above the inline cap and every cold publish -- charged to the node's queue
     * budget and then dropped at encode. The consumer fetches those bytes itself: through the
     * segment route, or under its grant.
     */
    static SubscriptionHub.Subscriber inlineOnly(Consumer<SubscriptionHub.Push> onSegment) {
        Objects.requireNonNull(onSegment, "onSegment");
        SubscriptionHub.Subscriber assembling = of(onSegment);
        return new SubscriptionHub.Subscriber() {
            @Override
            public SegmentSink open(List<SubscriptionHub.Push> pushes)
                    throws java.io.IOException {
                return carriesBytes(pushes) ? assembling.open(pushes) : DISCARDING;
            }

            @Override
            public void complete(List<SubscriptionHub.Push> pushes, SegmentSink sink)
                    throws java.io.IOException {
                if (carriesBytes(pushes)) {
                    assembling.complete(pushes, sink);
                    return;
                }
                for (SubscriptionHub.Push push : pushes) {
                    onSegment.accept(new SubscriptionHub.Push(push.key(), push.segmentKey(),
                            push.recordCount(), push.firstOffset(), push.via(), EMPTY,
                            push.grant(), push.sequencerEpoch(), push.chainSequence()));
                }
            }
        };
    }

    private static final byte[] EMPTY = new byte[0];

    private static final SegmentSink DISCARDING = (buffer, offset, length) -> { };

    private static boolean carriesBytes(List<SubscriptionHub.Push> pushes) {
        return pushes.stream().anyMatch(push ->
                push.via() == io.github.huyz0.os.biningester.format.FetchMode.INLINE);
    }

    private static final class AssemblingSink implements SegmentSink {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        @Override
        public void write(byte[] buffer, int offset, int length) {
            out.write(buffer, offset, length);
        }
    }
}
