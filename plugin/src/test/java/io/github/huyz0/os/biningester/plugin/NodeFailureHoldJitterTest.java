// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.client.SegmentSource;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.Grant;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongUnaryOperator;
import java.util.random.RandomGenerator;
import org.junit.jupiter.api.Test;

/**
 * M12.11 (M10.28b P1, T2): the node failure hold is jittered, so a blip that
 * fails one segment on every node does not re-fetch it in lockstep -- and only
 * ever LENGTHENED, so the consumer's attempts last the outage its policy covers
 * -- and the doubling's cap is pinned past where an unbounded shift wraps.
 */
class NodeFailureHoldJitterTest {

    /**
     * ⚠️ PAST THE SHIFT's WRAP: {@code floor << 64} is {@code floor} in Java, so
     * without the doubling cap a run's 65th failure would drop back to the floor.
     */
    @Test
    void theHoldIsTheCeilingAtFiftyFiveAndSixtyFiveFailures() {
        long ceiling = NodeSegmentSource.FAILURE_HOLD_CEILING.toMillis();

        assertThat(NodeSegmentSource.holdMillis(55)).isEqualTo(ceiling);
        assertThat(NodeSegmentSource.holdMillis(65)).isEqualTo(ceiling);
        assertThat(NodeSegmentSource.holdMillis(1))
                .isEqualTo(NodeSegmentSource.FAILURE_HOLD_FLOOR.toMillis());
    }

    /** Up-jitter: every hold in (base, 1.5 base], and two nodes' seeds part. */
    @Test
    void upJitterOnlyLengthensAndTwoSeedsHoldDifferently() {
        long base = NodeSegmentSource.FAILURE_HOLD_CEILING.toMillis();
        LongUnaryOperator one = NodeSegmentSource.upJitter(new SplittableRandom(1));
        LongUnaryOperator two = NodeSegmentSource.upJitter(new SplittableRandom(2));
        boolean parted = false;
        for (int i = 0; i < 100; i++) {
            long a = one.applyAsLong(base);
            long b = two.applyAsLong(base);
            assertThat(a).isBetween(base + 1, base + base / 2);
            assertThat(b).isBetween(base + 1, base + base / 2);
            parted |= a != b;
        }
        assertThat(parted).as("⚠️ NOT IN LOCKSTEP: different seeds, different holds").isTrue();
    }

    /** Both ends of the range, from a generator that answers its extremes (review T3). */
    @Test
    void upJittersRangeIsExactlyBasePlusOneToOneAndAHalfBase() {
        long base = 10_000;

        assertThat(NodeSegmentSource.upJitter(extreme(false)).applyAsLong(base))
                .isEqualTo(base + 1);
        assertThat(NodeSegmentSource.upJitter(extreme(true)).applyAsLong(base))
                .isEqualTo(base + base / 2);
    }

    /** The hold a failure starts is its backoff as the jitter makes it. */
    @Test
    void aFailuresHoldIsItsBackoffAsJittered() {
        AtomicInteger calls = new AtomicInteger();
        AtomicLong millis = new AtomicLong(1_000_000);
        NodeSegmentSource source = new NodeSegmentSource(failing(calls), 1L << 20);
        source.holdFailures(millis::get, base -> base / 2);
        long half = NodeSegmentSource.FAILURE_HOLD_FLOOR.toMillis() / 2;

        assertThatThrownBy(() -> source.fetchSegment("seg")).isInstanceOf(IOException.class);
        millis.addAndGet(half - 1);
        assertThatThrownBy(() -> source.fetchSegment("seg")).isInstanceOf(IOException.class);
        assertThat(calls).as("inside the jittered hold").hasValue(1);
        millis.addAndGet(1);
        assertThatThrownBy(() -> source.fetchSegment("seg")).isInstanceOf(IOException.class);
        assertThat(calls).as("the jittered hold is over at half the floor").hasValue(2);
    }

    /**
     * ⚠️ AS PRODUCTION WIRES IT (review T2): the node's entry point turns the
     * jitter on, so a failure is still held at exactly its backoff, where an
     * unjittered hold would have let the next fetch through.
     */
    @Test
    void theNodesHoldIsJitteredAsItsEntryPointWiresIt() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        AtomicLong millis = new AtomicLong(1_000_000);
        NodeSegmentSource hold = new NodeSegmentSource(failing(calls), 1L << 20);
        NodeSubscriptions subscriptions = new NodeSubscriptions(new SubscriptionTransport() {
            @Override
            public AutoCloseable subscribe(RunKey key, Listener listener) {
                return () -> { };
            }
        }, 16, hold);
        try {
            subscriptions.holdFailuresWith(millis::get);

            assertThatThrownBy(() -> hold.fetchSegment("seg")).isInstanceOf(IOException.class);
            millis.addAndGet(NodeSegmentSource.FAILURE_HOLD_FLOOR.toMillis());
            assertThatThrownBy(() -> hold.fetchSegment("seg")).isInstanceOf(IOException.class);
            assertThat(calls).as("still held at exactly the backoff: jittered longer").hasValue(1);
        } finally {
            subscriptions.close();
        }
    }

    private static SegmentSource failing(AtomicInteger calls) {
        return new SegmentSource() {
            @Override
            public byte[] fetch(Grant grant) throws IOException {
                return fetchSegment(grant.url());
            }

            @Override
            public byte[] fetchSegment(String segmentKey) throws IOException {
                calls.incrementAndGet();
                throw new IOException("503 from the store");
            }
        };
    }

    /** Answers {@code nextLong(bound)} with 0, or with {@code bound - 1}. */
    private static RandomGenerator extreme(boolean highest) {
        return new RandomGenerator() {
            @Override
            public long nextLong() {
                throw new UnsupportedOperationException("only bounded draws");
            }

            @Override
            public long nextLong(long bound) {
                return highest ? bound - 1 : 0;
            }
        };
    }
}
