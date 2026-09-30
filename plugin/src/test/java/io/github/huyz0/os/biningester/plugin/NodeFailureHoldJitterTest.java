// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.client.SegmentSource;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.Grant;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.ConcurrentHashMap;
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

    /**
     * ⚠️ TWO NODES, AS PRODUCTION BUILDS THEM, HOLD DIFFERENTLY (M13.12,
     * M12.11 review T1; M12 criterion 13's "two nodes"): each node's entry
     * point draws its own jitter, so the same failures on two nodes are not
     * held in lockstep. A fixed seed shared by every node would pass every
     * other case here.
     */
    @Test
    void twoNodesBuiltThroughTheirEntryPointHoldTheSameFailuresDifferently()
            throws Exception {
        List<Long> first = holdsOfANodeInItsOwnClassLoader();
        List<Long> second = holdsOfANodeInItsOwnClassLoader();

        long floor = NodeSegmentSource.FAILURE_HOLD_FLOOR.toMillis();
        assertThat(first).allSatisfy(hold -> assertThat(hold).isBetween(floor + 1,
                floor + floor / 2));
        assertThat(second).as("⚠️ NOT IN LOCKSTEP: the same ten failures are held"
                + " differently on the two nodes").isNotEqualTo(first);
    }

    /**
     * {@link #holdsOfANode} in a class loader of its own (M13.12 review T1): in
     * production every node is its own JVM, so static state is not shared
     * between nodes -- and a JVM-wide fixed seed, which two nodes in one loader
     * would draw from in turn, must not look like two nodes drawing apart.
     */
    @SuppressWarnings("unchecked")
    private static List<Long> holdsOfANodeInItsOwnClassLoader() throws Exception {
        String[] entries = System.getProperty("java.class.path")
                .split(System.getProperty("path.separator"));
        java.net.URL[] urls = new java.net.URL[entries.length];
        for (int i = 0; i < entries.length; i++) {
            urls[i] = java.nio.file.Path.of(entries[i]).toUri().toURL();
        }
        try (java.net.URLClassLoader loader = new java.net.URLClassLoader(urls,
                ClassLoader.getPlatformClassLoader())) {
            Class<?> isolated = loader.loadClass(NodeFailureHoldJitterTest.class.getName());
            assertThat(isolated).as("the premise: a class of the node's own loader")
                    .isNotSameAs(NodeFailureHoldJitterTest.class);
            java.lang.reflect.Method holds = isolated.getDeclaredMethod("holdsOfANode");
            holds.setAccessible(true);
            return (List<Long>) holds.invoke(null);
        }
    }

    /** How long a node built through its entry point holds each of ten keys' first failure. */
    private static List<Long> holdsOfANode() throws Exception {
        Map<String, AtomicInteger> calls = new ConcurrentHashMap<>();
        AtomicLong millis = new AtomicLong(1_000_000);
        NodeSegmentSource hold = new NodeSegmentSource(new SegmentSource() {
            @Override
            public byte[] fetch(Grant grant) throws IOException {
                return fetchSegment(grant.url());
            }

            @Override
            public byte[] fetchSegment(String segmentKey) throws IOException {
                calls.computeIfAbsent(segmentKey, k -> new AtomicInteger()).incrementAndGet();
                throw new IOException("503 from the store");
            }
        }, 1L << 20);
        NodeSubscriptions subscriptions = new NodeSubscriptions(new SubscriptionTransport() {
            @Override
            public AutoCloseable subscribe(RunKey key, Listener listener) {
                return () -> { };
            }
        }, 16, hold);
        List<Long> holds = new ArrayList<>();
        try {
            subscriptions.holdFailuresWith(millis::get);
            for (int i = 0; i < 10; i++) {
                String key = "seg-" + i;
                long start = millis.get();
                assertThatThrownBy(() -> hold.fetchSegment(key)).isInstanceOf(IOException.class);
                while (calls.get(key).get() == 1) {
                    millis.incrementAndGet();
                    assertThatThrownBy(() -> hold.fetchSegment(key))
                            .isInstanceOf(IOException.class);
                }
                holds.add(millis.get() - start);
            }
        } finally {
            subscriptions.close();
        }
        return holds;
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
