// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.RunKey;
import org.junit.jupiter.api.Test;

/**
 * M12.26: the host clock the node already hands its segment hold also becomes
 * the clock the node's clients' fetch backoffs are due on -- without it a
 * catch-up backing off keeps its quantum turn and live waits out its backoff.
 * A node with no hold (no {@code direct}) still gets it: the catch-up lane
 * backs off either way.
 */
class FetchBackoffClockWiringTest {

    private static SubscriptionTransport quiet() {
        return new SubscriptionTransport() {
            @Override
            public AutoCloseable subscribe(RunKey key, Listener listener) {
                return () -> { };
            }
        };
    }

    @Test
    void theHostClockBecomesTheClientsBackoffClockWithOrWithoutAHold() {
        NodeSubscriptions noHold = new NodeSubscriptions(quiet(), 16, null);
        // ⚠️ INTENTIONALLY CHANGED BY M13.6c: there is no clock-less policy to
        // read as null any more; before the host gives its clock, the node's
        // policy REFUSES to be timed, rather than backing off without one.
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> noHold.fetchRetry().clockMillis().getAsLong())
                .as("the premise: no clock yet, and none silently assumed")
                .isInstanceOf(IllegalStateException.class);

        noHold.holdFailuresWith(() -> 5_000L);

        assertThat(noHold.fetchRetry().clockMillis())
                .as("⚠️ THE CLIENTS' BACKOFFS ARE DUE ON THE HOST's CLOCK").isNotNull();
        assertThat(noHold.fetchRetry().clockMillis().getAsLong()).isEqualTo(5_000L);
    }

    /**
     * ⚠️ THE CLIENT's POLICY, NOT THE NODE's (M12.26 review T3, M13.6c): a client
     * started BEFORE the host gives its clock and one started AFTER both have
     * their failed fetch retried exactly when the host clock passes the
     * backoff -- not before, and not never.
     */
    @Test
    void clientsStartedBeforeAndAfterTheHostClockAreBothDueOnIt() throws Exception {
        java.util.Map<String, java.util.concurrent.atomic.AtomicInteger> fetches =
                new java.util.concurrent.ConcurrentHashMap<>();
        io.github.huyz0.os.biningester.client.SegmentSource failing =
                new io.github.huyz0.os.biningester.client.SegmentSource() {
                    @Override
                    public byte[] fetch(io.github.huyz0.os.biningester.format.Grant grant) {
                        throw new AssertionError("proxied only");
                    }

                    @Override
                    public byte[] fetchSegment(String segmentKey) throws java.io.IOException {
                        fetches.computeIfAbsent(segmentKey,
                                k -> new java.util.concurrent.atomic.AtomicInteger())
                                .incrementAndGet();
                        throw new java.io.IOException("502 from the ingester");
                    }
                };
        java.util.concurrent.atomic.AtomicLong host =
                new java.util.concurrent.atomic.AtomicLong(1_000L);
        CapturingTransport transport = new CapturingTransport();
        try (NodeSubscriptions node = new NodeSubscriptions(transport, 16, failing)) {
            RunKey before = new RunKey(java.util.UUID.randomUUID(), 0);
            io.github.huyz0.os.biningester.client.ConsumerClient early = node.clientFor(before);
            node.holdFailuresWith(host::get);
            RunKey after = new RunKey(java.util.UUID.randomUUID(), 0);
            io.github.huyz0.os.biningester.client.ConsumerClient late = node.clientFor(after);

            for (Object[] run : new Object[][] {{early, before, "seg-early"},
                    {late, after, "seg-late"}}) {
                io.github.huyz0.os.biningester.client.ConsumerClient client =
                        (io.github.huyz0.os.biningester.client.ConsumerClient) run[0];
                String segment = (String) run[2];
                transport.listener.onDelivery(new io.github.huyz0.os.biningester.client.Delivery(
                        (RunKey) run[1], segment, 1, 0L,
                        io.github.huyz0.os.biningester.format.FetchMode.PROXY, new byte[0]));
                assertThat(client.readNext(java.time.Duration.ZERO)).isEmpty();
                assertThat(client.readNext(java.time.Duration.ZERO)).isEmpty();
                assertThat(fetches.get(segment)).as(segment + ": not refetched before due")
                        .hasValue(1);
            }

            host.addAndGet(100_000L); // past any backoff: at most 1.5 x the 30 s ceiling
            assertThat(early.readNext(java.time.Duration.ZERO)).isEmpty();
            assertThat(late.readNext(java.time.Duration.ZERO)).isEmpty();
            assertThat(fetches.get("seg-early")).as("the client started BEFORE the clock")
                    .hasValue(2);
            assertThat(fetches.get("seg-late")).as("the client started after it").hasValue(2);
        }
    }

    /** Hands the node's one listener to the case. */
    private static final class CapturingTransport implements SubscriptionTransport {
        private volatile Listener listener;

        @Override
        public AutoCloseable subscribe(RunKey key, Listener l) {
            return subscribe(java.util.List.of(key), l);
        }

        @Override
        public MultiSubscription subscribe(java.util.List<RunKey> keys, Listener l) {
            this.listener = l;
            return new MultiSubscription() {
                @Override
                public void add(RunKey key) {
                }

                @Override
                public void remove(RunKey key) {
                }

                @Override
                public void close() {
                }
            };
        }
    }
}
