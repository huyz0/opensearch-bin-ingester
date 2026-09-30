// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.Grant;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The consumer retry's residue from M10.23's review (M11.13, H8): each
 * behaviour that review named unpinned, pinned.
 */
class ConsumerRetryResidueTest {

    private static final RunKey KEY = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000cd"), 0);

    /** Fails every fetch, counting them. */
    private static final class AlwaysFails implements SegmentSource {
        final AtomicInteger attempts = new AtomicInteger();

        @Override public byte[] fetch(Grant grant) throws IOException {
            return fetchSegment(grant.url());
        }

        @Override public byte[] fetchSegment(String segmentKey) throws IOException {
            attempts.incrementAndGet();
            throw new IOException("502 from the ingester");
        }
    }

    private static Delivery proxied(long firstOffset) {
        return new Delivery(KEY, "bins/c/data/seg.bseg", 1, firstOffset, FetchMode.PROXY,
                new byte[0]);
    }

    private static byte[] segmentOf(String id, long offset) throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(KEY, new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                ("{\"id\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8)), offset);
        return w.toByteArray(offset);
    }

    @Test
    void aSurfacedFailureStartsAFreshRoundOfAttempts() throws Exception {
        SegmentFetchRetry two = TestRetries.sleepAdvanced(Duration.ofSeconds(1),
                Duration.ofSeconds(30), 2, wait -> { });
        AlwaysFails source = new AlwaysFails();
        try (ConsumerClient client = new ConsumerClient(KEY, 16, source, two)) {
            client.deliver(proxied(0));
            assertThat(client.readNext(Duration.ofSeconds(5))).as("attempt 1").isEmpty();
            assertThat(client.readNext(Duration.ofSeconds(5))).as("its backoff").isEmpty();
            assertThatThrownBy(() -> client.readNext(Duration.ofSeconds(5)))
                    .as("the premise: attempt 2 of 2 surfaces").isInstanceOf(Exception.class);

            assertThat(client.readNext(Duration.ofSeconds(5)))
                    .as("⚠️ THE OPERATOR's RESUME IS ATTEMPT 1 OF A NEW ROUND, not a third "
                            + "that surfaces again at once")
                    .isEmpty();
            assertThat(source.attempts).hasValue(3);
        }
    }

    @Test
    void eachBackoffDoublesToTheCeilingAndIsJittered() throws Exception {
        List<Duration> waits = new CopyOnWriteArrayList<>();
        SegmentFetchRetry many = TestRetries.sleepAdvanced(Duration.ofSeconds(1),
                Duration.ofSeconds(4), 50, waits::add);
        AlwaysFails source = new AlwaysFails();
        try (ConsumerClient client = new ConsumerClient(KEY, 16, source, many)) {
            client.deliver(proxied(0));
            for (int poll = 0; poll < 2 * 12; poll++) {
                // each failed fetch is followed by the poll that serves its backoff
                assertThat(client.readNext(Duration.ofSeconds(60))).isEmpty();
            }
        }
        assertThat(waits).as("the premise: twelve backoffs served").hasSize(12);
        long[] nominal = {1_000, 2_000, 4_000};
        for (int i = 0; i < waits.size(); i++) {
            long base = nominal[Math.min(i, 2)];
            assertThat(waits.get(i).toMillis())
                    .as("⚠️ BACKOFF %d IS 0.5-1.5 x %d ms: doubling from the floor, capped at "
                            + "the ceiling", i + 1, base)
                    .isBetween(base / 2, base * 3 / 2);
        }
        assertThat(waits.subList(2, waits.size()).stream().distinct().count())
                .as("⚠️ JITTERED: ten backoffs at the ceiling are not all one value -- a "
                        + "rolling deploy's consumers would retry in step")
                .isGreaterThan(1);
    }

    @Test
    void aDuplicateDeliveryIsNotFetched() throws Exception {
        AtomicInteger fetches = new AtomicInteger();
        byte[] bytes = segmentOf("doc-0", 0);
        SegmentSource counting = new SegmentSource() {
            @Override public byte[] fetch(Grant grant) {
                throw new AssertionError("proxy only");
            }

            @Override public byte[] fetchSegment(String segmentKey) {
                fetches.incrementAndGet();
                return bytes;
            }
        };
        try (ConsumerClient client = new ConsumerClient(KEY, 16, counting,
                TestRetries.noFailedFetch())) {
            client.deliver(proxied(0));
            assertThat(client.readNext(Duration.ofSeconds(5))).isPresent();
            client.deliver(proxied(0));
            assertThat(client.readNext(Duration.ofMillis(200))).as("a duplicate is dropped")
                    .isEmpty();
        }
        assertThat(fetches)
                .as("⚠️ AND NOT FETCHED: a redelivery of offsets already read costs no GET")
                .hasValue(1);
    }

    /**
     * The STANDARD policy retries (M13.6c): production builds every client on
     * it -- {@code NodeSubscriptions} -- since the constructors that picked
     * their own policy are gone. A standard policy of one attempt would
     * restore the pause on every 502. The clock is fixed, so the backoff owed
     * never comes due and nothing sleeps.
     */
    @Test
    void aSubscribingClientOnTheStandardPolicyRetries() throws Exception {
        List<SubscriptionTransport.Listener> listeners = new CopyOnWriteArrayList<>();
        SubscriptionTransport transport = new SubscriptionTransport() {
            @Override
            public AutoCloseable subscribe(RunKey key, Listener listener) {
                listeners.add(listener);
                return () -> { };
            }
        };
        AlwaysFails source = new AlwaysFails();
        try (ConsumerClient client = new ConsumerClient(transport, KEY, 16, source,
                SegmentFetchRetry.standard(() -> 0L))) {
            listeners.forEach(l -> l.onDelivery(proxied(0)));
            assertThat(client.readNext(Duration.ZERO))
                    .as("⚠️ A 502 IS AN EMPTY POLL, not a pause: production builds every "
                            + "client on the standard policy")
                    .isEmpty();
            assertThat(source.attempts).as("fetched once, and owed a backoff").hasValue(1);
        }
    }
}
