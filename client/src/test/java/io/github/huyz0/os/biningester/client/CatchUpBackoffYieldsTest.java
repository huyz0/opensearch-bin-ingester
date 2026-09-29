// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;

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
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * M12.26 (split from M12.12): a catch-up head that is BACKING OFF gives its
 * quantum turn back to live. Once {@link ConsumerClient#LIVE_RECORD_QUANTUM}
 * live records were served the catch-up head was loaded first, and its
 * backoff escaped {@code readNext} before live was tried -- so live waited
 * out every catch-up backoff until the catch-up gave up.
 *
 * <p>⚠️ IT NEEDS A CLOCK. With the backoff a debt paid only by waiting, a
 * catch-up that yielded its turn would never be retried while live kept the
 * reader busy; with a due time on the policy's clock it is retried as soon as
 * it is due, whatever live is doing.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class CatchUpBackoffYieldsTest {

    private static final RunKey KEY = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000ce"), 0);

    private static byte[] segmentOf(String id, long offset) throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(KEY, new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                ("{\"id\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8)), offset);
        return w.toByteArray(offset);
    }

    private static Delivery proxied(String segmentKey, long firstOffset) {
        return new Delivery(KEY, segmentKey, 1, firstOffset, FetchMode.PROXY, new byte[0]);
    }

    @Test
    void aBackingOffCatchUpYieldsItsTurnToLiveAndIsRetriedWhenDue() throws Exception {
        Map<String, byte[]> segments = new ConcurrentHashMap<>();
        for (int i = 0; i < 10; i++) {
            segments.put("L" + i, segmentOf("L" + i, 100 + i));
        }
        segments.put("C0", segmentOf("C0", 0));
        AtomicInteger catchUpFetches = new AtomicInteger();
        SegmentSource source = new SegmentSource() {
            @Override public byte[] fetch(Grant grant) {
                throw new AssertionError("fetched by key");
            }

            @Override public byte[] fetchSegment(String segmentKey) throws IOException {
                if (segmentKey.equals("C0") && catchUpFetches.incrementAndGet() == 1) {
                    throw new IOException("502 from the ingester");
                }
                return segments.get(segmentKey);
            }
        };
        AtomicLong millis = new AtomicLong(1_000_000);
        List<Duration> waited = new CopyOnWriteArrayList<>();
        SegmentFetchRetry retry = new SegmentFetchRetry(Duration.ofSeconds(1),
                Duration.ofSeconds(30), 8, waited::add, millis::get);

        try (ConsumerClient c = new ConsumerClient(KEY, 64, source, retry)) {
            for (int i = 0; i < 10; i++) {
                c.deliver(proxied("L" + i, 100 + i));
            }
            UUID request = UUID.randomUUID();
            c.beginCatchUp(request);
            c.deliverCatchUp(request, proxied("C0", 0));

            for (int i = 0; i < ConsumerClient.LIVE_RECORD_QUANTUM; i++) {
                assertThat(id(c.readNext(Duration.ofSeconds(5)))).as("live record %d", i)
                        .isEqualTo("L" + i);
            }
            assertThat(c.readNext(Duration.ofSeconds(5)))
                    .as("the premise: the catch-up's turn, and its fetch failed").isEmpty();
            assertThat(catchUpFetches).hasValue(1);

            assertThat(id(c.readNext(Duration.ofSeconds(5))))
                    .as("⚠️ LIVE IS SERVED AT ONCE while the catch-up backs off, not after it")
                    .isEqualTo("L8");
            assertThat(catchUpFetches).as("the catch-up was not fetched before it was due")
                    .hasValue(1);
            assertThat(waited).as("and nobody waited out its backoff").isEmpty();

            millis.addAndGet(Duration.ofSeconds(2).toMillis());

            assertThat(id(c.readNext(Duration.ofSeconds(5))))
                    .as("⚠️ DUE NOW: retried while live still has records, and served")
                    .isEqualTo("C0");
            assertThat(catchUpFetches).hasValue(2);
        }
    }

    /**
     * ⚠️ WITH A CLOCK THE WAIT IS WHAT IS LEFT UNTIL DUE (review T1, T2): not
     * zero, which would return empty polls in a spin for the whole backoff,
     * and not the whole backoff again once part of it has passed; and at the
     * due instant itself the fetch is made.
     */
    @Test
    void aClockedBackoffWaitsExactlyWhatIsLeftUntilDueAndFetchesAtTheDueInstant()
            throws Exception {
        AtomicInteger fetches = new AtomicInteger();
        byte[] only = segmentOf("only", 7);
        SegmentSource source = new SegmentSource() {
            @Override public byte[] fetch(Grant grant) {
                throw new AssertionError("fetched by key");
            }

            @Override public byte[] fetchSegment(String segmentKey) throws IOException {
                if (fetches.incrementAndGet() == 1) {
                    throw new IOException("502 from the ingester");
                }
                return only;
            }
        };
        AtomicLong millis = new AtomicLong(1_000_000);
        List<Duration> waited = new CopyOnWriteArrayList<>();
        SegmentFetchRetry retry = new SegmentFetchRetry(Duration.ofSeconds(1),
                Duration.ofSeconds(30), 8, waited::add, millis::get);

        try (ConsumerClient c = new ConsumerClient(KEY, 16, source, retry)) {
            c.deliver(proxied("only", 7));
            assertThat(c.readNext(Duration.ofSeconds(10))).as("the fetch failed").isEmpty();
            assertThat(waited).as("the call that fetched waits no further").isEmpty();

            assertThat(c.readNext(Duration.ofSeconds(10))).isEmpty();
            assertThat(waited).as("⚠️ A WAIT, not an empty poll in a spin").hasSize(1);
            Duration backoff = waited.get(0);
            assertThat(backoff).as("the floor's jitter").isBetween(Duration.ofMillis(500),
                    Duration.ofMillis(1_500));

            long half = backoff.toMillis() / 2;
            millis.addAndGet(half);
            assertThat(c.readNext(Duration.ofSeconds(10))).isEmpty();
            assertThat(waited).as("⚠️ ONLY WHAT IS LEFT, not the backoff again")
                    .containsExactly(backoff, backoff.minusMillis(half));
            assertThat(fetches).as("nothing fetched before due").hasValue(1);

            millis.addAndGet(backoff.toMillis() - half);
            assertThat(id(c.readNext(Duration.ofSeconds(10))))
                    .as("⚠️ AT THE DUE INSTANT the fetch is made").isEqualTo("only");
            assertThat(waited).hasSize(2);
            assertThat(fetches).hasValue(2);
        }
    }

    private static String id(Optional<ConsumerRecord> record) {
        assertThat(record).as("a record").isPresent();
        return record.get().record().id();
    }
}
