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
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * M11.13's round-1 review: the DEPLOYED backoff served on an injected
 * sleeper (T2), the backoff reset as a failure surfaces (T3), and the
 * catch-up lane's one-decode-at-a-time lock (R3, T1).
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ConsumerRetryDefaultsTest {

    private static final RunKey KEY = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000cd"), 0);

    private static final class RecordingSleeper implements SegmentFetchRetry.Sleeper {
        final List<Duration> waits = new CopyOnWriteArrayList<>();

        @Override public void sleep(Duration wait) {
            waits.add(wait);
        }
    }

    private static byte[] segmentOf(String id, long offset) throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(KEY, new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                ("{\"id\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8)), offset);
        return w.toByteArray(offset);
    }

    /** Fails its first {@code failures} fetches, then serves {@code bytes}. */
    private static SegmentSource failingThen(int failures, byte[] bytes, AtomicInteger attempts) {
        return new SegmentSource() {
            @Override public byte[] fetch(Grant grant) throws IOException {
                return fetchSegment(grant.url());
            }

            @Override public byte[] fetchSegment(String segmentKey) throws IOException {
                if (attempts.incrementAndGet() <= failures) {
                    throw new IOException("502 from the ingester");
                }
                return bytes;
            }
        };
    }

    private static Delivery proxied(String segmentKey, long firstOffset) {
        return new Delivery(KEY, segmentKey, 1, firstOffset, FetchMode.PROXY, new byte[0]);
    }

    @Test
    void theDeployedBackoffIsServedAndTheRetryReadsTheRecords() throws Exception {
        RecordingSleeper sleeper = new RecordingSleeper();
        SegmentFetchRetry deployed = new SegmentFetchRetry(SegmentFetchRetry.DEFAULT.floor(),
                SegmentFetchRetry.DEFAULT.ceiling(), SegmentFetchRetry.DEFAULT.maxAttempts(),
                sleeper);
        AtomicInteger attempts = new AtomicInteger();
        try (ConsumerClient c = new ConsumerClient(KEY, 16,
                failingThen(1, segmentOf("a", 40), attempts), deployed)) {
            c.deliver(proxied("bins/c/data/seg.bseg", 40));

            assertThat(c.readNext(Duration.ZERO)).as("a 502 is an empty poll").isEmpty();
            Optional<ConsumerRecord> got = Optional.empty();
            for (int poll = 0; poll < 5 && got.isEmpty(); poll++) {
                got = c.readNext(Duration.ofHours(3));
            }

            assertThat(got).map(ConsumerRecord::offset).contains(40L);
        }
        assertThat(attempts).as("the failed fetch, then its one retry").hasValue(2);
        assertThat(sleeper.waits).as("⚠️ THE DEPLOYED FLOOR, jittered: one second x 0.5-1.5")
                .singleElement()
                .satisfies(wait -> assertThat(wait.toMillis()).isBetween(500L, 1_500L));
    }

    @Test
    void aSurfacedFailureStartsItsNextRoundAtTheFloorAgain() throws Exception {
        RecordingSleeper sleeper = new RecordingSleeper();
        SegmentFetchRetry three = new SegmentFetchRetry(Duration.ofSeconds(1),
                Duration.ofSeconds(30), 3, sleeper);
        try (ConsumerClient c = new ConsumerClient(KEY, 16,
                failingThen(Integer.MAX_VALUE, new byte[0], new AtomicInteger()), three)) {
            c.deliver(proxied("bins/c/data/seg.bseg", 40));
            int surfaced = 0;
            for (int poll = 0; poll < 20 && surfaced < 1; poll++) {
                try {
                    c.readNext(Duration.ofHours(3));
                } catch (RuntimeException failure) {
                    surfaced++;
                }
            }
            assertThat(surfaced).as("the premise: the policy's three attempts surfaced")
                    .isEqualTo(1);
            int waitsBefore = sleeper.waits.size();
            for (int poll = 0; poll < 3 && sleeper.waits.size() == waitsBefore; poll++) {
                c.readNext(Duration.ofHours(3));
            }

            assertThat(sleeper.waits).as("the first round waited floor, then 2 x floor")
                    .hasSizeGreaterThan(waitsBefore);
            assertThat(sleeper.waits.get(waitsBefore).toMillis())
                    .as("⚠️ THE NEXT ROUND STARTS AT THE FLOOR, not where the last one grew to")
                    .isBetween(500L, 1_500L);
        }
    }

    @Test
    void twoReadersNeverBothFetchTheCatchUpHead() throws Exception {
        List<String> fetched = new CopyOnWriteArrayList<>();
        CyclicBarrier both = new CyclicBarrier(2);
        byte[] s0 = segmentOf("a", 0);
        byte[] s1 = segmentOf("b", 1);
        SegmentSource source = new SegmentSource() {
            @Override public byte[] fetch(Grant grant) {
                throw new AssertionError("a proxied delivery is fetched by key");
            }

            @Override public byte[] fetchSegment(String segmentKey) {
                fetched.add(segmentKey);
                // ⚠️ WAITS FOR A SECOND FETCH, which only a second reader
                // peeking the SAME head could start: with the lock it never
                // comes and this times out, without it both meet here.
                try {
                    both.await(2, TimeUnit.SECONDS);
                } catch (Exception alone) {
                    // the expected outcome with the lock held
                }
                return segmentKey.equals("s0") ? s0 : s1;
            }
        };
        UUID request = UUID.randomUUID();
        try (ConsumerClient c = new ConsumerClient(KEY, 16, source, SegmentFetchRetry.DEFAULT)) {
            c.beginCatchUp(request);
            c.deliverCatchUp(request, proxied("s0", 0));
            c.deliverCatchUp(request, proxied("s1", 1));
            List<Throwable> failures = new CopyOnWriteArrayList<>();
            Runnable reader = () -> {
                try {
                    c.readNext(Duration.ZERO);
                } catch (Throwable t) {
                    failures.add(t);
                }
            };
            Thread first = Thread.ofPlatform().start(reader);
            Thread second = Thread.ofPlatform().start(reader);
            first.join(10_000);
            second.join(10_000);

            assertThat(failures).isEmpty();
        }
        assertThat(fetched).as("⚠️ ONE DECODE OF THE HEAD AT A TIME: s0 once, then s1")
                .containsExactly("s0", "s1");
    }
}
