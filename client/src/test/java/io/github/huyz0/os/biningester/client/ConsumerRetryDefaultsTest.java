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
        SegmentFetchRetry deployed = TestRetries.sleepAdvanced(HttpSubscriptionTransport.DEFAULT_RETRY_FLOOR,
                HttpSubscriptionTransport.DEFAULT_RETRY_CEILING, SegmentFetchRetry.DEFAULT_MAX_ATTEMPTS,
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
        SegmentFetchRetry three = TestRetries.sleepAdvanced(Duration.ofSeconds(1),
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
        Thread[] readers = new Thread[2];
        byte[] s0 = segmentOf("a", 0);
        byte[] s1 = segmentOf("b", 1);
        SegmentSource source = new SegmentSource() {
            @Override public byte[] fetch(Grant grant) {
                throw new AssertionError("a proxied delivery is fetched by key");
            }

            @Override public byte[] fetchSegment(String segmentKey) {
                fetched.add(segmentKey);
                if (segmentKey.equals("s0")) {
                    holdUntilTheOtherReaderIsBlockedOnMeOrFetching(readers, fetched);
                }
                return segmentKey.equals("s0") ? s0 : s1;
            }
        };
        UUID request = UUID.randomUUID();
        try (ConsumerClient c = new ConsumerClient(KEY, 16, source, TestRetries.noFailedFetch())) {
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
            Thread first = Thread.ofPlatform().unstarted(reader);
            Thread second = Thread.ofPlatform().unstarted(reader);
            readers[0] = first;
            readers[1] = second;
            first.start();
            second.start();
            first.join(10_000);
            second.join(10_000);

            assertThat(failures).isEmpty();
        }
        assertThat(fetched).as("⚠️ ONE DECODE OF THE HEAD AT A TIME: s0 once, then s1")
                .containsExactly("s0", "s1");
    }

    /**
     * Holds the head's fetch until the other reader is either BLOCKED on a
     * monitor this thread owns -- the decode lock, with it held -- or has
     * started a fetch of its own, which only a second reader peeking the SAME
     * head could, without it.
     *
     * <p>⚠️ NOT A FIXED WAIT (M12.21, M11.13 T5): the barrier this replaced
     * timed out after 2 s on EVERY passing run, because with the lock held the
     * second fetch it waited for never comes. The other reader finishing
     * without contending also releases it; 10 s is only the failure's bound.
     */
    private static void holdUntilTheOtherReaderIsBlockedOnMeOrFetching(Thread[] readers,
            List<String> fetched) {
        Thread me = Thread.currentThread();
        Thread other = readers[0] == me ? readers[1] : readers[0];
        java.lang.management.ThreadMXBean threads =
                java.lang.management.ManagementFactory.getThreadMXBean();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (fetched.size() >= 2 || other.getState() == Thread.State.TERMINATED) {
                return;
            }
            java.lang.management.ThreadInfo info = threads.getThreadInfo(other.threadId());
            if (info != null && info.getThreadState() == Thread.State.BLOCKED
                    && info.getLockOwnerId() == me.threadId()) {
                return;
            }
            Thread.onSpinWait();
        }
    }
}
