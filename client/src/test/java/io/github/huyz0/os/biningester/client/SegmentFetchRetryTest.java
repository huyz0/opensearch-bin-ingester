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
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * A failed segment fetch is RETRIED inside {@code readNext}, with bounded
 * backoff, rather than thrown (M10.23, research 02 §6).
 *
 * <p>⚠️ THE THROW WAS THE DEFECT: any exception out of {@code readNext} pauses
 * that shard until an operator resumes it, so before this a {@code 502} from a
 * restarting ingester cost a shard. ⚠️ AND NOTHING HERE WAITS: the backoff
 * goes through an injected sleeper that only records, so a case asserts what
 * was waited for instead of spending it.
 */
class SegmentFetchRetryTest {

    private static final RunKey KEY = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000cd"), 0);
    private static final String SEGMENT = "bins/c/data/the-retried.bseg";
    private static final Grant GRANT =
            new Grant("https://store.example/the-retried", Instant.ofEpochMilli(60_000L));
    /**
     * Long enough to serve a first backoff (at most 1.5 s) in one call.
     *
     * <p>⚠️ NOT MINUTES: a poll over an EMPTY queue still blocks for real, so
     * a regression that skipped a delivery would otherwise hang a case for as
     * long as it polls rather than fail it.
     */
    private static final Duration LONG_POLL = Duration.ofSeconds(5);
    private static final Duration SHORT_POLL = Duration.ofMillis(100);

    /** Records every wait and never waits. */
    private static final class RecordingSleeper implements SegmentFetchRetry.Sleeper {
        final List<Duration> waits = new CopyOnWriteArrayList<>();

        @Override public void sleep(Duration wait) {
            waits.add(wait);
        }

        Duration total() {
            return waits.stream().reduce(Duration.ZERO, Duration::plus);
        }
    }

    /** Fails its first {@code failures} fetches, by key or by grant, then serves. */
    private static final class FailsThenServes implements SegmentSource {
        private final byte[] bytes;
        private final int failures;
        private final String why;
        final AtomicInteger attempts = new AtomicInteger();

        FailsThenServes(byte[] bytes, int failures, String why) {
            this.bytes = bytes;
            this.failures = failures;
            this.why = why;
        }

        @Override public byte[] fetch(Grant grant) throws IOException {
            return answer();
        }

        @Override public byte[] fetchSegment(String segmentKey) throws IOException {
            return answer();
        }

        private byte[] answer() throws IOException {
            if (attempts.incrementAndGet() <= failures) {
                throw new IOException("proxied segment " + SEGMENT + " " + why);
            }
            return bytes;
        }
    }

    /** Answers each fetch from a script: {@code true} serves, {@code false} answers 502. */
    private static final class Scripted implements SegmentSource {
        private final byte[] bytes;
        private final java.util.Deque<Boolean> script;
        final AtomicInteger attempts = new AtomicInteger();

        Scripted(byte[] bytes, Boolean... script) {
            this.bytes = bytes;
            this.script = new java.util.ArrayDeque<>(List.of(script));
        }

        @Override public byte[] fetch(Grant grant) throws IOException {
            return fetchSegment(grant.url());
        }

        @Override public byte[] fetchSegment(String segmentKey) throws IOException {
            attempts.incrementAndGet();
            Boolean serve = script.poll();
            if (serve == null || !serve) {
                throw new IOException("proxied segment " + segmentKey + " answered 502");
            }
            return bytes;
        }
    }

    private static byte[] segmentOf(String... ids) throws Exception {
        SegmentWriter w = new SegmentWriter();
        for (String id : ids) {
            w.add(KEY, new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                    ("{\"id\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8)), 1L);
        }
        return w.toByteArray(1L);
    }

    private static Delivery proxied(long firstOffset, int recordCount) {
        return new Delivery(KEY, SEGMENT, recordCount, firstOffset, FetchMode.PROXY,
                new byte[0]);
    }

    private static SegmentFetchRetry retry(int maxAttempts, RecordingSleeper sleeper) {
        return TestRetries.sleepAdvanced(Duration.ofSeconds(1), Duration.ofSeconds(30),
                maxAttempts, sleeper);
    }

    /** Polls until {@code wanted} records arrived, or gives up after a bounded number. */
    private static List<ConsumerRecord> read(ConsumerClient c, int wanted) throws Exception {
        List<ConsumerRecord> out = new ArrayList<>();
        for (int poll = 0; poll < 300 && out.size() < wanted; poll++) {
            c.readNext(SHORT_POLL).ifPresent(out::add);
        }
        return out;
    }

    @Test
    void aTransient502ThenSuccessYieldsTheRecordsInOrder() throws Exception {
        RecordingSleeper sleeper = new RecordingSleeper();
        FailsThenServes source = new FailsThenServes(segmentOf("a", "b"), 1, "answered 502");
        try (ConsumerClient c = new ConsumerClient(KEY, 16, source, retry(8, sleeper))) {
            c.deliver(proxied(40, 2));
            c.deliver(new Delivery(KEY, "seg-inline", 1, 42, FetchMode.INLINE, segmentOf("c")));

            List<ConsumerRecord> records = read(c, 3);

            // ⚠️ OFFSET 40 IS THE ONE THAT FAILED. A retry that advanced the
            // expected offset before the fetch would read the retried delivery
            // as a duplicate and hand out 42 alone -- a skipped window.
            assertThat(records).extracting(ConsumerRecord::offset).containsExactly(40L, 41L, 42L);
            assertThat(records).extracting(r -> r.record().id()).containsExactly("a", "b", "c");
        }
        assertThat(source.attempts).as("the failed fetch, then its one retry").hasValue(2);
        assertThat(sleeper.total())
                .as("the retry waited a jittered floor first: at least half of one second")
                .isGreaterThanOrEqualTo(Duration.ofMillis(500));
    }

    @Test
    void anUnreachableSourceOnDIRECTIsRetriedTheSameWay() throws Exception {
        RecordingSleeper sleeper = new RecordingSleeper();
        FailsThenServes source = new FailsThenServes(segmentOf("d"), 2, "is unreachable");
        try (ConsumerClient c = new ConsumerClient(KEY, 16, source, retry(8, sleeper))) {
            c.deliver(new Delivery(KEY, "seg-direct", 1, 7, FetchMode.DIRECT, new byte[0], GRANT));

            assertThat(read(c, 1)).extracting(ConsumerRecord::offset).containsExactly(7L);
        }
        assertThat(source.attempts).hasValue(3);
    }

    /**
     * A failure that outlasts the policy SURFACES, after exactly
     * {@code maxAttempts} fetches, and the delivery is not skipped.
     *
     * <p>⚠️ BOTH HALVES MATTER. Retrying for ever is a shard that stalls out of
     * sight -- the failure mode the pause exists to make visible -- and
     * surfacing by DROPPING the delivery would resume past records nobody read.
     */
    @Test
    void aPersistentFailureIsBoundedAndSurfaced() throws Exception {
        RecordingSleeper sleeper = new RecordingSleeper();
        FailsThenServes source =
                new FailsThenServes(segmentOf("a"), Integer.MAX_VALUE, "answered 502");
        try (ConsumerClient c = new ConsumerClient(KEY, 16, source, retry(3, sleeper))) {
            c.deliver(proxied(40, 1));

            UncheckedIOException surfaced = null;
            for (int poll = 0; poll < 300 && surfaced == null; poll++) {
                try {
                    assertThat(c.readNext(SHORT_POLL)).isEmpty();
                } catch (UncheckedIOException e) {
                    surfaced = e;
                }
            }

            assertThat(surfaced).as("surfaced, not retried for ever").isNotNull();
            assertThat(surfaced).hasMessageContaining(SEGMENT).hasMessageContaining("502");
            assertThat(source.attempts).as("exactly the policy's attempts").hasValue(3);
            // ⚠️ TWO BACKOFFS, jittered to [0.5, 1.5] s then [1, 3] s, served
            // in slices of the poll timeout -- and none after the last attempt.
            assertThat(sleeper.total()).as("a backoff before each retry, none after the last")
                    .isBetween(Duration.ofMillis(1_500), Duration.ofMillis(4_500));
            assertThat(sleeper.waits).allMatch(w -> w.compareTo(SHORT_POLL) <= 0);
        }
    }

    /**
     * ⚠️ THE SURFACED DELIVERY IS STILL AT THE HEAD: resumed once the source
     * heals, the shard reads offset 40, not whatever was queued behind it.
     */
    @Test
    void aSurfacedDeliveryIsReadOnceTheSourceHeals() throws Exception {
        RecordingSleeper sleeper = new RecordingSleeper();
        FailsThenServes source = new FailsThenServes(segmentOf("a"), 2, "answered 502");
        try (ConsumerClient c = new ConsumerClient(KEY, 16, source, retry(2, sleeper))) {
            c.deliver(proxied(40, 1));
            c.deliver(new Delivery(KEY, "seg-inline", 1, 41, FetchMode.INLINE, segmentOf("b")));

            assertThat(c.readNext(LONG_POLL)).isEmpty();
            assertThat(c.readNext(LONG_POLL)).as("the backoff is served").isEmpty();
            assertThatThrownBy(() -> c.readNext(LONG_POLL))
                    .isInstanceOf(UncheckedIOException.class);

            assertThat(read(c, 2)).as("the operator's resume reads 40 first, then 41")
                    .extracting(ConsumerRecord::offset).containsExactly(40L, 41L);
        }
    }

    /**
     * {@code readNext(ZERO)} neither blocks nor fetches while a backoff is owed.
     *
     * <p>⚠️ {@code BinStoreShardConsumer.drain} calls it with ZERO after its
     * first record, so a retry that slept here would stall every batch, and
     * one that fetched here would turn a caller's loop into a request loop.
     */
    @Test
    void readNextZERONeitherBlocksNorRefetchesDuringABackoff() throws Exception {
        RecordingSleeper sleeper = new RecordingSleeper();
        FailsThenServes source = new FailsThenServes(segmentOf("a"), 1, "answered 502");
        try (ConsumerClient c = new ConsumerClient(KEY, 16, source, retry(8, sleeper))) {
            c.deliver(proxied(40, 1));
            assertThat(c.readNext(LONG_POLL)).isEmpty();

            for (int i = 0; i < 100; i++) {
                assertThat(c.readNext(Duration.ZERO)).isEmpty();
            }
            assertThat(source.attempts).as("no fetch while the backoff is owed").hasValue(1);
            assertThat(sleeper.waits).as("and no wait on a zero timeout").isEmpty();

            assertThat(read(c, 1)).extracting(ConsumerRecord::offset).containsExactly(40L);
        }
    }

    /**
     * A wait never exceeds the caller's own timeout, however much backoff is owed.
     */
    @Test
    void aBackoffIsServedInSlicesNoLongerThanTheCallersTimeout() throws Exception {
        RecordingSleeper sleeper = new RecordingSleeper();
        FailsThenServes source = new FailsThenServes(segmentOf("a"), 1, "answered 502");
        try (ConsumerClient c = new ConsumerClient(KEY, 16, source, retry(8, sleeper))) {
            c.deliver(proxied(40, 1));
            assertThat(c.readNext(LONG_POLL)).isEmpty();

            Optional<ConsumerRecord> got = Optional.empty();
            for (int poll = 0; poll < 100 && got.isEmpty(); poll++) {
                got = c.readNext(SHORT_POLL);
            }
            assertThat(got).map(ConsumerRecord::offset).contains(40L);
        }
        assertThat(sleeper.waits).allMatch(w -> w.compareTo(SHORT_POLL) <= 0);
        assertThat(sleeper.total()).isGreaterThanOrEqualTo(Duration.ofMillis(500));
    }

    /**
     * A segment that fetched but will not decode surfaces AT ONCE -- no
     * retry, no backoff -- and stays at the head.
     *
     * <p>⚠️ THE SECOND THROW IS THE R3 FINDING (M10.2 review): the queue kept
     * the permits a throwing decode took, so the next poll found nothing to
     * read and returned an ordinary empty poll over a delivery still queued.
     */
    @Test
    void aCorruptSegmentSurfacesAtOnceAndIsNotRetriedOrSkipped() throws Exception {
        RecordingSleeper sleeper = new RecordingSleeper();
        FailsThenServes source = new FailsThenServes(new byte[] {1, 2, 3, 4}, 0, "unused");
        try (ConsumerClient c = new ConsumerClient(KEY, 16, source, retry(8, sleeper))) {
            c.deliver(proxied(40, 1));

            assertThatThrownBy(() -> c.readNext(SHORT_POLL))
                    .isInstanceOf(UncheckedIOException.class);
            assertThat(source.attempts).as("fetched once and not retried").hasValue(1);
            assertThat(sleeper.waits).isEmpty();

            assertThatThrownBy(() -> c.readNext(SHORT_POLL))
                    .as("still at the head, and still refused")
                    .isInstanceOf(UncheckedIOException.class);
        }
    }

    /**
     * A success RESETS the attempt count: with two attempts, a fail, a
     * success and a fail is a retry, not a surfaced failure.
     *
     * <p>⚠️ WITHOUT THE RESET the budget is per CLIENT LIFETIME rather than per
     * segment: a consumer that met one 502 an hour ago pauses its shard on the
     * next one, because its count never came back down.
     */
    @Test
    void aSuccessResetsTheAttemptCount() throws Exception {
        RecordingSleeper sleeper = new RecordingSleeper();
        Scripted source = new Scripted(segmentOf("a"), false, true, false, true);
        try (ConsumerClient c = new ConsumerClient(KEY, 16, source, retry(2, sleeper))) {
            c.deliver(proxied(40, 1));
            c.deliver(proxied(41, 1));

            assertThat(read(c, 2)).as("the second failure was retried, not surfaced")
                    .extracting(ConsumerRecord::offset).containsExactly(40L, 41L);
        }
        assertThat(source.attempts).hasValue(4);
    }

    /**
     * A success RESETS the backoff to the floor: three failures grow it to
     * 8 s, and the next segment's first failure waits a jittered 1 s again.
     */
    @Test
    void aSuccessRestartsTheBackoffAtTheFloor() throws Exception {
        RecordingSleeper sleeper = new RecordingSleeper();
        Scripted source = new Scripted(segmentOf("a"), false, false, false, true, false, true);
        try (ConsumerClient c = new ConsumerClient(KEY, 16, source, retry(8, sleeper))) {
            c.deliver(proxied(40, 1));
            assertThat(read(c, 1)).extracting(ConsumerRecord::offset).containsExactly(40L);
            int waitsBefore = sleeper.waits.size();

            c.deliver(proxied(41, 1));
            assertThat(read(c, 1)).extracting(ConsumerRecord::offset).containsExactly(41L);

            Duration second = sleeper.waits.subList(waitsBefore, sleeper.waits.size()).stream()
                    .reduce(Duration.ZERO, Duration::plus);
            // ⚠️ [0.5, 1.5] s from the floor; an unreset backoff of 8 s would
            // wait at least 4 s. The two ranges do not meet, whatever the jitter.
            assertThat(second).as("the next segment's wait restarts at the floor")
                    .isBetween(Duration.ofMillis(500), Duration.ofMillis(1_500));
        }
        assertThat(source.attempts).hasValue(6);
    }

    /**
     * The DEPLOYED policy retries: a client on the standard policy answers a
     * 502 with an empty poll, not a pause, and owes a backoff before it asks
     * again.
     *
     * <p>⚠️ THE STANDARD POLICY, which production builds every client on
     * (M13.6c: {@code NodeSubscriptions}, since no constructor picks its own):
     * a standard policy of one attempt would restore the pause on every 502
     * while every injected-policy case stayed green. ⚠️ AND NOTHING SLEEPS
     * (M11.13, H8; M10.23 review T8): the standard sleeper sleeps for real, so
     * this case polls with {@code ZERO} on a fixed clock, which neither waits
     * nor fetches while a backoff is owed; that the backoff is then served and
     * the records read is {@link #aTransient502ThenSuccessYieldsTheRecordsInOrder}'s.
     */
    @Test
    void aClientOnTheStandardPolicyRetriesATransientFailure() throws Exception {
        Scripted source = new Scripted(segmentOf("a"), false, true);
        try (ConsumerClient c = new ConsumerClient(KEY, 16, source,
                SegmentFetchRetry.standard(() -> 0L))) {
            c.deliver(proxied(40, 1));

            assertThat(c.readNext(Duration.ZERO)).as("a 502 is an empty poll, not a pause")
                    .isEmpty();
            for (int poll = 0; poll < 5; poll++) {
                assertThat(c.readNext(Duration.ZERO))
                        .as("a backoff is owed: no second fetch yet").isEmpty();
            }
        }
        assertThat(source.attempts).as("intentional: one fetch, its retry owed").hasValue(1);
    }
}
