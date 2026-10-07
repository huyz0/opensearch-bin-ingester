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
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * M12.12 (H13, carried from M10.28): the live and catch-up lanes interleave
 * (a live-service quantum), and one {@link SegmentFetcher} held the retry
 * state of both, so a catch-up segment that failed deferred every LIVE fetch
 * behind its backoff. Each lane now has its own; the attempts per failing
 * segment are the same {@code maxAttempts} as before.
 */
class ConsumerLaneRetryTest {

    private static final RunKey KEY = new RunKey(
            UUID.fromString("00000000-0000-0000-0000-0000000000cd"), 0);
    private static final String CATCH_UP = "bins/c/data/catch-up.bseg";
    private static final String LIVE = "bins/c/data/live.bseg";

    /** Fails the catch-up segment every time; serves the live one. */
    private static final class OneFails implements SegmentSource {
        final Map<String, AtomicInteger> asked = new ConcurrentHashMap<>();
        final List<String> order = new CopyOnWriteArrayList<>();
        private final byte[] live;

        OneFails(byte[] live) {
            this.live = live;
        }

        @Override
        public byte[] fetch(Grant grant) throws IOException {
            return fetchSegment(grant.url());
        }

        @Override
        public byte[] fetchSegment(String segmentKey) throws IOException {
            asked.computeIfAbsent(segmentKey, k -> new AtomicInteger()).incrementAndGet();
            order.add(segmentKey);
            if (segmentKey.equals(CATCH_UP)) {
                throw new IOException("502 from the ingester");
            }
            return live;
        }
    }

    @Test
    void aFailingCatchUpSegmentDoesNotDeferTheLiveOne() throws Exception {
        SegmentFetchRetry retry = TestRetries.sleepAdvanced(Duration.ofSeconds(1),
                Duration.ofSeconds(30), 8, wait -> { });
        OneFails source = new OneFails(segmentOf("live-doc", 5));
        try (ConsumerClient client = new ConsumerClient(KEY, 16, source, retry)) {
            UUID request = UUID.randomUUID();
            client.beginCatchUp(request);
            client.deliverCatchUp(request, new Delivery(KEY, CATCH_UP, 1, 0, FetchMode.PROXY,
                    new byte[0]));
            assertThat(client.readNext(Duration.ZERO)).as("the catch-up fetch fails").isEmpty();
            assertThat(source.asked).containsKey(CATCH_UP);

            client.deliver(new Delivery(KEY, LIVE, 1, 5, FetchMode.PROXY, new byte[0]));
            Optional<ConsumerRecord> read = Optional.empty();
            for (int poll = 0; poll < 3 && read.isEmpty(); poll++) {
                read = client.readNext(Duration.ZERO);
            }

            assertThat(source.asked).as("⚠️ THE LIVE SEGMENT IS FETCHED, not deferred behind "
                    + "the catch-up's backoff").containsKey(LIVE);
            assertThat(read).as("and its record read").isPresent();
            assertThat(read.get().record().id()).isEqualTo("live-doc");
        }
    }

    /**
     * ⚠️ THE ATTEMPTS PER FAILING SEGMENT DO NOT RISE with a fetcher per lane
     * (SPEC's H13 cost row): the catch-up segment is fetched {@code maxAttempts}
     * times and then surfaces, as it was with one fetcher.
     */
    @Test
    void aFailingCatchUpSegmentIsFetchedExactlyMaxAttemptsTimes() throws Exception {
        SegmentFetchRetry retry = TestRetries.sleepAdvanced(Duration.ofSeconds(1),
                Duration.ofSeconds(30), 8, wait -> { });
        OneFails source = new OneFails(segmentOf("live-doc", 5));
        try (ConsumerClient client = new ConsumerClient(KEY, 16, source, retry)) {
            UUID request = UUID.randomUUID();
            client.beginCatchUp(request);
            client.deliverCatchUp(request, new Delivery(KEY, CATCH_UP, 1, 0, FetchMode.PROXY,
                    new byte[0]));
            boolean surfaced = false;
            for (int poll = 0; poll < 40 && !surfaced; poll++) {
                try {
                    client.readNext(Duration.ofMinutes(1)); // the no-op sleeper serves backoffs
                } catch (RuntimeException expected) {
                    surfaced = true;
                }
            }

            assertThat(surfaced).as("the budget is spent and the failure surfaces").isTrue();
            assertThat(source.asked.get(CATCH_UP)).as("⚠️ maxAttempts fetches, no more")
                    .hasValue(8);
        }
    }

    /**
     * ⚠️ UP TO TWICE ACROSS BOTH LANES, AND NO MORE (M13.12, M12.12 review T2):
     * each lane spends its own {@code maxAttempts}. With a catch-up segment
     * that has failed {@code maxAttempts - 1} times, a failing live segment
     * still surfaces at exactly its own {@code maxAttempts}, not at what a
     * shared budget has left; a catch-up failure that surfaces along the way
     * does so at exactly its own; and neither lane is fetched more than its
     * budget before it surfaces, so the window is at most 2 x maxAttempts.
     *
     * <p>⚠️ WHICHEVER LANE THE QUEUE SERVES (M13.12 review T2): since M13.47 a
     * due catch-up takes the turn of a backing-off live head, where before the
     * catch-up waited. Both are within the budgets, so this counts per lane at
     * each lane's own surfacing, not in total.
     */
    @Test
    void eachLaneSpendsItsOwnMaxAttemptsBeforeAFailureSurfaces() throws Exception {
        SegmentFetchRetry retry = TestRetries.sleepAdvanced(Duration.ofSeconds(1),
                Duration.ofSeconds(30), 8, wait -> { });
        BothFail source = new BothFail();
        try (ConsumerClient client = new ConsumerClient(KEY, 16, source, retry)) {
            UUID request = UUID.randomUUID();
            client.beginCatchUp(request);
            client.deliverCatchUp(request, new Delivery(KEY, CATCH_UP, 1, 0, FetchMode.PROXY,
                    new byte[0]));
            for (int poll = 0; poll < 40 && source.count(CATCH_UP) < 7; poll++) {
                assertThat(client.readNext(Duration.ofMinutes(1)))
                        .as("the catch-up's failures, inside its budget").isEmpty();
            }
            assertThat(source.count(CATCH_UP)).as("the premise: catch-up failed 7 times")
                    .isEqualTo(7);

            client.deliver(new Delivery(KEY, LIVE, 1, 5, FetchMode.PROXY, new byte[0]));
            Integer liveAtItsSurfacing = null;
            Integer catchUpAtItsSurfacing = null;
            for (int poll = 0; poll < 80 && liveAtItsSurfacing == null; poll++) {
                try {
                    client.readNext(Duration.ofMinutes(1)); // the no-op sleeper serves backoffs
                } catch (RuntimeException failure) {
                    String named = String.valueOf(failure.getMessage())
                            + (failure.getCause() == null ? "" : failure.getCause().getMessage());
                    if (named.contains(LIVE)) {
                        liveAtItsSurfacing = source.count(LIVE);
                    } else if (named.contains(CATCH_UP) && catchUpAtItsSurfacing == null) {
                        catchUpAtItsSurfacing = source.count(CATCH_UP);
                    }
                }
                assertThat(source.count(LIVE)).as("live never past its own budget unsurfaced")
                        .isLessThanOrEqualTo(8);
            }

            assertThat(liveAtItsSurfacing).as("⚠️ LIVE SURFACES AT ITS OWN maxAttempts, not at"
                    + " the one a shared budget has left").isEqualTo(8);
            if (catchUpAtItsSurfacing != null) {
                assertThat(catchUpAtItsSurfacing).as("and catch-up, if it surfaced, at its own")
                        .isEqualTo(8);
            } else {
                assertThat(source.count(CATCH_UP))
                        .as("catch-up, unsurfaced, never past its own budget")
                        .isLessThanOrEqualTo(8);
            }
        }
    }

    /** Fails both segments every time. */
    private static final class BothFail implements SegmentSource {
        final Map<String, AtomicInteger> asked = new ConcurrentHashMap<>();

        int count(String segmentKey) {
            AtomicInteger n = asked.get(segmentKey);
            return n == null ? 0 : n.get();
        }

        @Override
        public byte[] fetch(Grant grant) throws IOException {
            return fetchSegment(grant.url());
        }

        @Override
        public byte[] fetchSegment(String segmentKey) throws IOException {
            asked.computeIfAbsent(segmentKey, k -> new AtomicInteger()).incrementAndGet();
            throw new IOException("502 from the ingester");
        }
    }

    private static byte[] segmentOf(String id, long offset) throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(KEY, new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                ("{\"id\":\"" + id + "\"}").getBytes(StandardCharsets.UTF_8)), offset);
        return w.toByteArray(offset);
    }
}
