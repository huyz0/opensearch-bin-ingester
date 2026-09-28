// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.LocalFsBinStore;
import io.github.huyz0.os.biningester.format.RunEntry;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentReader;
import io.github.huyz0.os.biningester.ingest.AppendResult;
import io.github.huyz0.os.biningester.ingest.DefaultIngest;
import io.github.huyz0.os.biningester.ingest.Ingest;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import io.github.huyz0.os.biningester.security.Principal;
import io.github.huyz0.os.biningester.sequencer.TestSequencers;
import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A high lane is committed sooner, END TO END (M10.9, ADR-0074, M10 criterion
 * 12): a producer's {@code lane=2} over HTTP, through {@code BulkService} and
 * {@code DefaultIngest}, into a {@link LocalFsBinStore}, is acknowledged after
 * at most {@code ceiling ÷ 4} of clock time with the interval at its ceiling,
 * and carries the lane −1 records already buffered in the same flush.
 *
 * <p>⚠️ END TO END IS THE POINT. {@code AccumulatorLaneTest} (M10.7) proves the
 * deadline on the accumulator alone; a lane dropped between the query
 * parameter and {@code Accumulator.add} ON THIS PATH -- {@code BulkService},
 * {@code DefaultIngest} -- passes that test and fails this one. ⚠️ Not on
 * every path: {@code RoutedIngest} is not on this one, and a lane dropped
 * there is {@code RoutedIngestLaneTest}'s to catch (M11.9, H4; M10.9 review
 * T1).
 *
 * <p>⚠️ THE CLOCK IS FROZEN AND ADVANCED, never slept on. Nothing is due while
 * it stands still, so which records share a flush is decided by the test, not
 * by a race; the flush loop re-reads the clock at least every quarter of the
 * floor, so an advance is noticed within ~62 ms of real time.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LaneOvertakeTest {

    private static final UUID LOGS = UUID.fromString("00000000-0000-4000-8000-00000000ab19");
    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs"));
    private static final int LOW = 1;
    private static final int HIGH = 2;
    private static final Duration CEILING = IngestConfig.DEFAULT_INTERVAL_CEILING;
    private static final Duration FLOOR = Duration.ofMillis(250);

    @TempDir
    Path root;

    private WebServer server;

    @AfterEach
    void stop() {
        if (server != null) {
            server.stop();
        }
    }

    /** ⚠️ ADVANCED, never slept on. */
    private static final class TestClock extends Clock {
        private final AtomicLong millis = new AtomicLong(1_700_000_000_000L);

        @Override
        public long millis() {
            return millis.get();
        }

        void advance(Duration d) {
            millis.addAndGet(d.toMillis());
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis());
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }

    /**
     * Passes the EXPLICIT-partition appends through unchanged, and says when a
     * partition's records have all been handed to the accumulator. ⚠️ Routed
     * appends are not forwarded -- they fall to {@code Ingest}'s defaults,
     * which refuse -- and this test writes explicit partitions only (M11.9,
     * H4; M10.9 review P1). {@code DefaultIngest} consumes
     * the source under its lock, record by record, into the active buffer, so
     * the source returning IS the records being buffered.
     *
     * <p>⚠️ KEYED BY PARTITION, NOT BY LANE, so a build that dropped the lane
     * upstream still reaches the assertion that names the failure instead of
     * hanging on a latch for a lane that never arrives.
     */
    private static final class BufferedWitness implements Ingest {
        private final DefaultIngest delegate;
        private final Map<Integer, CountDownLatch> buffered = new ConcurrentHashMap<>();

        BufferedWitness(DefaultIngest delegate) {
            this.delegate = delegate;
        }

        CountDownLatch of(int partition) {
            return buffered.computeIfAbsent(partition, p -> new CountDownLatch(1));
        }

        @Override
        public AppendResult append(Principal principal, String index, int partition,
                RecordSource records) throws IOException {
            return append(principal, index, partition, (byte) 0, records);
        }

        @Override
        public AppendResult append(Principal principal, String index, int partition, byte lane,
                RecordSource records) throws IOException {
            return delegate.append(principal, index, partition, lane, sink -> {
                records.forEachRecord(sink);
                of(partition).countDown();
            });
        }

        @Override
        public boolean acceptsLane(byte lane) {
            return delegate.acceptsLane(lane);
        }

        @Override
        public void close() {
            // the test closes the delegate itself
        }
    }

    private static String body(String prefix, int n) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < n; i++) {
            b.append("{\"index\":{\"_id\":\"").append(prefix).append(i).append("\"}}\n")
                    .append("{\"n\":").append(i).append("}\n");
        }
        return b.toString();
    }

    private static CompletableFuture<Integer> post(WebClient client, int partition,
            String lane, String body) {
        return CompletableFuture.supplyAsync(() -> {
            try (HttpClientResponse r = client.post("/logs/_bulk")
                    .queryParam("partition", Integer.toString(partition))
                    .queryParam("lane", lane).submit(body)) {
                return r.status().code();
            }
        });
    }

    private static void awaitPushes(List<SubscriptionHub.Push> seen, int n) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (seen.size() < n) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("only " + seen.size() + " of " + n + " pushes arrived");
            }
            Thread.onSpinWait();
        }
    }

    @Test
    void aPLUS2AppendIsAckedWithinTheCEILINGOver4AndCarriesTheBUFFEREDMinus1RecordsAtTheirOFFSETS()
            throws Exception {
        // ⚠️ A ZERO lengthen delay, so the first near-empty flush lengthens the
        // interval to the ceiling (the default 5 s), which is the state
        // criterion 12 is about: at the floor a +2 record has nothing to overtake.
        IngestConfig config = new IngestConfig(FLOOR, 8L << 20, "cluster-a",
                IngestConfig.DEFAULT_MAX_QUEUED_PUSH_BYTES, CEILING,
                IngestConfig.DEFAULT_FILL_RATIO_LOW_THRESHOLD,
                IngestConfig.DEFAULT_FILL_RATIO_HIGH_THRESHOLD, Duration.ZERO,
                IngestConfig.DEFAULT_INTERVAL_SHORTEN_DELAY);
        TestClock clock = new TestClock();
        LocalFsBinStore store = new LocalFsBinStore(root);
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> low = new CopyOnWriteArrayList<>();
        List<SubscriptionHub.Push> high = new CopyOnWriteArrayList<>();
        try (var lowSub = hub.subscribe(new RunKey(LOGS, LOW), SubscriptionHub.assembling(low::add));
                var highSub = hub.subscribe(new RunKey(LOGS, HIGH),
                        SubscriptionHub.assembling(high::add));
                DefaultIngest ingest = new DefaultIngest(config, store, "bins/cluster-a", "pod1",
                        TestSequencers.leased(store, "bins/cluster-a", "pod1"), hub, clock,
                        index -> LOGS, ignored -> { }, new IndexCostLedger())) {
            BufferedWitness witness = new BufferedWitness(ingest);
            server = WebServer.builder().port(0)
                    .routing(HttpRouting.builder().register(new BulkService(witness, PRINCIPAL)))
                    .build().start();
            WebClient client = WebClient.builder()
                    .baseUri("http://localhost:" + server.port()).build();

            // ---- warm-up: one lane-0 record on the LOW partition, flushed by
            // advancing the clock one floor. It lengthens the interval, and it
            // takes offset 0, so the −1 records' original offsets start at 1.
            CompletableFuture<Integer> warm = post(client, LOW, "0", body("w", 1));
            assertThat(witness.of(LOW).await(10, TimeUnit.SECONDS)).isTrue();
            clock.advance(FLOOR);
            assertThat(warm.get(10, TimeUnit.SECONDS)).isEqualTo(202);
            awaitPushes(low, 1);
            assertThat(ingest.flushSpacingMillis())
                    .as("PREMISE: the interval is lengthened to the ceiling")
                    .isEqualTo(CEILING.toMillis());
            low.clear();
            witness.buffered.clear();

            // ---- lane −1 records buffered, then a lane +2 append, at ONE instant.
            CompletableFuture<Integer> minus1 = post(client, LOW, "-1", body("low", 3));
            assertThat(witness.of(LOW).await(10, TimeUnit.SECONDS))
                    .as("PREMISE: the −1 records are buffered").isTrue();
            CompletableFuture<Integer> plus2 = post(client, HIGH, "2", body("high", 1));
            assertThat(witness.of(HIGH).await(10, TimeUnit.SECONDS))
                    .as("PREMISE: the +2 record is buffered").isTrue();

            // ---- ceiling ÷ 4 of clock time, and no more.
            // ⚠️ -1 for "no answer in 10 s of REAL time" rather than a
            // TimeoutException, so a lane-ignoring build fails on the message
            // below. The clock does not move again, so no later wait helps it.
            clock.advance(CEILING.dividedBy(4));
            assertThat(plus2.completeOnTimeout(-1, 10, TimeUnit.SECONDS).join())
                    .as("the +2 append is acknowledged after ceiling ÷ 4 of clock time; "
                            + "a lane-ignoring build waits the full ceiling")
                    .isEqualTo(202);
            assertThat(minus1.get(10, TimeUnit.SECONDS))
                    .as("the buffered −1 records ride the same flush").isEqualTo(202);

            awaitPushes(low, 1);
            awaitPushes(high, 1);
            assertThat(low.get(0).segmentKey())
                    .as("ONE flush: the −1 run and the +2 run are in the same segment")
                    .isEqualTo(high.get(0).segmentKey());
            assertThat(low.get(0).firstOffset())
                    .as("the −1 records keep their original offsets, after the warm-up's 0")
                    .isEqualTo(1L);
            assertThat(low.get(0).recordCount()).isEqualTo(3);

            // ⚠️ READ BACK from the store: the lanes the producer named are the
            // lanes the committed segment carries.
            byte[] segment;
            try (InputStream in = store.get(high.get(0).segmentKey())) {
                segment = in.readAllBytes();
            }
            assertThat(SegmentReader.open(segment).directory().stream()
                    .collect(Collectors.toMap(RunEntry::key, RunEntry::lane)))
                    .containsEntry(new RunKey(LOGS, LOW), (byte) -1)
                    .containsEntry(new RunKey(LOGS, HIGH), (byte) 2);
        }
    }
}
