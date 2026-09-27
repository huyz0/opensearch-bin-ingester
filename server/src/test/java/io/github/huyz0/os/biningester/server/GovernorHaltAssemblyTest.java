// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CostGovernor;
import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentFormat;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import io.github.huyz0.os.biningester.http.EndpointSliceView;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.security.Principal;
import io.github.huyz0.os.biningester.sequencer.ChainBackfill;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Inbox;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The assembled node hands the governor's halt and the ingest's spacing to
 * the pieces that need them (M10.11, ADR-0075, criterion 14).
 *
 * <p>⚠️ **THE HALT IS REACHED THE PRODUCTION WAY**: data PUTs recorded against
 * the root's own governor, far past the spacing in force, and the window
 * rolled. A governor built halted by a test seam would prove the seam, not the
 * wiring; these cases red when the root hands either consumer {@code () ->
 * true}, or leaves the spacing supplier unattached.
 */
@Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class GovernorHaltAssemblyTest {

    private static final String PREFIX = "bins/cluster-a";
    private static final String INDEX = "logs";

    /** Real time plus an offset a case moves forward: see {@code GovernorAssemblyTest}. */
    private static final class OffsetClock extends Clock {
        private volatile Duration offset = Duration.ZERO;

        void advance(Duration by) {
            offset = offset.plus(by);
        }

        @Override
        public Instant instant() {
            return Instant.now().plus(offset);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }

    /**
     * ⚠️ A DAY-LONG LEASE AND PASS INTERVAL, so hours of clock do not expire
     * the term and the root's scheduler never ticks the loop a case ticks.
     */
    private static ServerConfig config(String pod, String az, IngestConfig ingest) {
        return new ServerConfig(pod, az, "cluster-a", PREFIX,
                new StoreConfig("memory", Optional.empty()),
                Duration.ofDays(1), Duration.ofSeconds(3), "http://" + pod + ":8080",
                ingest, 0, "producer-1", Set.of(INDEX),
                new RetentionConfig(Duration.ofMinutes(1), Duration.ofHours(2),
                        Duration.ofSeconds(10), Duration.ofHours(3), Duration.ofDays(1)));
    }

    private static SequencerTransport noPeers() {
        return new SequencerTransport() {
            @Override
            public CommitDelta send(String endpoint, CommitRequest request) {
                throw new UnsupportedOperationException("no peer expected: " + endpoint);
            }

            @Override
            public void close() {
            }
        };
    }

    /**
     * Drives the root's governor past {@link CostGovernor#HALT}: ~20× the data
     * PUTs the 250 ms floor expects in a window, then the window rolled.
     *
     * <p>⚠️ RETRIED, NOT TIMED: a burst that straddles a window boundary is
     * split across two windows and may rate under the halt, so it is recorded
     * again rather than trusted.
     */
    private static void halt(CostGovernor governor, OffsetClock clock) {
        for (int attempt = 0; attempt < 3 && governor.lastRatio() < CostGovernor.HALT;
                attempt++) {
            for (int i = 0; i < 5000; i++) {
                governor.recordDataPut(1);
            }
            clock.advance(Duration.ofSeconds(60));
        }
        assertThat(governor.lastRatio())
                .as("the premise: discretionary work is halted, the kill switch is not")
                .isGreaterThanOrEqualTo(CostGovernor.HALT).isLessThan(CostGovernor.KILL);
    }

    private static String storeSegment(ObservedStore store) throws Exception {
        SegmentWriter writer = new SegmentWriter();
        writer.add(new RunKey(UUID.randomUUID(), 0), new SegmentRecord("doc-1", OpType.INDEX,
                OptionalLong.empty(), "payload".getBytes(StandardCharsets.UTF_8)), 1L);
        String key = new SegmentKey(PREFIX, 1L, "writera", 0L,
                SegmentFormat.DIRECTORY_ENTRY_BYTES).key();
        store.put(key, Body.ofBytes(writer.toByteArray(1L)));
        return key;
    }

    @Test
    void aHALTEDNodesPrefetcherIssuesNOGetForASegmentItOWNS() throws Exception {
        OffsetClock clock = new OffsetClock();
        EndpointSliceView view = new EndpointSliceView();
        // ⚠️ THIS POD IS ITS AZ's ONLY READY ENDPOINT, so it owns every segment
        // another AZ writes: every other reason not to fetch is ruled out.
        view.apply("{\"type\":\"ADDED\",\"object\":{\"metadata\":{\"name\":\"ingesters\"},"
                + "\"endpoints\":[{\"addresses\":[\"owner1\"],\"zone\":\"az-b\","
                + "\"conditions\":{\"ready\":true},\"targetRef\":{\"name\":\"owner1\"}}]}}");
        try (ObservedStore shared = new ObservedStore(Inbox.prefixFor(PREFIX));
                Assembly assembly = Assembly.open(
                        config("owner1", "az-b", IngestConfig.defaults("cluster-a")), shared,
                        noPeers(), clock, view, new CrossAzBytes("az-b"))) {
            String segment = storeSegment(shared);
            halt(assembly.governor(), clock);

            assembly.prefetchDurableSegment(segment, "az-a");

            assertThat(shared.getsOf(segment))
                    .as("⚠️ NO GET WHILE DISCRETIONARY WORK IS HALTED: the root handed the "
                            + "prefetcher the governor's halt")
                    .isZero();
        }
    }

    @Test
    void aHALTEDNodesRetentionPassDEFERSWithNoSweepLIST() throws Exception {
        OffsetClock clock = new OffsetClock();
        try (ObservedStore shared = new ObservedStore(Inbox.prefixFor(PREFIX));
                Assembly assembly = Assembly.open(
                        config("pod1", "az-a", IngestConfig.defaults("cluster-a")), shared,
                        noPeers(), clock)) {
            assembly.retention().tick(); // the term is first seen now
            // ⚠️ HOURS ON, so hours are past their grace and the sweep is due.
            clock.advance(Duration.ofHours(4));
            halt(assembly.governor(), clock);
            long refusedBefore = assembly.governor().counts().discretionaryRefusals();
            long termTicksBefore = assembly.retention().termTicks();

            assembly.retention().tick();

            assertThat(assembly.retention().termTicks() - termTicksBefore)
                    .as("the premise: the tick found the live term").isEqualTo(1);
            assertThat(shared.listsUnder(PREFIX + "/data/"))
                    .as("⚠️ THE PASS DEFERRED: no hour of data/ was listed while halted")
                    .isZero();
            assertThat(assembly.governor().counts().discretionaryRefusals() - refusedBefore)
                    .as("and the loop asked the root's governor, which counted the refusal")
                    .isEqualTo(1);
        }
    }

    @Test
    void theROOTsGovernorReadsTheINGESTsLengthenedSpacingNotTheFLOOR() throws Exception {
        // ⚠️ A ZERO LENGTHEN DELAY: one near-empty flush lengthens the interval
        // to the 5 s ceiling at once.
        IngestConfig lengthens = new IngestConfig(Duration.ofMillis(250), 8L << 20, "cluster-a",
                IngestConfig.DEFAULT_MAX_QUEUED_PUSH_BYTES, Duration.ofSeconds(5),
                IngestConfig.DEFAULT_FILL_RATIO_LOW_THRESHOLD,
                IngestConfig.DEFAULT_FILL_RATIO_HIGH_THRESHOLD, Duration.ZERO,
                IngestConfig.DEFAULT_INTERVAL_SHORTEN_DELAY);
        AtomicReference<LongSupplier> spacing = new AtomicReference<>();
        try (ObservedStore shared = new ObservedStore(Inbox.prefixFor(PREFIX));
                Assembly assembly = Assembly.openForTest(config("pod1", "az-a", lengthens),
                        shared, noPeers(), Clock.systemUTC(), ChainBackfill::inBackground,
                        (c, clock, supplied) -> {
                            spacing.set(supplied);
                            return GovernorWiring.DEFAULT.create(c, clock, supplied);
                        })) {
            String uuid = java.util.Base64.getUrlEncoder().withoutPadding()
                    .encodeToString(new byte[16]);
            assembly.catalog().register(new IndexRegistration(uuid, INDEX, List.of(), 4, 4, 1, 1));
            assembly.ingest().append(new Principal("cluster-a", "producer-1", Set.of(INDEX)),
                    INDEX, 0, sink -> sink.accept(new SegmentRecord("doc-1", OpType.INDEX,
                            OptionalLong.of(1), "x".getBytes(StandardCharsets.UTF_8))));

            // ⚠️ A DEADLINE, NOT A SLEEP: the adapted interval is carried back to
            // the active buffer after the append is answered.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (spacing.get().getAsLong() != 5000 && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(spacing.get().getAsLong())
                    .as("⚠️ THE GOVERNOR's SPACING IS THE INGEST's LENGTHENED INTERVAL: a "
                            + "supplier never attached to the ingest answers the floor for ever")
                    .isEqualTo(5000);
        }
    }
}
