// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.CostGovernor;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.ingest.SegmentSink;
import io.github.huyz0.os.biningester.security.Principal;
import io.github.huyz0.os.biningester.sequencer.ChainBackfill;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Inbox;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The governor is wired into the assembled node, and NFR-16 holds (M10.11,
 * ADR-0075, criterion 14).
 *
 * <p>⚠️ **ZERO REFUSALS IS ONLY EVIDENCE IF THE GOVERNOR SAW THE TRAFFIC.** An
 * unwired governor refuses nothing too, so the steady-state case also asserts
 * that it rated the data PUTs and admitted the recovery LISTs, and the
 * takeover case that the recovery LISTs were admitted AS DECLARED.
 */
@Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class GovernorAssemblyTest {

    private static final String PREFIX = "bins/cluster-a";
    private static final String INDEX = "logs";
    private static final String INDEX_UUID = base64Url(UUID.randomUUID());
    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of(INDEX));

    private static String base64Url(UUID uuid) {
        ByteBuffer b = ByteBuffer.allocate(16);
        b.putLong(uuid.getMostSignificantBits());
        b.putLong(uuid.getLeastSignificantBits());
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b.array());
    }

    /**
     * Real time plus an offset a case moves forward.
     *
     * <p>⚠️ **TIME STILL FLOWS**, so the writer's interval flush fires as it
     * would in production; the offset is how a case reaches the hours a sweep
     * is allowed into without waiting for them.
     */
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

    private static ServerConfig config(String pod) {
        return new ServerConfig(pod, "az-a", "cluster-a", PREFIX,
                new StoreConfig("memory", Optional.empty()),
                Duration.ofSeconds(10), Duration.ofSeconds(3), "http://" + pod + ":8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", Set.of(INDEX), RetentionConfig.defaults(), java.util.Optional.empty(), "uid-" + pod, io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL, io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false, java.util.Optional.empty(), PeerConfig.off(0));
    }

    /**
     * ⚠️ A DAY-LONG LEASE, so moving the clock hours forward does not expire
     * the term the sweep needs, and a DAY-LONG PASS INTERVAL, so the root's own
     * scheduler never ticks the loop this case ticks by hand.
     */
    private static ServerConfig steadyConfig() {
        return new ServerConfig("pod1", "az-a", "cluster-a", PREFIX,
                new StoreConfig("memory", Optional.empty()),
                Duration.ofDays(1), Duration.ofSeconds(3), "http://pod1:8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", Set.of(INDEX),
                new RetentionConfig(Duration.ofMinutes(1), Duration.ofHours(2),
                        Duration.ofSeconds(10), Duration.ofHours(3), Duration.ofDays(1)), java.util.Optional.empty(), "uid-pod1", io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL, io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false, java.util.Optional.empty(), PeerConfig.off(0));
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

    /** ADR-0075's defaults with the bucket EMPTY and its clock frozen: no token refills. */
    private static CostGovernor drained(AtomicReference<CostGovernor> built) {
        CostGovernor governor = new CostGovernor(
                CostGovernor.Settings.defaults(IngestConfig.DEFAULT_MAX_SEGMENT_BYTES),
                Clock.fixed(Instant.parse("2026-09-27T00:00:00Z"), ZoneOffset.UTC), () -> 250);
        for (int i = 0; i < 300; i++) {
            if (!governor.admitList()) {
                throw new AssertionError("a fresh bucket holds its whole burst");
            }
        }
        built.set(governor);
        return governor;
    }

    @Test
    void aTAKEOVERsRecoveryBackfillAndInboxDrainSUCCEEDWithTheLISTBucketEMPTY()
            throws Exception {
        RunKey stream = new RunKey(UUID.randomUUID(), 0);
        try (BinStore shared = StoreFactory.open(new StoreConfig("memory", Optional.empty()))) {
            try (Assembly first = Assembly.open(config("pod1"), shared, noPeers(),
                    Clock.systemUTC())) {
                LocalSequencer term = LocalSequencer.underneath(first.heldTerm()).orElseThrow();
                // ⚠️ K COMMITS, so a successor's replay starts at a checkpoint and
                // the backfill has a chain below it to LIST.
                for (long i = 0; i < LocalSequencer.CHECKPOINT_EVERY_DELTAS + 5; i++) {
                    term.commit(new CommitRequest("p", "i", i, "seg/" + i, Map.of(stream, 1)));
                }
            }
            // ⚠️ AN ACKED INTENT A DEAD POD LEFT: only the takeover drain applies it.
            Inbox.write(shared, PREFIX,
                    new CommitRequest("podz", "i9", 0, "seg/deferred", Map.of(stream, 2)));

            AtomicReference<CostGovernor> governor = new AtomicReference<>();
            AtomicReference<Thread> backfill = new AtomicReference<>();
            try (Assembly second = Assembly.openForTest(config("pod2"), shared, noPeers(),
                    Clock.systemUTC(), (store, prefix, chain, serving) -> {
                        Thread worker = ChainBackfill.inBackground(store, prefix, chain, serving);
                        backfill.set(worker);
                        return worker;
                    }, (config, clock, spacing) -> drained(governor))) {
                LocalSequencer term = LocalSequencer.underneath(second.heldTerm())
                        .orElseThrow(() -> new AssertionError("⚠️ THE TAKEOVER DID NOT "
                                + "RECOVER: a refused chain-end or replay LIST fails the start"));
                assertThat(second.governor()).as("the root holds the governor it built")
                        .isSameAs(governor.get());

                assertThat(backfill.get().join(Duration.ofSeconds(60)))
                        .as("the backfill finishes").isTrue();
                assertThat(term.chain().snapshot().fromFloor())
                        .as("⚠️ THE BACKFILL, ON ITS OWN THREAD, LISTED WITH THE BUCKET EMPTY "
                                + "and reached the floor")
                        .isTrue();

                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (!shared.list(Inbox.prefixFor(PREFIX), null, 10).objects().isEmpty()
                        && System.nanoTime() < deadline) {
                    Thread.onSpinWait();
                }
                assertThat(shared.list(Inbox.prefixFor(PREFIX), null, 10).objects())
                        .as("⚠️ THE INBOX DRAIN, ON ITS OWN THREAD, LISTED WITH THE BUCKET "
                                + "EMPTY and applied the intent")
                        .isEmpty();
                assertThat(term.chain().snapshot().deltas())
                        .anyMatch(d -> d.segments().stream()
                                .anyMatch(s -> s.segmentKey().equals("seg/deferred")));

                CostGovernor.Counts counts = second.governor().counts();
                assertThat(counts.listRefusals()).as("no recovery LIST was refused").isZero();
                assertThat(counts.recoveryLists())
                        .as("⚠️ AND THEY WERE ADMITTED AS DECLARED RECOVERY, through the "
                                + "governed store -- an unwired governor counts none")
                        .isGreaterThanOrEqualTo(3);
            }
        }
    }

    @Test
    void aSTEADYStateAppendCommitReadAndSweepRecordsZEROREFUSALS() throws Exception {
        OffsetClock clock = new OffsetClock();
        try (BinStore shared = StoreFactory.open(new StoreConfig("memory", Optional.empty()));
                Assembly assembly = Assembly.open(steadyConfig(), shared, noPeers(), clock)) {
            assembly.catalog().register(
                    new IndexRegistration(INDEX_UUID, INDEX, List.of(), 4, 4, 1, 1));
            assembly.retention().tick(); // the term is first seen now

            for (int i = 0; i < 3; i++) {
                String id = "doc-" + i;
                assembly.ingest().append(PRINCIPAL, INDEX, 0, sink -> sink.accept(
                        new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                                id.getBytes(StandardCharsets.UTF_8))));
            }
            List<String> segments = shared.list(PREFIX + "/data/", null, 100).objects().stream()
                    .map(ObjectStat::key).toList();
            assertThat(segments).as("the premise: the appends were written").isNotEmpty();
            for (String segment : segments) {
                assembly.segmentProxy().streamTo(segment, List.<SegmentSink>of((buffer, off, len) -> { }));
            }

            // ⚠️ ONE WINDOW ON: the governor rates the window the PUTs fell in.
            clock.advance(Duration.ofSeconds(61));
            double ratio = assembly.governor().lastRatio();
            assertThat(ratio)
                    .as("⚠️ THE GOVERNOR RATED THE DATA PUTS -- an unwired one reads 0 -- and "
                            + "a steady run is far under the %s× alarm", CostGovernor.ALARM)
                    .isGreaterThan(0).isLessThan(CostGovernor.ALARM);

            // ⚠️ HOURS ON: segments past the floor and hours past their grace, so
            // the tick runs the retention pass AND the orphan sweep's LISTs.
            clock.advance(Duration.ofHours(4));
            long listsBefore = assembly.storeCounts().lists();
            assembly.retention().tick();
            assertThat(assembly.storeCounts().lists() - listsBefore)
                    .as("the premise: the sweep LISTed through the node's store")
                    .isPositive();

            CostGovernor.Counts counts = assembly.governor().counts();
            assertThat(counts.listRefusals()).as("⚠️ NFR-16: ZERO LIST REFUSALS").isZero();
            assertThat(counts.discretionaryRefusals())
                    .as("⚠️ NFR-16: ZERO DISCRETIONARY REFUSALS").isZero();
            assertThat(counts.recoveryLists())
                    .as("the term's own startup recovery was admitted as declared")
                    .isPositive();
        }
    }
}
