// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CostGovernor;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.ChainBackfill;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import io.helidon.metrics.api.Metrics;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * M12.16 (M10.27 P2): the governor gauges read the most recently bound
 * governor, and a CLOSED pod's governor stayed bound -- so after a pod closed,
 * a scrape still reported its alarm, and in a JVM assembling several pods it
 * read whichever was constructed last, closed or not.
 */
class GovernorMetricsUnbindTest {

    private static final class MutableClock extends Clock {
        private volatile Instant now = Instant.parse("2026-09-29T10:00:00Z");

        void advance(Duration by) {
            now = now.plus(by);
        }

        @Override
        public Instant instant() {
            return now;
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

    /** A governor whose last window read 10x: alarmed. */
    private static CostGovernor alarmed(MutableClock clock) {
        CostGovernor governor = quiet(clock);
        for (int i = 0; i < 600; i++) {
            governor.recordDataPut(0);
        }
        clock.advance(Duration.ofMinutes(1));
        assertThat(governor.alarmed()).as("the premise: 10x is past the 3x alarm").isTrue();
        return governor;
    }

    /** One-minute windows, a 1 s spacing: 60 PUTs are expected per window. */
    private static CostGovernor quiet(MutableClock clock) {
        return new CostGovernor(new CostGovernor.Settings(1.0, 1, Duration.ofMinutes(1), 8L << 20),
                clock, () -> 1000);
    }

    private static double alarmGauge() {
        return Metrics.globalRegistry().gauge(GovernorMetrics.ALARM, List.of()).orElseThrow()
                .value().doubleValue();
    }

    @Test
    void aClosedPodsGovernorIsNoLongerRead() {
        GovernorMetrics metrics = GovernorMetrics.bind(alarmed(new MutableClock()));
        assertThat(alarmGauge()).as("the premise: the bound governor is read").isEqualTo(1.0);

        metrics.close();

        assertThat(alarmGauge()).as("⚠️ A CLOSED POD's ALARM IS NOT THE PROCESS's").isEqualTo(0.0);
    }

    @Test
    void closingAnEarlierPodLeavesTheLaterPodsGovernorBound() {
        GovernorMetrics earlier = GovernorMetrics.bind(quiet(new MutableClock()));
        GovernorMetrics later = GovernorMetrics.bind(alarmed(new MutableClock()));

        earlier.close();

        assertThat(alarmGauge())
                .as("⚠️ CLOSING THE EARLIER POD UNBINDS ONLY ITS OWN governor, not the later one")
                .isEqualTo(1.0);
        later.close();
        assertThat(alarmGauge()).isEqualTo(0.0);
    }

    @Test
    void closingAnAssembledPodUnbindsItsGovernor() throws Exception {
        MutableClock governorClock = new MutableClock();
        GovernorWiring.GovernorFactory factory =
                (config, clock, spacing) -> alarmed(governorClock);
        try (var store = new MemoryBinStore()) {
            Assembly assembly = Assembly.openForTest(config(), store, noPeers(),
                    Clock.systemUTC(), ChainBackfill::inBackground, factory);
            assertThat(alarmGauge()).as("the premise: the pod's governor is bound").isEqualTo(1.0);

            assembly.close();

            assertThat(alarmGauge()).as("⚠️ THE ASSEMBLY's CLOSE UNBINDS WHAT IT BOUND")
                    .isEqualTo(0.0);
        }
    }

    private static ServerConfig config() {
        return new ServerConfig("poda", "az-a", "cluster-a", "bins/cluster-a",
                new StoreConfig("memory", Optional.empty()), Duration.ofSeconds(30),
                Duration.ofSeconds(20), "http://poda:8080", IngestConfig.defaults("cluster-a"),
                0, "producer-1", Set.of("logs"), RetentionConfig.defaults(), java.util.Optional.empty(), "", io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL, io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false);
    }

    private static SequencerTransport noPeers() {
        return new SequencerTransport() {
            @Override
            public io.github.huyz0.os.biningester.format.CommitDelta send(String endpoint,
                    CommitRequest request) {
                throw new UnsupportedOperationException("no peer expected");
            }

            @Override
            public void close() {
            }
        };
    }
}
