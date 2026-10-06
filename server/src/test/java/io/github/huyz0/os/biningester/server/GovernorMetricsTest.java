// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CostGovernor;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.ChainBackfill;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The pod's governor is exported on its metrics route (M10.27, cost.md rule
 * 17, observability.md rule 4): refusals by class and in total, and the ratio,
 * alarm and kill switch read from the live governor.
 */
class GovernorMetricsTest {

    private static final class MutableClock extends Clock {
        private volatile Instant now = Instant.parse("2026-09-27T10:00:00Z");

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

    private static ServerConfig config() {
        return new ServerConfig("poda", "az-a", "cluster-a", "bins/cluster-a",
                new StoreConfig("memory", Optional.empty()), Duration.ofSeconds(30),
                Duration.ofSeconds(20), "http://poda:8080", IngestConfig.defaults("cluster-a"),
                0, "producer-1", Set.of("logs"), RetentionConfig.defaults(), java.util.Optional.empty(), "uid-poda", io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL, io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false, java.util.Optional.empty(), PeerConfig.off(0));
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

    @Test
    void refusalsRatioAlarmAndKillSwitchAreExportedFromThePodsGovernor() throws Exception {
        MutableClock governorClock = new MutableClock();
        // ⚠️ A ONE-LIST BURST AND A 1 s SPACING: 60 data PUTs are expected in a
        // one-minute window, so 600 read as exactly the 10x halt.
        GovernorWiring.GovernorFactory factory = (config, clock, spacing) -> new CostGovernor(
                new CostGovernor.Settings(1.0, 1, Duration.ofMinutes(1), 8L << 20),
                governorClock, () -> 1000);
        try (var store = new MemoryBinStore();
                Assembly assembly = Assembly.openForTest(config(), store, noPeers(),
                        Clock.systemUTC(), ChainBackfill::inBackground, factory);
                FrontDoor door = FrontDoor.start(assembly, Clock.systemUTC())) {
            CostGovernor governor = assembly.governor();
            HttpClient client = HttpClient.newHttpClient();
            String before = scrape(client, door.port());

            assertThat(governor.admitList()).as("the premise: the burst's one token").isTrue();
            // ⚠️ TWO LIST REFUSALS AND ONE DISCRETIONARY, so each class's counter is
            // pinned to its own class: with one of each, swapped counters pass.
            assertThat(governor.admitList()).as("the premise: refused past it").isFalse();
            assertThat(governor.admitList()).as("and again").isFalse();
            for (int i = 0; i < 600; i++) {
                governor.recordDataPut(0);
            }
            governorClock.advance(Duration.ofMinutes(1));
            assertThat(governor.discretionaryAllowed()).as("the premise: 10x halts").isFalse();

            String after = scrape(client, door.port());
            assertThat(sample(after, GovernorMetrics.LIST_REFUSALS)
                    - sample(before, GovernorMetrics.LIST_REFUSALS))
                    .as("two LISTs refused, counted as they happened").isEqualTo(2.0);
            assertThat(sample(after, GovernorMetrics.DISCRETIONARY_REFUSALS)
                    - sample(before, GovernorMetrics.DISCRETIONARY_REFUSALS))
                    .as("one discretionary start refused").isEqualTo(1.0);
            assertThat(sample(after, GovernorMetrics.REFUSALS)
                    - sample(before, GovernorMetrics.REFUSALS))
                    .as("⚠️ THE FRONT-PAGE TILE IS EVERY CLASS, not one of them")
                    .isEqualTo(3.0);
            assertThat(sample(after, GovernorMetrics.RATIO))
                    .as("⚠️ THE LIVE GOVERNOR's RATIO, read at the scrape").isEqualTo(10.0);
            assertThat(sample(after, GovernorMetrics.ALARM)).isEqualTo(1.0);
            assertThat(sample(after, GovernorMetrics.KILL_SWITCH))
                    .as("10x halts; only 100x trips the switch").isEqualTo(0.0);
            assertThat(metricLine(after, GovernorMetrics.REFUSALS))
                    .as("⚠️ NO LABEL BEYOND HELIDON's SCOPE: the allow-list is closed")
                    .startsWith(GovernorMetrics.REFUSALS + "{scope=\"application\",}");

            governorClock.advance(Duration.ofMinutes(1));
            for (int i = 0; i < 6000; i++) {
                governor.recordDataPut(0);
            }
            governorClock.advance(Duration.ofMinutes(1));
            String killed = scrape(client, door.port());
            assertThat(sample(killed, GovernorMetrics.KILL_SWITCH))
                    .as("100x trips the switch, and the gauge says so").isEqualTo(1.0);
        }
    }

    private static String scrape(HttpClient client, int port) throws Exception {
        HttpResponse<String> response = client.send(HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + "/observe/metrics?scope=application"))
                .GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return response.body();
    }

    private static double sample(String exposition, String name) {
        String line = metricLine(exposition, name);
        return Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
    }

    private static String metricLine(String exposition, String name) {
        return exposition.lines()
                .filter(each -> each.startsWith(name + "{") || each.startsWith(name + " "))
                .findFirst().orElseThrow(() -> new AssertionError("missing metric " + name));
    }
}
