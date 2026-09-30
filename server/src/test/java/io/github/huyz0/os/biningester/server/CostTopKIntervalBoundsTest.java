// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.CostTable;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.ingest.CostTopKReporter;
import io.github.huyz0.os.biningester.ingest.RefusedIndices;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * M12.9 (M11.5 P1b, P3): the top-K cost interval has a ceiling -- past it,
 * {@code toNanos} overflowed at scheduling with an error naming no key; its
 * one-second floor is M11.5's, pinned by {@code ServerPropertiesCostFloorTest}
 * -- and each line states the window it actually covers, not the configured
 * one.
 */
class CostTopKIntervalBoundsTest {

    private static final class TestClock extends Clock {
        private Instant now = Instant.parse("2026-09-29T10:00:00Z");

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

    @Test
    void theReporterRefusesAnIntervalPastItsCeilingAndTakesTheCeilingAndOff() {
        for (Duration bad : List.of(CostTopKReporter.MAX_INTERVAL.plusMillis(1),
                Duration.ofDays(200_000))) {
            assertThatThrownBy(() -> reporter(bad, new TestClock(), new ArrayList<>()))
                    .as(bad.toString()).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining(bad.toString());
        }
        for (Duration good : List.of(CostTopKReporter.MAX_INTERVAL, Duration.ZERO,
                Duration.ofMillis(100))) {
            assertThat(reporter(good, new TestClock(), new ArrayList<>())).isNotNull();
        }
    }

    @Test
    void anIntervalPastTheCeilingIsAConfigurationErrorNamingTheKey() {
        for (String bad : List.of("P2D", "PT24H0.001S")) {
            Map<String, String> settings = minimal();
            settings.put(ServerProperties.COST_TOP_K_INTERVAL, bad);

            assertThatThrownBy(() -> ServerProperties.parse(settings)).as(bad)
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining(ServerProperties.COST_TOP_K_INTERVAL);
        }
    }

    /** ⚠️ A LATE LINE SAYS HOW LATE (M11.5 P3): its window is the time since the last. */
    @Test
    void aLineStatesTheWindowItActuallyCovers() {
        TestClock clock = new TestClock();
        List<String> lines = new ArrayList<>();
        CostTopKReporter reporter = reporter(Duration.ofMinutes(5), clock, lines);

        clock.advance(Duration.ofMinutes(7)); // the scheduler stalled for two
        reporter.tick();
        clock.advance(Duration.ofMinutes(5));
        reporter.tick();

        assertThat(lines).hasSize(2);
        assertThat(lines.get(0)).contains("over the last PT7M");
        assertThat(lines.get(1)).contains("over the last PT5M");
    }

    private static CostTopKReporter reporter(Duration interval, Clock clock, List<String> lines) {
        return new CostTopKReporter(new IndexCostLedger(), Map::of, CostTable.free(), interval,
                clock, lines::add, new RefusedIndices(), () -> 0);
    }

    private static Map<String, String> minimal() {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_UID, "uid-pod1");
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "memory");
        settings.put(ServerProperties.ENDPOINT, "http://pod1:8080");
        settings.put(ServerProperties.HTTP_PORT, "8080");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        return settings;
    }
}
