// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CostTable;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger.Charge;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * An index charged once and idle since is not named again: an interval in
 * which nothing was charged says {@code none} even after the pod's first
 * charge (M11.5, review round 1).
 */
class CostTopKReporterIdleTest {

    private static final UUID A = UUID.fromString("00000000-0000-4000-8000-00000000000a");
    private static final Duration INTERVAL = Duration.ofMinutes(5);

    @Test
    void anIndexIdleThisIntervalIsNotNamedAndAnIdleIntervalSaysNone() {
        Instant[] now = {Instant.parse("2026-09-28T10:00:00Z")};
        Clock clock = new Clock() {
            @Override
            public Instant instant() {
                return now[0];
            }

            @Override
            public java.time.ZoneId getZone() {
                return ZoneOffset.UTC;
            }

            @Override
            public Clock withZone(java.time.ZoneId zone) {
                return this;
            }
        };
        IndexCostLedger ledger = new IndexCostLedger();
        List<String> lines = new ArrayList<>();
        CostTopKReporter reporter = new CostTopKReporter(ledger, () -> Map.of(A, "alpha"),
                CostTable.awsS3Standard(), INTERVAL, clock, lines::add, new RefusedIndices(), () -> 0);

        ledger.apportion(Charge.DATA_PUT, Map.of(A, 1L));
        now[0] = now[0].plus(INTERVAL);
        reporter.tick();
        now[0] = now[0].plus(INTERVAL);
        reporter.tick();

        assertThat(lines).hasSize(2);
        assertThat(lines.get(0)).as("the premise: the charge was named").contains("alpha");
        assertThat(lines.get(1))
                .as("⚠️ NOTHING THIS INTERVAL: no zero-cost row for alpha, and 'none' said")
                .contains("none")
                .doesNotContain("alpha");
    }
}
