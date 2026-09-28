// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CostTable;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * M12.5 (M11 review F5): the top-K cost line names the indices refused
 * {@code 429} in its interval -- bounded, since an index name reaches it from a
 * producer.
 */
class CostTopKRefusedTest {

    private static final Duration INTERVAL = Duration.ofMinutes(5);

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

    private final TestClock clock = new TestClock();
    private final RefusedIndices refused = new RefusedIndices();
    private final List<String> lines = new ArrayList<>();
    private final CostTopKReporter reporter = new CostTopKReporter(new IndexCostLedger(),
            Map::of, CostTable.free(), INTERVAL, clock, lines::add, refused);

    @Test
    void aLineNamesTheIndicesRefusedInItsIntervalAndTheNextLineDoesNot() {
        refused.record("logs");
        refused.record("logs");
        refused.record("audit");
        clock.advance(INTERVAL);
        reporter.tick();
        clock.advance(INTERVAL);
        reporter.tick();

        assertThat(lines).hasSize(2);
        assertThat(lines.get(0)).contains("refused 429: audit, logs");
        assertThat(lines.get(1)).as("drained with its line").doesNotContain("refused");
    }

    @Test
    void itNamesAtMostEightAndCountsTheRest() {
        for (int i = 0; i < 20; i++) {
            refused.record("index-" + (100 + i));
        }

        RefusedIndices.Drained drained = refused.drain();

        assertThat(drained.names()).hasSize(RefusedIndices.NAMED)
                .as("the first refused, in the order refused").startsWith("index-100");
        assertThat(drained.more()).isEqualTo(12);
        refused.record("logs");
        clock.advance(INTERVAL);
        reporter.tick();
        assertThat(lines.get(0)).contains("refused 429: logs").doesNotContain("more");
    }
}
