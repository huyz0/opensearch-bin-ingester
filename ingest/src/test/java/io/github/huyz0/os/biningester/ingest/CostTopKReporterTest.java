// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.CostTable;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger.Charge;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * One line per interval, naming the top 3 indices by what they cost IN that
 * interval (M11.5, M11 criterion 8), on an injected clock.
 */
class CostTopKReporterTest {

    private static final UUID A = UUID.fromString("00000000-0000-4000-8000-00000000000a");
    private static final UUID B = UUID.fromString("00000000-0000-4000-8000-00000000000b");
    private static final UUID C = UUID.fromString("00000000-0000-4000-8000-00000000000c");
    private static final UUID D = UUID.fromString("00000000-0000-4000-8000-00000000000d");
    private static final Duration INTERVAL = Duration.ofMinutes(5);

    private static final class TestClock extends Clock {
        private Instant now = Instant.parse("2026-09-28T10:00:00Z");

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
    private final IndexCostLedger ledger = new IndexCostLedger();
    private final List<String> lines = new ArrayList<>();

    private CostTopKReporter reporter(Duration interval) {
        return new CostTopKReporter(ledger, () -> Map.of(A, "alpha", B, "beta", C, "gamma"),
                CostTable.awsS3Standard(), interval, clock, lines::add);
    }

    private void puts(UUID index, int n) {
        for (int i = 0; i < n; i++) {
            ledger.apportion(Charge.DATA_PUT, Map.of(index, 1L));
        }
    }

    @Test
    void nothingIsEmittedBeforeTheFirstIntervalAndExactlyOneLineAtIt() {
        CostTopKReporter reporter = reporter(INTERVAL);
        puts(A, 1);

        clock.advance(INTERVAL.minusMillis(1));
        assertThat(reporter.tick()).isFalse();
        assertThat(lines).as("not due yet").isEmpty();

        clock.advance(Duration.ofMillis(1));
        assertThat(reporter.tick()).isTrue();
        assertThat(reporter.tick()).as("⚠️ ONE LINE PER INTERVAL, however often ticked")
                .isFalse();
        assertThat(lines).hasSize(1);
    }

    @Test
    void theLineNamesTheTopThreeByEstimatedCostInOrder() {
        CostTopKReporter reporter = reporter(INTERVAL);
        puts(A, 2);
        puts(B, 5);
        puts(C, 3);
        puts(D, 1);

        clock.advance(INTERVAL);
        reporter.tick();

        assertThat(lines).singleElement().satisfies(line -> {
            assertThat(line).contains("beta $0.000025000 (5.000000 requests), "
                    + "gamma $0.000015000 (3.000000 requests), "
                    + "alpha $0.000010000 (2.000000 requests)");
            assertThat(line).as("⚠️ THE FOURTH IS NOT NAMED").doesNotContain(D.toString());
        });
    }

    @Test
    void eachLineRanksWhatItsOwnIntervalCostNotTheLifetime() {
        CostTopKReporter reporter = reporter(INTERVAL);
        puts(A, 100);
        clock.advance(INTERVAL);
        reporter.tick();

        puts(D, 2);
        puts(A, 1);
        clock.advance(INTERVAL);
        reporter.tick();

        assertThat(lines).hasSize(2);
        assertThat(lines.get(1))
                .as("⚠️ A's 100 BEFORE DO NOT COUNT NOW: its 1 this interval ranks under D's 2")
                .contains(": " + D + " $0.000010000 (2.000000 requests), "
                        + "alpha $0.000005000 (1.000000 requests)");
    }

    @Test
    void onAFreeBackendTheBusiestIndexStillLeadsByRequests() {
        CostTopKReporter reporter = new CostTopKReporter(ledger, Map::of, CostTable.free(),
                INTERVAL, clock, lines::add);
        puts(A, 1);
        puts(D, 4);

        clock.advance(INTERVAL);
        reporter.tick();

        assertThat(lines).singleElement().asString()
                .as("⚠️ ZERO DOLLARS EACH, SO REQUESTS DECIDE: D first despite its higher id")
                .contains(": " + D + " $0.000000000 (4.000000 requests), " + A);
    }

    @Test
    void anIntervalInWhichNothingCostAnythingIsStillSaid() {
        CostTopKReporter reporter = reporter(INTERVAL);

        clock.advance(INTERVAL);
        reporter.tick();

        assertThat(lines).singleElement().asString().contains("none");
    }

    @Test
    void aLateTickEmitsOneLineAndTheNextIsAnIntervalFromThen() {
        CostTopKReporter reporter = reporter(INTERVAL);

        clock.advance(INTERVAL.multipliedBy(3));
        reporter.tick();
        reporter.tick();
        clock.advance(INTERVAL.minusMillis(1));
        reporter.tick();

        assertThat(lines).as("no catch-up burst, and not due again yet").hasSize(1);
    }

    @Test
    void aZeroIntervalNeverEmitsAndANegativeOneIsRefused() {
        CostTopKReporter off = reporter(Duration.ZERO);
        puts(A, 1);

        clock.advance(Duration.ofDays(1));

        assertThat(off.enabled()).isFalse();
        assertThat(off.tick()).isFalse();
        assertThat(lines).isEmpty();
        assertThat(reporter(INTERVAL).enabled()).isTrue();
        assertThatThrownBy(() -> reporter(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
