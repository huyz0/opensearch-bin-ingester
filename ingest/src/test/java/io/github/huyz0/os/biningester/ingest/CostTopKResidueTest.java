// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CostTable;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * M13.10 (M12 harvest R8): what the top-K cost line leaves out, it says. The
 * overflow past the named refused indices counts REFUSALS, so it says so, in
 * the line itself (M12.5 P1, T1, T3); and the registrations the line cannot
 * rank, because their index UUID does not decode, are counted in it (M12.8 P3).
 */
class CostTopKResidueTest {

    private static final Duration INTERVAL = Duration.ofMinutes(5);
    private static final Instant START = Instant.parse("2026-10-01T10:00:00Z");

    private final RefusedIndices refused = new RefusedIndices();
    private final AtomicInteger undecodable = new AtomicInteger();
    private final List<String> lines = new ArrayList<>();

    @Test
    void theOverflowPastTheNamedIndicesSaysItCountsRefusals() {
        for (char index = 'a'; index <= 'h'; index++) {
            refused.record(String.valueOf(index));
        }
        refused.record("i");
        refused.record("i");
        refused.record("i");
        refused.record("j");

        String line = lineAfterOneInterval();

        assertThat(line).as("⚠️ FOUR REFUSALS OF TWO INDICES: the count is of refusals,"
                + " and the line says so rather than 'and 4 more' (indices?)")
                .endsWith(" -- refused 429: a, b, c, d, e, f, g, h"
                        + " and 4 more refusals of other indices");
    }

    @Test
    void theLineCountsTheRegistrationsItCannotRank() {
        undecodable.set(2);

        assertThat(lineAfterOneInterval())
                .endsWith("none -- no index request was charged -- 2 registrations not ranked:"
                        + " their index UUIDs do not decode (see /admin/cost)");
    }

    @Test
    void oneOfEachIsSaidInTheSingular() {
        for (char index = 'a'; index <= 'i'; index++) {
            refused.record(String.valueOf(index));
        }
        undecodable.set(1);

        assertThat(lineAfterOneInterval()).endsWith(
                " -- 1 registration not ranked: its index UUID does not decode (see /admin/cost)"
                        + " -- refused 429: a, b, c, d, e, f, g, h and 1 more refusal of other indices");
    }

    @Test
    void aLineWithNothingLeftOutSaysNothingOfIt() {
        refused.record("logs");

        assertThat(lineAfterOneInterval())
                .endsWith(" -- refused 429: logs")
                .doesNotContain("more").doesNotContain("not ranked");
    }

    private String lineAfterOneInterval() {
        Clock later = Clock.fixed(START.plus(INTERVAL), ZoneOffset.UTC);
        CostTopKReporter reporter = new CostTopKReporter(new IndexCostLedger(), Map::of,
                CostTable.free(), INTERVAL, new Clock() {
                    private boolean started;

                    @Override
                    public Instant instant() {
                        if (!started) {
                            started = true;
                            return START;
                        }
                        return later.instant();
                    }

                    @Override
                    public java.time.ZoneId getZone() {
                        return ZoneOffset.UTC;
                    }

                    @Override
                    public Clock withZone(java.time.ZoneId zone) {
                        return this;
                    }
                }, lines::add, refused, undecodable::get);
        assertThat(reporter.tick()).isTrue();
        assertThat(lines).hasSize(1);
        return lines.getFirst();
    }
}
