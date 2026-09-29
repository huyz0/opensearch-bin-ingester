// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

/**
 * M12.19a (M11.8 T5): an idle index's BYTES bucket refills to one second's
 * rate and no further, as its records bucket does. Only the records cap had a
 * case, so a byte bucket that banked a long idle time -- and then admitted a
 * burst of that whole idle time's bytes -- passed every test.
 */
class IndexQuotasByteBurstTest {

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
    void anIdleIndexsByteBucketRefillsToOneSecondsBurstAndNoFurther() {
        TestClock clock = new TestClock();
        IndexQuotas quotas = new IndexQuotas(new IndexQuotas.Config(
                new IndexQuotas.Limit(1_000, 0), Map.of(), 8), clock, name -> true);
        quotas.admit("logs").ticket().orElseThrow().release();
        clock.advance(Duration.ofSeconds(100));

        IndexQuotas.Ticket ticket = quotas.admit("logs").ticket().orElseThrow();
        ticket.charge(List.of(new SegmentRecord("big", OpType.INDEX, OptionalLong.empty(),
                new byte[2_500])));
        ticket.release();

        assertThat(quotas.admit("logs").refusal())
                .as("⚠️ A BURST OF ONE SECOND's 1,000 BYTES, not of 100 idle seconds' "
                        + "100,000: 2,500 charged leaves ~1,500 in debt, 2 s to repay")
                .hasValueSatisfying(r -> assertThat(r.retryAfterSeconds()).isEqualTo(2));
    }
}
