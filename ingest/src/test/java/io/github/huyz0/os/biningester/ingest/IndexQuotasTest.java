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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Per-index debt token buckets and the in-flight cap (M11.8, ADR-0078, M11
 * criterion 11), on an injected clock.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class IndexQuotasTest {

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

    private static List<SegmentRecord> records(int n) {
        List<SegmentRecord> records = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            records.add(new SegmentRecord("r" + i, OpType.INDEX, OptionalLong.empty(),
                    new byte[0]));
        }
        return records;
    }

    private IndexQuotas quotas(IndexQuotas.Limit defaults, Map<String, IndexQuotas.Limit> perIndex,
            int cap) {
        return new IndexQuotas(new IndexQuotas.Config(defaults, perIndex, cap), clock,
                java.util.Optional::of, name -> java.util.List.of());
    }

    @Test
    void anUnconfiguredIndexIsNeverRefusedHoweverMuchItSends() {
        IndexQuotas quotas = quotas(IndexQuotas.Limit.UNLIMITED, Map.of(), 1);

        for (int i = 0; i < 100; i++) {
            IndexQuotas.Admission admission = quotas.admit("logs");
            assertThat(admission.ticket()).isPresent();
            admission.ticket().get().charge(records(1_000));
        }
        assertThat(quotas.admit("logs").refusal()).as("no rate, no cap").isEmpty();
    }

    @Test
    void anIndexInDebtIsRefusedUntilTheDebtIsRepaidAndToldHowLong() {
        IndexQuotas quotas = quotas(new IndexQuotas.Limit(0, 2), Map.of(), 8);

        IndexQuotas.Ticket first = quotas.admit("logs").ticket().orElseThrow();
        first.charge(records(5));
        first.release();

        IndexQuotas.Admission refused = quotas.admit("logs");
        assertThat(refused.refusal()).as("⚠️ 2 of burst, 5 charged: 3 records in debt")
                .hasValueSatisfying(r -> assertThat(r.retryAfterSeconds())
                        .as("3 records at 2/s: 1.5 s, rounded up").isEqualTo(2));
        clock.advance(Duration.ofMillis(1_499));
        assertThat(quotas.admit("logs").refusal()).as("not yet repaid").isPresent();
        clock.advance(Duration.ofMillis(1));
        assertThat(quotas.admit("logs").ticket()).as("repaid to zero: admitted").isPresent();
    }

    @Test
    void theDeeperOfTheTwoBucketsDecidesTheRetry() {
        // 10 B/s and 1 record/s; one record of ~ (5 + 1 + 5 + 0) = 11+ framed bytes.
        IndexQuotas quotas = quotas(new IndexQuotas.Limit(10, 100), Map.of(), 8);
        IndexQuotas.Ticket ticket = quotas.admit("logs").ticket().orElseThrow();
        List<SegmentRecord> big = List.of(new SegmentRecord("r", OpType.INDEX,
                OptionalLong.empty(), new byte[45]));
        ticket.charge(big);

        long framed = Accumulator.estimatedFramedBytes(big.get(0));
        long expected = (long) Math.ceil((framed - 10) / 10.0);
        assertThat(quotas.admit("logs").refusal())
                .hasValueSatisfying(r -> assertThat(r.retryAfterSeconds())
                        .as("the BYTES debt, the deeper").isEqualTo(expected));
    }

    @Test
    void anOverrideAppliesToItsIndexAloneAndAnotherIndexIsAdmitted() {
        IndexQuotas quotas = quotas(IndexQuotas.Limit.UNLIMITED,
                Map.of("hot", new IndexQuotas.Limit(0, 1)), 8);
        quotas.admit("hot").ticket().orElseThrow().charge(records(10));

        assertThat(quotas.admit("hot").refusal()).as("hot is in debt").isPresent();
        assertThat(quotas.admit("cold").ticket())
                .as("⚠️ PER INDEX: another index on the same pod is admitted").isPresent();
    }

    @Test
    void aRequestAdmittedWithTokensMayTakeTheBucketIntoDebtAndIsNeverCutOff() {
        IndexQuotas quotas = quotas(new IndexQuotas.Limit(0, 1), Map.of(), 8);
        IndexQuotas.Ticket ticket = quotas.admit("logs").ticket().orElseThrow();

        ticket.charge(records(1));
        ticket.charge(records(1_000));

        assertThat(quotas.admit("logs").refusal())
                .hasValueSatisfying(r -> assertThat(r.retryAfterSeconds()).isEqualTo(1_000));
    }

    @Test
    void aQuotadIndexHoldsAtMostItsCapOfAdmittedRequestsAndAReleaseReadmits()
            throws Exception {
        IndexQuotas quotas = quotas(new IndexQuotas.Limit(0, 1_000_000), Map.of(), 2);
        IndexQuotas.Ticket a = quotas.admit("logs").ticket().orElseThrow();
        IndexQuotas.Ticket b = quotas.admit("logs").ticket().orElseThrow();

        assertThat(quotas.admit("logs").refusal())
                .as("⚠️ THE CAP, WITH THE BUCKET FULL: concurrency cannot push the debt past it")
                .hasValueSatisfying(r -> assertThat(r.retryAfterSeconds()).isEqualTo(1));

        a.release();
        a.release();
        IndexQuotas.Ticket c = quotas.admit("logs").ticket().orElseThrow();
        assertThat(quotas.admit("logs").refusal()).as("released once, freed once").isPresent();

        b.release();
        assertThat(quotas.admit("logs").ticket()).as("a slot freed by a request's end readmits")
                .isPresent();
    }

    @Test
    void anIdleIndexRefillsToOneSecondsBurstAndNoFurther() {
        IndexQuotas quotas = quotas(new IndexQuotas.Limit(0, 2), Map.of(), 8);
        // The bucket exists before the idle time, so it is refilled across it.
        quotas.admit("logs").ticket().orElseThrow().release();
        clock.advance(Duration.ofSeconds(100));

        IndexQuotas.Ticket ticket = quotas.admit("logs").ticket().orElseThrow();
        ticket.charge(records(3));
        ticket.release();

        assertThat(quotas.admit("logs").refusal())
                .as("⚠️ A BURST OF ONE SECOND's RATE, not of the idle time: 2 of burst, 3 "
                        + "charged, 1 in debt")
                .hasValueSatisfying(r -> assertThat(r.retryAfterSeconds()).isEqualTo(1));
    }
}
