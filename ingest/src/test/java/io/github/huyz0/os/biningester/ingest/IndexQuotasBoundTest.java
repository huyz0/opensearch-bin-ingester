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
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * M12.4 (M11 review F6, M11.8 P4; security.md rule 5): with a default quota,
 * {@link IndexQuotas} made a bucket for any name a producer sent and never
 * removed one, so the map grew with the names an unauthenticated producer
 * cared to invent.
 */
class IndexQuotasBoundTest {

    private static final IndexQuotas.Limit TEN_A_SECOND = new IndexQuotas.Limit(0, 10);
    private static final Duration IDLE = Duration.ofMinutes(5);

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

    private IndexQuotas quotas(Set<String> known) {
        return new IndexQuotas(new IndexQuotas.Config(TEN_A_SECOND, Map.of(), 8, IDLE), clock,
                known::contains, name -> java.util.List.of());
    }

    @Test
    void aNameTheCatalogDoesNotKnowGetsNoBucketAndIsNotRefusedHere() {
        IndexQuotas quotas = quotas(Set.of("logs"));

        for (int i = 0; i < 1_000; i++) {
            IndexQuotas.Admission admitted = quotas.admit("invented-" + i);
            assertThat(admitted.ticket()).as("refused downstream, as unregistered").isPresent();
            admitted.ticket().get().charge(records(100));
            admitted.ticket().get().release();
        }

        assertThat(quotas.bucketCount()).as("⚠️ NO STATE PER INVENTED NAME").isZero();
        quotas.admit("logs").ticket().orElseThrow().release();
        assertThat(quotas.bucketCount()).as("a known index still gets its bucket").isEqualTo(1);
    }

    @Test
    void anIdleFullBucketIsDroppedAfterItsIdleTimeAndABusyOneIsKept() {
        IndexQuotas quotas = quotas(Set.of("logs", "metrics", "traces"));
        quotas.admit("logs").ticket().orElseThrow().release();
        IndexQuotas.Ticket held = quotas.admit("metrics").ticket().orElseThrow();

        clock.advance(IDLE.minusSeconds(1));
        quotas.admit("traces").ticket().orElseThrow().release();
        assertThat(quotas.bucketCount()).as("not yet idle for the whole time").isEqualTo(3);

        clock.advance(Duration.ofSeconds(2));
        quotas.admit("traces").ticket().orElseThrow().release();
        assertThat(quotas.bucketCount())
                .as("logs dropped; metrics holds a request; traces was just used")
                .isEqualTo(2);
        held.release();
    }

    /**
     * ⚠️ DROPPING IS LOSSLESS ONLY FOR A FULL BUCKET: a fresh one starts full,
     * so dropping one in debt would forgive the debt.
     */
    @Test
    void aBucketInDebtIsKeptUntilRepaidSoItsRefusalStands() {
        IndexQuotas quotas = quotas(Set.of("logs", "metrics"));
        IndexQuotas.Ticket ticket = quotas.admit("logs").ticket().orElseThrow();
        ticket.charge(records(10 + 10 * (int) IDLE.plusMinutes(1).toSeconds())); // > 6 min of debt
        ticket.release();

        clock.advance(IDLE.plusSeconds(1));
        quotas.admit("metrics").ticket().orElseThrow().release();

        assertThat(quotas.bucketCount()).as("logs still owes").isEqualTo(2);
        assertThat(quotas.admit("logs").refusal()).as("its debt still refuses it").isPresent();
    }

    /**
     * ⚠️ REGISTERED WHILE ITS REQUEST WAITED (M12.4 review P1, round 2), IN
     * PRODUCTION's ORDER: {@code BulkService} charges a chunk, then appends it,
     * and a routed append waits inside for its index's registration
     * (ADR-0015). A one-chunk request's only charge therefore comes before the
     * name is known -- it is still owed, and charged when the request ends.
     */
    @Test
    void aOneChunkRequestRegisteredDuringItsWaitIsStillCharged() {
        java.util.Set<String> known = new java.util.HashSet<>();
        IndexQuotas quotas = new IndexQuotas(new IndexQuotas.Config(TEN_A_SECOND, Map.of(), 8,
                IDLE), clock, known::contains, name -> java.util.List.of());
        IndexQuotas.Ticket ticket = quotas.admit("logs").ticket().orElseThrow();

        ticket.charge(records(25)); // charged, then appended: the wait is inside
        known.add("logs"); // the registration arrives during the wait
        ticket.release();

        assertThat(quotas.admit("logs").refusal()).as("its 25 records are owed against 10/s")
                .isPresent();
    }

    /** A later chunk, charged after the registration, binds the ticket and pays the first too. */
    @Test
    void aLaterChunkBindsTheTicketAndPaysForTheFirst() {
        java.util.Set<String> known = new java.util.HashSet<>();
        IndexQuotas quotas = new IndexQuotas(new IndexQuotas.Config(TEN_A_SECOND, Map.of(), 1,
                IDLE), clock, known::contains, name -> java.util.List.of());
        IndexQuotas.Ticket ticket = quotas.admit("logs").ticket().orElseThrow();

        ticket.charge(records(8));
        known.add("logs");
        ticket.charge(records(8));

        assertThat(quotas.admit("logs").refusal()).as("its slot counts against the cap of 1")
                .isPresent();
        ticket.release();
        assertThat(quotas.admit("logs").refusal()).as("16 records owed, not 8").isPresent();
    }

    /**
     * ⚠️ OWED ONCE, NOT TWICE (M13.14, M12.4 review T5): a ticket for a name
     * unknown at admission tallies what it is charged, and on binding charges
     * the tally and forgets it. Kept, the release would charge it again: seven
     * records owed and bound would cost fourteen, and the index's next request
     * would be refused for records it never sent.
     */
    @Test
    void aDeferredTicketChargesWhatItOwedOnceWhenItBinds() {
        Set<String> known = java.util.concurrent.ConcurrentHashMap.newKeySet();
        IndexQuotas quotas = quotas(known);

        IndexQuotas.Ticket ticket = quotas.admit("late").ticket().orElseThrow();
        ticket.charge(records(4));
        known.add("late");
        ticket.charge(records(3)); // binds: the 4 owed and these 3
        ticket.release();

        assertThat(quotas.admit("late").refusal())
                .as("7 of its 10 charged, so it is admitted; 14 would be debt").isEmpty();
    }

    /**
     * ⚠️ AND ITS BYTES ONCE TOO (M13.14 review T1): a bytes-only quota, where
     * a byte tally kept after binding would charge the pre-registration bytes
     * again at release, and refuse the index for bytes it never sent.
     */
    @Test
    void aDeferredTicketChargesTheBytesItOwedOnceWhenItBinds() {
        long each = Accumulator.estimatedFramedBytes(records(1).getFirst());
        Set<String> known = java.util.concurrent.ConcurrentHashMap.newKeySet();
        IndexQuotas quotas = new IndexQuotas(new IndexQuotas.Config(
                new IndexQuotas.Limit(10 * each, 0), Map.of(), 8, IDLE), clock,
                known::contains, name -> java.util.List.of());

        IndexQuotas.Ticket ticket = quotas.admit("late").ticket().orElseThrow();
        ticket.charge(records(4));
        known.add("late");
        ticket.charge(records(3));
        ticket.release();

        assertThat(quotas.admit("late").refusal())
                .as("7 records' bytes of its 10's burst charged, so it is admitted").isEmpty();
    }

    /**
     * ⚠️ THE FIRST ALIAS IN SORTED ORDER WINS (M13.14, M12.13 review T2): an
     * index with no override of its own, named by two aliases that both have
     * one, takes the alias that sorts first -- however its aliases are listed.
     */
    @Test
    void theOverrideOfTheAliasThatSortsFirstApplies() {
        IndexQuotas quotas = new IndexQuotas(new IndexQuotas.Config(IndexQuotas.Limit.UNLIMITED,
                Map.of("a-alias", new IndexQuotas.Limit(0, 1),
                        "b-alias", new IndexQuotas.Limit(0, 100)), 8, IDLE),
                clock, name -> true, name -> List.of("b-alias", "a-alias"));

        IndexQuotas.Ticket ticket = quotas.admit("logs").ticket().orElseThrow();
        ticket.charge(records(2));
        ticket.release();

        assertThat(quotas.admit("logs").refusal())
                .as("a-alias's 1 a second, listed second but sorting first: 2 records is debt")
                .isPresent();
    }

    private static List<SegmentRecord> records(int n) {
        List<SegmentRecord> records = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            records.add(new SegmentRecord("r" + i, OpType.INDEX, OptionalLong.empty(),
                    new byte[0]));
        }
        return records;
    }
}
