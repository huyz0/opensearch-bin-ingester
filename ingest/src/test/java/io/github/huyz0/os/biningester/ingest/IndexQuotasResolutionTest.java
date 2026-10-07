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
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

/**
 * A ticket for a name unknown at admission binds to its CONCRETE index under
 * that index's limit (M13.46): an alias written before its index registered
 * bound a bucket of its own, whose debt refused nothing sent to the index;
 * under an unlimited default it was waved through uncharged, an override on
 * the index notwithstanding; and it carried the default where the index had
 * an override.
 */
class IndexQuotasResolutionTest {

    private static final Duration IDLE = Duration.ofSeconds(10);

    private static final class TestClock extends Clock {
        private Instant now = Instant.parse("2026-10-08T10:00:00Z");

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
    /** Registered names, each to its concrete index: an alias maps to the index. */
    private final Map<String, String> registered = new ConcurrentHashMap<>();

    private IndexQuotas quotas(IndexQuotas.Limit defaults, Map<String, IndexQuotas.Limit> perIndex,
            int cap) {
        return new IndexQuotas(new IndexQuotas.Config(defaults, perIndex, cap, IDLE), clock,
                name -> Optional.ofNullable(registered.get(name)), this::aliasesOf);
    }

    /** The aliases registered for {@code index}. */
    private List<String> aliasesOf(String index) {
        return registered.entrySet().stream()
                .filter(e -> e.getValue().equals(index) && !e.getKey().equals(index))
                .map(Map.Entry::getKey).sorted().toList();
    }

    private static List<SegmentRecord> records(int n) {
        List<SegmentRecord> records = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            records.add(new SegmentRecord("r" + i, OpType.INDEX, OptionalLong.empty(),
                    new byte[0]));
        }
        return records;
    }

    private void register(String index, String... aliases) {
        registered.put(index, index);
        for (String alias : aliases) {
            registered.put(alias, index);
        }
    }

    @Test
    void anALIASWrittenBeforeItsIndexRegisteredChargesTheIndexsBucket() {
        IndexQuotas quotas = quotas(new IndexQuotas.Limit(0, 2), Map.of(), 8);

        IndexQuotas.Ticket early = quotas.admit("logs-write").ticket().orElseThrow();
        register("logs", "logs-write");
        early.charge(records(5));
        early.release();

        assertThat(quotas.admit("logs").refusal())
                .as("the alias's debt is the index's: one bucket, one cap").isPresent();
        assertThat(quotas.bucketCount()).as("and no bucket keyed by the alias").isEqualTo(1);
    }

    @Test
    void anUNKNOWNNameUnderAnUnlimitedDefaultIsChargedByItsIndexsOverride() {
        IndexQuotas quotas = quotas(IndexQuotas.Limit.UNLIMITED,
                Map.of("logs", new IndexQuotas.Limit(0, 2)), 8);

        // ⚠️ THROUGH AN ALIAS, which names no override: at admission it read as
        // the unlimited default, and was waved through on it
        IndexQuotas.Ticket early = quotas.admit("logs-write").ticket().orElseThrow();
        register("logs", "logs-write");
        early.charge(records(5));
        early.release();

        assertThat(quotas.admit("logs").refusal())
                .as("charged, not waved through under the unlimited default").isPresent();
    }

    @Test
    void anUNKNOWNNameCarriesItsIndexsOverrideNotTheDefault() {
        IndexQuotas quotas = quotas(new IndexQuotas.Limit(0, 100),
                Map.of("logs", new IndexQuotas.Limit(0, 2)), 8);

        IndexQuotas.Ticket early = quotas.admit("logs-write").ticket().orElseThrow();
        register("logs", "logs-write");
        early.charge(records(5));
        early.release();

        assertThat(quotas.admit("logs").refusal())
                .as("the override of 2 a second, not the default of 100").isPresent();
    }

    @Test
    void aNAMEBindingToAnUnlimitedIndexHoldsNoSlotAndChargesNothing() {
        IndexQuotas quotas = quotas(IndexQuotas.Limit.UNLIMITED, Map.of(), 1);

        IndexQuotas.Ticket early = quotas.admit("logs").ticket().orElseThrow();
        register("logs");
        early.charge(records(5));

        assertThat(quotas.bucketCount()).as("an unlimited index gets no bucket").isZero();
        early.release();
    }

    @Test
    void aBINDINGIntoABucketAtItsCapGoesPastItUntilReleased() {
        register("logs", "logs-write");
        IndexQuotas quotas = quotas(new IndexQuotas.Limit(0, 1_000), Map.of(), 1);
        IndexQuotas.Ticket holder = quotas.admit("logs").ticket().orElseThrow();
        registered.remove("logs-write");
        IndexQuotas.Ticket early = quotas.admit("logs-write").ticket().orElseThrow();
        register("logs", "logs-write");

        early.charge(records(1)); // binds past the cap: it cannot be refused mid-body
        holder.release();
        assertThat(quotas.admit("logs").refusal())
                .as("the bound ticket still holds the one slot").isPresent();
        early.release();
        assertThat(quotas.admit("logs").refusal()).as("its slot back on release").isEmpty();
    }

    @Test
    void theIDLESweepRunsAtMostOncePerIdleExpiry() {
        register("a");
        register("b");
        register("c");
        register("x");
        IndexQuotas quotas = quotas(new IndexQuotas.Limit(0, 1_000), Map.of(), 8);
        quotas.admit("a").ticket().orElseThrow().release();
        clock.advance(Duration.ofSeconds(1));
        quotas.admit("x").ticket().orElseThrow().release();
        clock.advance(Duration.ofSeconds(9));
        quotas.admit("b").ticket().orElseThrow().release(); // sweeps: a dropped, x kept
        assertThat(quotas.bucketCount()).as("the premise: the first sweep dropped a")
                .isEqualTo(2);

        clock.advance(Duration.ofSeconds(1)); // x idle 10 s now, but the last sweep 1 s ago
        quotas.admit("c").ticket().orElseThrow().release();

        assertThat(quotas.bucketCount()).as("⚠️ NO SECOND SWEEP within one idle expiry")
                .isEqualTo(3);
    }

    /**
     * ⚠️ AN OVERRIDE KEYED ONLY BY AN ALIAS applies to a window write (review
     * round 1, T1; ADR-0078 decision 4): read at admission it was unknown, and
     * the write went free under an unlimited default.
     */
    @Test
    void anOVERRIDEKeyedOnlyByAnAliasAppliesToAWindowWrite() {
        IndexQuotas quotas = quotas(IndexQuotas.Limit.UNLIMITED,
                Map.of("logs-write", new IndexQuotas.Limit(0, 2)), 8);

        IndexQuotas.Ticket early = quotas.admit("logs").ticket().orElseThrow();
        register("logs", "logs-write");
        early.charge(records(5));
        early.release();

        assertThat(quotas.admit("logs").refusal())
                .as("the alias's override, found when the ticket bound").isPresent();
    }
}
