// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * M12.13 (M11.8 P5): a quota override named by an ALIAS applies to the
 * concrete index it names -- the front door charges the concrete index, so an
 * override keyed by the alias matched nothing, silently, and the index ran on
 * the default.
 */
class QuotaPropertiesAliasTest {

    private static final Clock FROZEN =
            Clock.fixed(Instant.parse("2026-09-29T10:00:00Z"), ZoneOffset.UTC);
    private static final Set<String> KNOWN = Set.of("logs-000002", "metrics");

    private static IndexQuotas quotas(Map<String, IndexQuotas.Limit> overrides) {
        return new IndexQuotas(new IndexQuotas.Config(IndexQuotas.Limit.UNLIMITED, overrides, 8),
                FROZEN, name -> KNOWN.contains(name) ? java.util.Optional.of(name)
                        : java.util.Optional.empty(),
                index -> index.equals("logs-000002") ? List.of("logs-write", "logs") : List.of());
    }

    @Test
    void anOverrideNamedByAnAliasAppliesToItsConcreteIndex() {
        IndexQuotas quotas = quotas(Map.of("logs-write", new IndexQuotas.Limit(0, 2)));

        spend(quotas, "logs-000002", 5);

        assertThat(quotas.admit("logs-000002").refusal())
                .as("⚠️ THE ALIAS's OVERRIDE LIMITS ITS INDEX: 5 records at 2/s is debt")
                .isPresent();
        spend(quotas, "metrics", 5);
        assertThat(quotas.admit("metrics").refusal()).as("an index no override names")
                .isEmpty();
    }

    @Test
    void anOverrideNamingTheConcreteIndexWinsOverOneNamingItsAlias() {
        IndexQuotas quotas = quotas(Map.of("logs-write", new IndexQuotas.Limit(0, 2),
                "logs-000002", new IndexQuotas.Limit(0, 100)));

        spend(quotas, "logs-000002", 5);

        assertThat(quotas.admit("logs-000002").refusal()).as("its own 100/s, not the alias's")
                .isEmpty();
    }

    private static void spend(IndexQuotas quotas, String index, int records) {
        IndexQuotas.Ticket ticket = quotas.admit(index).ticket().orElseThrow();
        List<SegmentRecord> batch = new ArrayList<>();
        for (int i = 0; i < records; i++) {
            batch.add(new SegmentRecord("r" + i, OpType.INDEX, OptionalLong.empty(), new byte[0]));
        }
        ticket.charge(batch);
        ticket.release();
    }
}
