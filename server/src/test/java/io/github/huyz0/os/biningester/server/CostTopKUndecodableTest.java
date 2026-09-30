// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CostTable;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import io.github.huyz0.os.biningester.ingest.RefusedIndices;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * M13.10 (M12.8 review P3): the pod's top-K line counts the catalog's
 * registrations whose index UUID does not decode -- the ones its ranking,
 * keyed by stream id, cannot name.
 */
class CostTopKUndecodableTest {

    @Test
    void thePodsLineCountsTheCatalogsUndecodableRegistrations() {
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(new IndexRegistration("AAAAAAAAQACAAAAAAAAAqg", "logs", List.of(),
                4, 4, 1, 1));
        catalog.register(new IndexRegistration("not-a-uuid", "broken", List.of(), 4, 4, 1, 1));
        catalog.register(new IndexRegistration("also-not", "worse", List.of(), 4, 4, 1, 1));
        Duration interval = Duration.ofMinutes(5);
        Instant start = Instant.parse("2026-10-01T10:00:00Z");
        Clock[] clock = {Clock.fixed(start, ZoneOffset.UTC)};
        Clock moving = new Clock() {
            @Override
            public Instant instant() {
                return clock[0].instant();
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
        List<String> lines = new ArrayList<>();
        var reporter = CostReporting.topK(new IndexCostLedger(), catalog, CostTable.free(),
                interval, moving, new RefusedIndices(), lines::add);

        clock[0] = Clock.fixed(start.plus(interval), ZoneOffset.UTC);
        assertThat(reporter.tick()).isTrue();

        assertThat(lines).singleElement().asString().endsWith(" -- 2 registrations not ranked:"
                + " their index UUIDs do not decode (see /admin/cost)");
    }
}
