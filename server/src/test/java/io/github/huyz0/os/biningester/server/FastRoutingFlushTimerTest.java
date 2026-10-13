// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * The upload's deadline is the tightest {@code flush_timer} among the
 * catalog's FAST indices (ADR-0081 §7; M13.31c review round 1, T2) -- a
 * default-path index's timer is no fast deadline -- and ADR-0013's 5 s with
 * none.
 */
class FastRoutingFlushTimerTest {

    @Test
    void theTIGHTESTFastIndexsTimerIsTheDeadline() {
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(IndexRegistration.unsplit("nVzgup36TLqWp7VBBREj1w", "orders", 1)
                .withFastSettings(3_000, true, 2));
        catalog.register(IndexRegistration.unsplit("Qk1rUnfdRyCrk1L0DLaXEg", "audit", 1)
                .withFastSettings(1_000, true, 2));
        catalog.register(IndexRegistration.unsplit("aaaaaaaaaaaaaaaaaaaaaA", "logs", 1)
                .withFastSettings(100, false, 2));

        assertThat(FastRouting.tightestFlushTimer(catalog)).isEqualTo(Duration.ofSeconds(1));
    }

    @Test
    void withNOFastIndexTheDeadlineIsTheDefault() {
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(IndexRegistration.unsplit("aaaaaaaaaaaaaaaaaaaaaA", "logs", 1)
                .withFastSettings(100, false, 2));

        assertThat(FastRouting.tightestFlushTimer(catalog)).isEqualTo(Duration.ofSeconds(5));
    }
}
