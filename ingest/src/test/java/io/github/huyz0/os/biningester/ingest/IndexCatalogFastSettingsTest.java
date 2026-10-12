// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A live settings change reaches the catalog (M13.23, criterion 7).
 *
 * <p>⚠️ THE CATALOG IS WHERE THE LEADER READS AN INDEX'S MODE (ADR-0082 §1:
 * "the catalog is where the leader reads modes"), so a re-registration that
 * kept the first shape's settings -- an alias-only diff, say, or a
 * put-if-absent -- would leave a running index in the mode it was created in
 * while every placement field kept working, and nothing about routing would
 * look wrong.
 */
class IndexCatalogFastSettingsTest {

    private static final String UUID = "nVzgup36TLqWp7VBBREj1w";

    @Test
    void walQUORUMSNameEveryFastIndexByItsStreamIdAndNoOther() {
        // ⚠️ M13.27l: what the leader's term record is fed from (ADR-0081 §4).
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(new IndexRegistration(UUID, "orders", List.of(), 3, 3, 1, 1)
                .withFastSettings(250, true, 2));
        catalog.register(new IndexRegistration("Qk1rUnfdRyCrk1L0DLaXEg", "logs", List.of(),
                1, 1, 1, 1));

        assertThat(catalog.walQuorums())
                .isEqualTo(Map.of(RunKey.ofIndexUuid(UUID, 0).indexId(), 2));
    }

    @Test
    void aLIVESettingsChangeReplacesTheModeTheCatalogAnswers() {
        IndexCatalog catalog = new IndexCatalog();
        IndexRegistration created = new IndexRegistration(UUID, "orders", List.of("orders-w"),
                3, 3, 1, 1);
        catalog.register(created);

        catalog.register(created.withFastSettings(250, true, 3));

        IndexRegistration now = catalog.resolve("orders").orElseThrow();
        assertThat(now.wal()).as("wal flipped on the live index").isTrue();
        assertThat(now.walQuorum()).isEqualTo(3);
        assertThat(now.flushTimerMillis()).isEqualTo(250);
        assertThat(catalog.resolve("orders-w").orElseThrow().wal())
                .as("and through the alias too: a write by alias must see the same mode")
                .isTrue();
    }
}
