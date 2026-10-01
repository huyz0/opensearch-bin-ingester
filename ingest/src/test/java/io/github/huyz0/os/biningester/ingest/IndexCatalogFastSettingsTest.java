// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import java.util.List;
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
