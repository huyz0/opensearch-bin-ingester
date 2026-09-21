// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The catalog tells whoever is waiting that a registration arrived (M8.32).
 *
 * <p>⚠️ **THE ORDER IS THE CONTRACT.** A routed write waiting for its index
 * re-reads the catalog when woken; a listener run BEFORE the update finds the
 * index still unknown, goes back to sleep, and the write is refused at its
 * timeout for an index that had been registered all along.
 */
class IndexCatalogListenerTest {

    private static IndexRegistration logs(int shards) {
        return new IndexRegistration("dXVpZC1sb2dz", "logs", List.of("current"), shards, shards,
                1, 1);
    }

    @Test
    void aLISTENERRunsAFTERTheUpdateSoARECHECKFindsTheNEWEntry() {
        IndexCatalog catalog = new IndexCatalog();
        List<Optional<IndexRegistration>> seen = new ArrayList<>();
        catalog.whenRegistered(() -> seen.add(catalog.resolve("current")));

        catalog.register(logs(4));
        catalog.register(logs(8));

        assertThat(seen)
                .as("⚠️ EACH LISTENER RUN SEES THE REGISTRATION THAT TRIGGERED IT, alias "
                        + "included -- a run before the update sees nothing, then the old one")
                .extracting(found -> found.map(IndexRegistration::numShards).orElse(-1))
                .containsExactly(4, 8);
    }

    @Test
    void EVERYListenerRunsOnEVERYRegistration() {
        IndexCatalog catalog = new IndexCatalog();
        int[] runs = {0, 0};
        catalog.whenRegistered(() -> runs[0]++);
        catalog.whenRegistered(() -> runs[1]++);

        catalog.register(logs(4));
        catalog.register(logs(4));

        assertThat(runs).containsExactly(2, 2);
    }
}
