// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A cost report names an index by the stream id the ledger holds, derived the
 * way the write path derives it (M11.4, M7.2).
 */
class IndexCatalogNamesTest {

    private static final String LOGS_UUID = "AAAAAAAAQACAAAAAAAAAqg";
    private static final String AUDIT_UUID = "AAAAAAAAQACAAAAAAAAAuw";

    @Test
    void everyRegisteredIndexIsNamedByItsStreamId() {
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(new IndexRegistration(LOGS_UUID, "logs", List.of(), 4, 4, 1, 1));
        catalog.register(new IndexRegistration(AUDIT_UUID, "audit", List.of(), 4, 4, 1, 1));

        assertThat(catalog.namesById())
                .as("⚠️ BY RunKey.ofIndexUuid, the id the ledger was charged under")
                .containsEntry(RunKey.ofIndexUuid(LOGS_UUID, 0).indexId(), "logs")
                .containsEntry(RunKey.ofIndexUuid(AUDIT_UUID, 0).indexId(), "audit")
                .hasSize(2);
    }
}
