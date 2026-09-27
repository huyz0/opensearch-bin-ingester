// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import java.util.Optional;
import java.util.UUID;

/** How the composition root maps an index name to its stream (extracted from {@code Assembly} by M11.1, unchanged). */
final class CatalogStreams {

    private CatalogStreams() {
    }

    /**
     * The stream an index name resolves to.
     *
     * <p>⚠️ **ONE MAPPING, AND IT IS THE PLUGIN'S.** The consumer derives its
     * subscription key from the index UUID ({@code RunKey.ofIndexUuid}, M7.2);
     * a root that made up its own — one per index NAME, say — would publish to
     * a key nobody subscribes to. Two mappings that disagree do not throw. They
     * make two streams for one index, and the consumer's never moves.
     *
     * @throws IllegalArgumentException if the index has not been registered.
     *     ⚠️ **REFUSED RATHER THAN INVENTED**, which M8.32 replaces with FR-13's
     *     pending pool: a made-up stream for an unregistered index accepts
     *     records no consumer will ever subscribe to and returns 202 for them.
     */
    static UUID streamFor(IndexCatalog catalog, String indexOrAlias) {
        Optional<IndexRegistration> registration = catalog.resolve(indexOrAlias);
        if (registration.isEmpty()) {
            throw new IllegalArgumentException("index is not registered: " + indexOrAlias);
        }
        // ⚠️ THROUGH `RunKey.ofIndexUuid` RATHER THAN DECODING HERE, partition 0
        // discarded. The partition is the caller's and only the index half is
        // wanted, but routing the decode through the one method that owns it is
        // exactly M7.2's rule: an index uuid is BASE64URL, `UUID.fromString`
        // throws on every real one, and a second decoder that disagreed would
        // not throw -- it would make two streams for one index.
        return RunKey.ofIndexUuid(registration.get().indexUuid(), 0).indexId();
    }
}
