// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Where a takeover learns an index's streams (M13.33aa review round 1, T1):
 * every partition of its fast registration in the catalog, and none for an
 * index the catalog does not hold as fast -- the source that decides which
 * streams of a lost index are voided rather than resumed at {@code c0}.
 */
class FastPeerStreamsOfTest {

    private static final String ORDERS = "nVzgup36TLqWp7VBBREj1w";
    private static final String LOGS = "AAAAAAAAAAAAAAAAAAAAAQ";
    private static final String SLOW = "AAAAAAAAAAAAAAAAAAAAAg";

    @Test
    void anINDEXsStreamsAreItsFastRegistrationsPartitionsAndNoOthers() {
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(new IndexRegistration(ORDERS, "orders", List.of(), 3, 3, 1, 1)
                .withFastSettings(250, true, 2));
        catalog.register(new IndexRegistration(LOGS, "logs", List.of(), 2, 2, 1, 1)
                .withFastSettings(250, true, 1));
        catalog.register(new IndexRegistration(SLOW, "slow", List.of(), 4, 4, 1, 1));
        UUID orders = RunKey.ofIndexUuid(ORDERS, 0).indexId();

        var streamsOf = FastPeer.streamsOf(catalog);

        assertThat(streamsOf.apply(orders)).containsExactly(new RunKey(orders, 0),
                new RunKey(orders, 1), new RunKey(orders, 2));
        assertThat(streamsOf.apply(RunKey.ofIndexUuid(SLOW, 0).indexId()))
                .as("not fast: no stream of it is a fast term's").isEmpty();
        assertThat(streamsOf.apply(new UUID(7, 7))).as("not held").isEmpty();
    }
}
