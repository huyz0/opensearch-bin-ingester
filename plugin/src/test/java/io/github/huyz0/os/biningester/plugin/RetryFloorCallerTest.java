// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.HttpSubscriptionTransport;
import org.junit.jupiter.api.Test;
import org.opensearch.common.settings.Settings;

/** The production plugin path must use the spread floor, not only expose it (M8.71). */
class RetryFloorCallerTest {

    @Test
    void thePRODUCTIONNodeUsesTheDEFAULTRetryFloor() {
        BinStorePlugin plugin = new BinStorePlugin(Settings.builder()
                .put("node.name", "retry-floor-node")
                .put(BinStorePlugin.INGESTER_ENDPOINT.getKey(), "http://127.0.0.1:1")
                .build());
        try {
            HttpSubscriptionTransport transport =
                    (HttpSubscriptionTransport) plugin.subscriptions().transport();
            assertThat(transport.retryFloor())
                    .as("the shipped plugin path must pass the one-second spread floor into "
                            + "the transport; a constant with no production caller cannot "
                            + "shape reconnects")
                    .isEqualTo(HttpSubscriptionTransport.DEFAULT_RETRY_FLOOR);
        } finally {
            plugin.subscriptions().close();
            BinStorePlugin.uninstall();
        }
    }
}
