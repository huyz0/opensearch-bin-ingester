// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.HttpSubscriptionTransport;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import io.github.huyz0.os.biningester.ingest.WatermarkTable;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * The three settings over the real wire, into the catalog (M13.23, criterion
 * 7; review round 1, T5).
 *
 * <p>⚠️ THE WHOLE PATH CRITERION 7 NAMES: the plugin's transport encodes a v2
 * frame, the ingester's register route decodes it and hands it to the
 * catalog. The codec and the catalog are each tested alone; a route that
 * rebuilt the registration from its placement fields -- the shape every
 * earlier caller used -- would pass both and leave every index in the mode
 * it was created in.
 */
class FastSettingsOverTheWireTest {

    private static final String UUID = "nVzgup36TLqWp7VBBREj1w";

    private WebServer server;
    private HttpSubscriptionTransport transport;

    @AfterEach
    void stop() {
        if (transport != null) {
            transport.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    @Test
    void aV2RegistrationReachesTheCATALOGWithItsSettings() throws Exception {
        IndexCatalog catalog = new IndexCatalog();
        WatermarkTable watermarks = new WatermarkTable(Clock.systemUTC(), Duration.ofMinutes(1),
                Duration.ofHours(2), Duration.ofMinutes(30));
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new SubscriptionService(
                        new SubscriptionHub(), catalog, watermarks, Clock.systemUTC())))
                .build().start();
        transport = new HttpSubscriptionTransport("http://localhost:" + server.port(), () -> { },
                Duration.ofMillis(20), Duration.ofMillis(200), Duration.ofSeconds(5));

        transport.register(new IndexRegistration(UUID, "orders", List.of("orders-w"), 2, 2, 1, 1)
                .withFastSettings(250, true, 3));

        IndexRegistration landed = catalog.resolve("orders-w").orElseThrow();
        assertThat(landed.wal()).as("wal, through the transport, the route and the catalog")
                .isTrue();
        assertThat(landed.walQuorum()).isEqualTo(3);
        assertThat(landed.flushTimerMillis()).isEqualTo(250);
    }
}
