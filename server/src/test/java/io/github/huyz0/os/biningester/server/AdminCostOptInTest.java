// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.http.AdminCostService;
import io.github.huyz0.os.biningester.ingest.CostTopKReporter;
import io.github.huyz0.os.biningester.ingest.IndexQuotas;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * M12.6 (M11 review F7): {@code GET /admin/cost} lists index names on the
 * producer port, which is unauthenticated until M1.7c, against BulkService's
 * bodiless-403 rule -- so it is served only where an operator turned it on.
 */
class AdminCostOptInTest {

    @Test
    void anUnconfiguredPodDoesNotServeTheRoute() throws Exception {
        assertThat(status(config(false))).as("not registered: nothing to answer").isEqualTo(404);
    }

    @Test
    void aPodThatTurnedItOnServesIt() throws Exception {
        assertThat(status(config(true))).isEqualTo(200);
    }

    @Test
    void thePropertyTurnsItOnAndItsAbsenceLeavesItOff() {
        Map<String, String> on = minimal();
        on.put(ServerProperties.ADMIN_COST_ENABLED, "true");

        assertThat(ServerProperties.parse(on).adminCost()).isTrue();
        assertThat(ServerProperties.parse(minimal()).adminCost()).as("⚠️ OFF BY DEFAULT").isFalse();
    }

    private static int status(ServerConfig config) throws Exception {
        try (var store = new MemoryBinStore();
                var assembly = Assembly.open(config, store, noPeers(), Clock.systemUTC());
                var door = FrontDoor.start(assembly, Clock.systemUTC())) {
            return HttpClient.newHttpClient().send(HttpRequest.newBuilder()
                    .uri(URI.create("http://127.0.0.1:" + door.port() + AdminCostService.PATH
                            + "?top=10")).GET().build(),
                    HttpResponse.BodyHandlers.discarding()).statusCode();
        }
    }

    private static ServerConfig config(boolean adminCost) {
        return new ServerConfig("writera", "az-a", "cluster-a", "bins/cluster-a",
                new StoreConfig("memory", Optional.empty()), Duration.ofSeconds(10),
                Duration.ofSeconds(3), "http://writer-a:8080", IngestConfig.defaults("cluster-a"),
                0, "producer", Set.of("logs"),
                new RetentionConfig(Duration.ofMinutes(1), Duration.ofHours(2),
                        Duration.ofSeconds(10), Duration.ofHours(3), Duration.ofDays(1)),
                Optional.empty(), "uid-writera", CostTopKReporter.DEFAULT_INTERVAL,
                IndexQuotas.Config.none(), adminCost, java.util.Optional.empty());
    }

    private static Map<String, String> minimal() {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_UID, "uid-pod1");
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "memory");
        settings.put(ServerProperties.ENDPOINT, "http://pod1:8080");
        settings.put(ServerProperties.HTTP_PORT, "8080");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        return settings;
    }

    private static SequencerTransport noPeers() {
        return new SequencerTransport() {
            @Override
            public io.github.huyz0.os.biningester.format.CommitDelta send(
                    String endpoint, CommitRequest request) {
                throw new AssertionError("unexpected peer commit");
            }

            @Override
            public void close() {
            }
        };
    }
}
