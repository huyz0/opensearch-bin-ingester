// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.ingest.CostTopKReporter;
import io.github.huyz0.os.biningester.ingest.IndexQuotas;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.ingest.LaneAdmission;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * M12.5 (M11 review F5): each {@code 429} the front door sends is counted
 * pod-level, lane admission and quota under their own unlabelled names, and a
 * quota refusal names its index for the next top-K cost line.
 */
class RefusalCountersTest {

    @Test
    void eachRefusalIsCountedUnderItsOwnUnlabelledNameAndAQuotaOneNamesItsIndex()
            throws Exception {
        try (var store = new MemoryBinStore();
                var assembly = Assembly.open(config(), store, noPeers(), Clock.systemUTC());
                var door = FrontDoor.start(assembly, Clock.systemUTC())) {
            assembly.catalog().register(new IndexRegistration("AAAAAAAAQACAAAAAAAAAqg", "logs",
                    List.of(), 4, 4, 1, 1));
            HttpClient client = HttpClient.newHttpClient();
            String before = scrape(client, door.port());

            assertThat(post(client, door.port(), 10)).as("admitted, into debt").isEqualTo(202);
            assertThat(post(client, door.port(), 1)).as("refused by its quota").isEqualTo(429);
            String afterQuota = scrape(client, door.port());
            assertThat(delta(before, afterQuota, RefusalMetrics.QUOTA_REFUSALS)).isEqualTo(1.0);
            assertThat(delta(before, afterQuota, RefusalMetrics.ADMISSION_REFUSALS))
                    .as("a quota refusal is not an admission one").isEqualTo(0.0);
            assertThat(assembly.refusedIndices().drain().names())
                    .as("named for the next top-K line").containsExactly("logs");

            List<LaneAdmission.Permit> held = new ArrayList<>();
            for (Optional<LaneAdmission.Permit> permit = door.laneAdmission().tryAcquire((byte) 0);
                    permit.isPresent(); permit = door.laneAdmission().tryAcquire((byte) 0)) {
                held.add(permit.get());
            }
            try {
                assertThat(post(client, door.port(), 1)).as("the pod's budget is held")
                        .isEqualTo(429);
            } finally {
                held.forEach(LaneAdmission.Permit::release);
            }
            String after = scrape(client, door.port());
            assertThat(delta(afterQuota, after, RefusalMetrics.ADMISSION_REFUSALS))
                    .isEqualTo(1.0);
            assertThat(delta(afterQuota, after, RefusalMetrics.QUOTA_REFUSALS)).isEqualTo(0.0);
            for (String name : List.of(RefusalMetrics.ADMISSION_REFUSALS,
                    RefusalMetrics.QUOTA_REFUSALS)) {
                assertThat(metricLine(after, name))
                        .as("⚠️ NO LABEL BEYOND HELIDON's SCOPE (cost.md rule 16)")
                        .startsWith(name + "{scope=\"application\",}");
            }
        }
    }

    /**
     * M13.10 (M12.5 review T2): a refusal POSTED THROUGH AN ALIAS names the
     * concrete index on the top-K line -- the name the quota charged, not the
     * one the producer sent.
     */
    @Test
    void aRefusalThroughAnAliasNamesTheConcreteIndex() throws Exception {
        try (var store = new MemoryBinStore();
                var assembly = Assembly.open(config(), store, noPeers(), Clock.systemUTC());
                var door = FrontDoor.start(assembly, Clock.systemUTC())) {
            assembly.catalog().register(new IndexRegistration("AAAAAAAAQACAAAAAAAAAqg", "logs",
                    List.of("logs-current"), 4, 4, 1, 1));
            HttpClient client = HttpClient.newHttpClient();

            assertThat(post(client, door.port(), "logs-current", 10))
                    .as("admitted through the alias, into debt").isEqualTo(202);
            assertThat(post(client, door.port(), "logs-current", 1))
                    .as("refused by the concrete index's quota").isEqualTo(429);
            assertThat(assembly.refusedIndices().drain().names())
                    .as("the concrete index, not the alias sent").containsExactly("logs");
        }
    }

    private static ServerConfig config() {
        return new ServerConfig("writera", "az-a", "cluster-a", "bins/cluster-a",
                new StoreConfig("memory", Optional.empty()), Duration.ofSeconds(10),
                Duration.ofSeconds(3), "http://writer-a:8080", IngestConfig.defaults("cluster-a"),
                0, "producer", Set.of("logs", "logs-current"),
                new RetentionConfig(Duration.ofMinutes(1), Duration.ofHours(2),
                        Duration.ofSeconds(10), Duration.ofHours(3), Duration.ofDays(1)),
                Optional.empty(), "", CostTopKReporter.DEFAULT_INTERVAL,
                new IndexQuotas.Config(IndexQuotas.Limit.UNLIMITED,
                        Map.of("logs", new IndexQuotas.Limit(0, 1)), 8), false);
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

    private static int post(HttpClient client, int port, int records) throws Exception {
        return post(client, port, "logs", records);
    }

    private static int post(HttpClient client, int port, String index, int records)
            throws Exception {
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < records; i++) {
            body.append("{\"index\":{\"_id\":\"d").append(i).append("\"}}\n{\"f\":1}\n");
        }
        return client.send(HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + "/" + index + "/_bulk?partition=0"))
                .POST(HttpRequest.BodyPublishers.ofString(body.toString())).build(),
                HttpResponse.BodyHandlers.discarding()).statusCode();
    }

    private static String scrape(HttpClient client, int port) throws Exception {
        HttpResponse<String> response = client.send(HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + port + "/observe/metrics?scope=application"))
                .GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return response.body();
    }

    private static double delta(String before, String after, String name) {
        return sample(after, name) - sample(before, name);
    }

    private static double sample(String exposition, String name) {
        String line = metricLine(exposition, name);
        return Double.parseDouble(line.substring(line.lastIndexOf(' ') + 1));
    }

    private static String metricLine(String exposition, String name) {
        return exposition.lines()
                .filter(each -> each.startsWith(name + "{") || each.startsWith(name + " "))
                .findFirst().orElseThrow(() -> new AssertionError("missing metric " + name));
    }
}
