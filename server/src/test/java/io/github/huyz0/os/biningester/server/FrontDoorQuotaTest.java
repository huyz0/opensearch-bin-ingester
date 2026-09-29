// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.ingest.CostTopKReporter;
import io.github.huyz0.os.biningester.ingest.IndexQuotas;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import io.helidon.http.HeaderNames;
import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The assembled front door admits {@code _bulk} through the quotas the pod was
 * CONFIGURED with (M11.8 review T4): a {@code BulkService} that can refuse by
 * quota is not a node that does.
 */
class FrontDoorQuotaTest {

    private static final String PREFIX = "bins/cluster-a";

    private static String body(int records) {
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < records; i++) {
            body.append("{\"index\":{\"_id\":\"d").append(i).append("\"}}\n{\"f\":1}\n");
        }
        return body.toString();
    }

    @Test
    void theAssembledFrontDoorRefusesAnIndexOverItsConfiguredQuota() throws Exception {
        ServerConfig config = new ServerConfig("writera", "az-a", "cluster-a", PREFIX,
                new StoreConfig("memory", Optional.empty()), Duration.ofSeconds(10),
                Duration.ofSeconds(3), "http://writer-a:8080", IngestConfig.defaults("cluster-a"),
                0, "producer", Set.of("logs"),
                new RetentionConfig(Duration.ofMinutes(1), Duration.ofHours(2),
                        Duration.ofSeconds(10), Duration.ofHours(3), Duration.ofDays(1)),
                Optional.empty(), "", CostTopKReporter.DEFAULT_INTERVAL,
                new IndexQuotas.Config(IndexQuotas.Limit.UNLIMITED,
                        Map.of("logs", new IndexQuotas.Limit(0, 1)), 8));
        SequencerTransport noPeers = new SequencerTransport() {
            @Override
            public io.github.huyz0.os.biningester.format.CommitDelta send(
                    String endpoint, CommitRequest request) {
                throw new AssertionError("unexpected peer commit");
            }

            @Override
            public void close() {
            }
        };
        try (var store = new MemoryBinStore();
                var assembly = Assembly.open(config, store, noPeers, Clock.systemUTC());
                var door = FrontDoor.start(assembly, Clock.systemUTC())) {
            assembly.catalog().register(new IndexRegistration("AAAAAAAAQACAAAAAAAAAqg", "logs",
                    List.of(), 4, 4, 1, 1));
            WebClient http = WebClient.builder().baseUri("http://127.0.0.1:" + door.port())
                    .build();

            try (HttpClientResponse first = http.post("/logs/_bulk")
                    .queryParam("partition", "0").submit(body(10))) {
                assertThat(first.status().code()).as("admitted with tokens, into debt")
                        .isEqualTo(202);
            }
            try (HttpClientResponse refused = http.post("/logs/_bulk")
                    .queryParam("partition", "0").submit(body(1))) {
                assertThat(refused.status().code())
                        .as("⚠️ THE CONFIGURED QUOTA REACHED THE NODE's FRONT DOOR")
                        .isEqualTo(429);
                assertThat(refused.headers().first(HeaderNames.RETRY_AFTER)).isPresent();
            }
        }
    }

    /**
     * M12.13 (review T1): the front door looks an index's aliases up in its
     * catalog, so an override keyed by the alias an operator writes through
     * limits the concrete index the pod charges.
     */
    @Test
    void anOverrideNamedByAnAliasReachesTheFrontDoor() throws Exception {
        ServerConfig config = new ServerConfig("writera", "az-a", "cluster-a", PREFIX,
                new StoreConfig("memory", Optional.empty()), Duration.ofSeconds(10),
                Duration.ofSeconds(3), "http://writer-a:8080", IngestConfig.defaults("cluster-a"),
                0, "producer", Set.of("logs"),
                new RetentionConfig(Duration.ofMinutes(1), Duration.ofHours(2),
                        Duration.ofSeconds(10), Duration.ofHours(3), Duration.ofDays(1)),
                Optional.empty(), "", CostTopKReporter.DEFAULT_INTERVAL,
                new IndexQuotas.Config(IndexQuotas.Limit.UNLIMITED,
                        Map.of("logs-write", new IndexQuotas.Limit(0, 1)), 8));
        SequencerTransport noPeers = new SequencerTransport() {
            @Override
            public io.github.huyz0.os.biningester.format.CommitDelta send(
                    String endpoint, CommitRequest request) {
                throw new AssertionError("unexpected peer commit");
            }

            @Override
            public void close() {
            }
        };
        try (var store = new MemoryBinStore();
                var assembly = Assembly.open(config, store, noPeers, Clock.systemUTC());
                var door = FrontDoor.start(assembly, Clock.systemUTC())) {
            assembly.catalog().register(new IndexRegistration("AAAAAAAAQACAAAAAAAAAqg", "logs",
                    List.of("logs-write"), 4, 4, 1, 1));
            WebClient http = WebClient.builder().baseUri("http://127.0.0.1:" + door.port())
                    .build();

            try (HttpClientResponse first = http.post("/logs/_bulk")
                    .queryParam("partition", "0").submit(body(10))) {
                assertThat(first.status().code()).isEqualTo(202);
            }
            try (HttpClientResponse refused = http.post("/logs/_bulk")
                    .queryParam("partition", "0").submit(body(1))) {
                assertThat(refused.status().code())
                        .as("⚠️ THE ALIAS's OVERRIDE LIMITS ITS INDEX AT THE NODE's FRONT DOOR")
                        .isEqualTo(429);
            }
        }
    }

}
