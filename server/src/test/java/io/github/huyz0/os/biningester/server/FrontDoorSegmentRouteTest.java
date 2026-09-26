// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.http.SegmentService;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import io.helidon.webclient.api.WebClient;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The assembled front door carries the proxy segment route, bound to this
 * deployment's prefix (M10.1, FR-6, ADR-0073).
 */
class FrontDoorSegmentRouteTest {

    private static final String PREFIX = "bins/cluster-a";

    @Test
    void theAssembledFrontDoorServesThisDeploymentsSegmentsAndNoOneElses() throws Exception {
        ServerConfig config = new ServerConfig("writera", "az-a", "cluster-a", PREFIX,
                new StoreConfig("memory", Optional.empty()), Duration.ofSeconds(10),
                Duration.ofSeconds(3), "http://writer-a:8080", IngestConfig.defaults("cluster-a"),
                0, "producer", Set.of("logs"));
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
        String ours = new SegmentKey(PREFIX, 1, "writera", 1, 48).key();
        String theirs = new SegmentKey("bins/cluster-b", 1, "writera", 1, 48).key();
        byte[] bytes = "segment-bytes".getBytes(StandardCharsets.UTF_8);
        try (var store = new MemoryBinStore()) {
            store.put(ours, Body.ofBytes(bytes));
            store.put(theirs, Body.ofBytes(bytes));
            try (var assembly = Assembly.open(config, store, noPeers, Clock.systemUTC());
                    var door = FrontDoor.start(assembly, Clock.systemUTC())) {
                WebClient client = WebClient.builder()
                        .baseUri("http://127.0.0.1:" + door.port()).build();
                try (var response = client.get(SegmentService.PATH)
                        .queryParam(SegmentService.KEY_PARAM, ours).request()) {
                    assertThat(response.status().code()).isEqualTo(200);
                    assertThat(response.entity().as(byte[].class)).isEqualTo(bytes);
                }
                try (var response = client.get(SegmentService.PATH)
                        .queryParam(SegmentService.KEY_PARAM, theirs).request()) {
                    assertThat(response.status().code())
                            .as("another deployment's prefix in the same bucket is refused")
                            .isEqualTo(400);
                }
            }
        }
    }
}
