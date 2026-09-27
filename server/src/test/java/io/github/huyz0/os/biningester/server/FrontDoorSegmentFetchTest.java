// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.client.SegmentFetchRoute;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The assembled front door serves the proxy segment route (M10.1): the route
 * existing as a class is not the route existing on a node.
 */
class FrontDoorSegmentFetchTest {

    private static final String PREFIX = "bins/cluster-a";

    @Test
    void theAssembledFrontDoorServesADurableSegmentAndRefusesAControlKey() throws Exception {
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
        String key = new SegmentKey(PREFIX, 1, "writera", 7, 48).key();
        byte[] segment = "not decoded by the route, served as stored".getBytes();
        try (var store = new MemoryBinStore();
                var assembly = Assembly.open(config, store, noPeers, Clock.systemUTC());
                var door = FrontDoor.start(assembly, Clock.systemUTC())) {
            store.put(key, Body.ofBytes(segment));
            WebClient http = WebClient.builder().baseUri("http://127.0.0.1:" + door.port())
                    .build();
            try (HttpClientResponse served = http.get(SegmentFetchRoute.PATH)
                    .queryParam(SegmentFetchRoute.KEY_PARAM, key).request()) {
                assertThat(served.status().code()).isEqualTo(200);
                assertThat(served.entity().as(byte[].class)).isEqualTo(segment);
            }
            try (HttpClientResponse refused = http.get(SegmentFetchRoute.PATH)
                    .queryParam(SegmentFetchRoute.KEY_PARAM, PREFIX + "/ctl/lease/0.json")
                    .request()) {
                assertThat(refused.status().code()).isEqualTo(400);
            }
        }
    }

    @Test
    void theRouteCountsIntoThePodsOwnCounterAndReadsThroughThePodsOwnCache() throws Exception {
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
        String key = new SegmentKey(PREFIX, 1, "writera", 8, 48).key();
        byte[] segment = new byte[5_000];
        CrossAzBytes pods = new CrossAzBytes("az-a");
        try (var store = new MemoryBinStore();
                var assembly = Assembly.open(config, store, noPeers, Clock.systemUTC());
                var door = FrontDoor.start(assembly, Clock.systemUTC(), line -> { }, pods)) {
            store.put(key, Body.ofBytes(segment));
            WebClient http = WebClient.builder().baseUri("http://127.0.0.1:" + door.port())
                    .build();
            long getsBefore = assembly.storeCounts().gets();
            for (int i = 0; i < 2; i++) {
                try (HttpClientResponse served = http.get(SegmentFetchRoute.PATH)
                        .queryParam(SegmentFetchRoute.KEY_PARAM, key)
                        .queryParam(SegmentFetchRoute.AZ_PARAM, "az-b").request()) {
                    assertThat(served.entity().as(byte[].class)).isEqualTo(segment);
                }
            }

            assertThat(pods.crossAzBytes(CrossAzBytes.Transport.PROXY_READ))
                    .as("⚠️ THE POD'S NFR-5 COUNTER: a counter of the route's own would "
                            + "leave the pod's figure showing no proxy bytes at all")
                    .isEqualTo(2L * segment.length);
            assertThat(assembly.storeCounts().gets() - getsBefore)
                    .as("the node-wide cache: two fetches, one store read")
                    .isEqualTo(1);
        }
    }
}
