// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.DurableSegmentSignalFrame;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import io.helidon.webclient.api.WebClient;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class FrontDoorDurableSignalTest {
    @Test
    void durableSignalRouteRejectsAnUnlistedSocketSource() throws Exception {
        ServerConfig config = new ServerConfig("writera", "az-a", "cluster-a", "bins/cluster-a",
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
        try (var store = new MemoryBinStore();
                var assembly = Assembly.open(config, store, noPeers, Clock.systemUTC());
                var door = FrontDoor.start(assembly, Clock.systemUTC());
                var response = WebClient.builder().baseUri("http://127.0.0.1:" + door.port())
                        .build().post("/ctl/durable-segment")
                        .submit(new DurableSegmentSignalFrame("writerb", "az-b",
                                new SegmentKey("bins/cluster-a", 1, "writerb", 1, 48).key())
                                .encode())) {
            assertThat(response.status().code()).isEqualTo(403);
        }
    }
}
