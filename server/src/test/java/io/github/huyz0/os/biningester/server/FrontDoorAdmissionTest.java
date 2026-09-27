// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.ingest.LaneAdmission;
import io.github.huyz0.os.biningester.ingest.LaneSet;
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
 * The assembled front door admits {@code _bulk} through the POD's lane
 * admission, built from the configured budget and active set (M10.8): a
 * {@code BulkService} that can refuse 429 is not a node that does.
 */
class FrontDoorAdmissionTest {

    private static final String PREFIX = "bins/cluster-a";

    private static int post(WebClient http, int lane) {
        try (HttpClientResponse answer = http.post("/logs/_bulk")
                .queryParam("partition", "0").queryParam("lane", Integer.toString(lane))
                .submit("{\"index\":{\"_id\":\"a\"}}\n{\"f\":1}\n")) {
            return answer.status().code();
        }
    }

    @Test
    void theAssembledFrontDoorAdmitsByTheConfiguredBudgetAndLanes() throws Exception {
        IngestConfig d = IngestConfig.defaults("cluster-a");
        // ⚠️ A BUDGET OF 6 OVER {0, 1}: floors 1 and 2. At the default 256 no
        // held permit below refuses anything, and over the default -2..2 the
        // same budget gives lane 1 a floor of 1 -- so each half of the
        // configuration is told apart by one of the two answers below.
        IngestConfig ingest = new IngestConfig(d.intervalFloor(), d.maxSegmentBytes(),
                d.trustDomain(), d.maxQueuedPushBytes(), d.intervalCeiling(),
                d.fillRatioLowThreshold(), d.fillRatioHighThreshold(),
                d.intervalLengthenDelay(), d.intervalShortenDelay(), false,
                LaneSet.of((byte) 0, (byte) 1), 6);
        ServerConfig config = new ServerConfig("writera", "az-a", "cluster-a", PREFIX,
                new StoreConfig("memory", Optional.empty()), Duration.ofSeconds(10),
                Duration.ofSeconds(3), "http://writer-a:8080", ingest,
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
                var door = FrontDoor.start(assembly, Clock.systemUTC())) {
            WebClient http = WebClient.builder().baseUri("http://127.0.0.1:" + door.port())
                    .build();
            LaneAdmission pod = door.laneAdmission();
            for (int i = 0; i < 6; i++) {
                pod.tryAcquire((byte) 0).orElseThrow();
            }
            pod.tryAcquire((byte) 1).orElseThrow();

            assertThat(post(http, 0)).as("the configured budget of 6 is saturated")
                    .isEqualTo(429);
            // ⚠️ NOT 429 IS THE WHOLE CLAIM: past admission, an explicit write to
            // an index nobody registered is answered 500 today (M10.30).
            assertThat(post(http, 1)).as("lane 1's configured floor is 2, and it holds 1")
                    .isNotEqualTo(429);
        }
    }
}
