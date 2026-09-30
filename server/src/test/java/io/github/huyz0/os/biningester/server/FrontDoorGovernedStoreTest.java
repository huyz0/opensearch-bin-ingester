// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.SegmentFetchRoute;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.http.HttpSequencerTransport;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Inbox;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import io.helidon.webclient.api.HttpClientResponse;
import io.helidon.webclient.api.WebClient;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * The front door's store requests go through the node's own store stack
 * (M10.26): counted into {@code storeCounts()} and seen by the governor.
 *
 * <p>⚠️ **BEFORE M10.26 BOTH WENT TO THE RAW BACKEND**, so the commit route's
 * inbox drain LIST and the fetch route's absent-key {@code stat} reached the
 * store without being counted or governed -- a regression there was invisible
 * to the governor and to the pod's own request figures.
 */
class FrontDoorGovernedStoreTest {

    private static final String PREFIX = "bins/cluster-a";

    private static ServerConfig config() {
        return new ServerConfig("writera", "az-a", "cluster-a", PREFIX,
                new StoreConfig("memory", Optional.empty()), Duration.ofSeconds(10),
                Duration.ofSeconds(3), "http://writer-a:8080", IngestConfig.defaults("cluster-a"),
                0, "producer", Set.of("logs"), RetentionConfig.defaults(), java.util.Optional.empty(), "", io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL, io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false);
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

    @Test
    void theCommitRoutesInboxDrainListIsCountedAndDeclaredRecovery() throws Exception {
        ObservedStore store = new ObservedStore(Inbox.prefixFor(PREFIX));
        try (var assembly = Assembly.open(config(), store, noPeers(), Clock.systemUTC());
                var door = FrontDoor.start(assembly, Clock.systemUTC())) {
            store.awaitInboxListed();
            long nodeListsBefore = assembly.storeCounts().lists();
            long reachedBefore = store.listsUnder(Inbox.prefixFor(PREFIX));
            long recoveryBefore = assembly.governor().counts().recoveryLists();

            WebClient http = WebClient.builder().baseUri("http://127.0.0.1:" + door.port())
                    .build();
            try (HttpClientResponse drained = http.post(HttpSequencerTransport.DRAIN_PATH)
                    .queryParam("pod", "podz").request()) {
                assertThat(drained.status().code()).as("the node holds the term").isEqualTo(200);
            }

            long reached = store.listsUnder(Inbox.prefixFor(PREFIX)) - reachedBefore;
            assertThat(reached).as("the drain listed the inbox").isPositive();
            assertThat(assembly.storeCounts().lists() - nodeListsBefore)
                    .as("⚠️ EVERY INBOX LIST THAT REACHED THE STORE IS IN THE NODE'S COUNT: "
                            + "through the raw backend it was none")
                    .isEqualTo(reached);
            assertThat(assembly.governor().counts().recoveryLists() - recoveryBefore)
                    .as("⚠️ AND THE GOVERNOR SAW IT AS DECLARED RECOVERY, never refusable")
                    .isEqualTo(reached);
        }
    }

    @Test
    void theFetchRoutesAbsentKeyStatIsCounted() throws Exception {
        ObservedStore store = new ObservedStore(Inbox.prefixFor(PREFIX));
        String absent = new SegmentKey(PREFIX, 1, "writera", 9, 48).key();
        try (var assembly = Assembly.open(config(), store, noPeers(), Clock.systemUTC());
                var door = FrontDoor.start(assembly, Clock.systemUTC())) {
            store.awaitInboxListed();
            long statsBefore = assembly.storeCounts().stats();

            WebClient http = WebClient.builder().baseUri("http://127.0.0.1:" + door.port())
                    .build();
            try (HttpClientResponse missing = http.get(SegmentFetchRoute.PATH)
                    .queryParam(SegmentFetchRoute.KEY_PARAM, absent).request()) {
                assertThat(missing.status().code()).isEqualTo(404);
            }

            assertThat(assembly.storeCounts().stats() - statsBefore)
                    .as("⚠️ THE ABSENT-KEY STAT IS IN THE NODE'S COUNT: through the raw "
                            + "backend it was not")
                    .isEqualTo(1);
        }
    }
}
