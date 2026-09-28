// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.security.Principal;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * {@code GET /admin/cost} on an assembled pod answers from the pod's own
 * ledger after real writes (M11.4, M11 criterion 7, T2): the route's index
 * shares plus unattributed equal the pod's counted data and commit PUTs.
 */
class AdminCostAssemblyTest {

    private static final String PREFIX = "bins/cluster-a";
    private static final String INDEX = "logs";
    private static final String INDEX_UUID = base64Url(UUID.randomUUID());
    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of(INDEX));

    private static String base64Url(UUID uuid) {
        ByteBuffer b = ByteBuffer.allocate(16);
        b.putLong(uuid.getMostSignificantBits());
        b.putLong(uuid.getLeastSignificantBits());
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b.array());
    }

    private static ServerConfig config() {
        return new ServerConfig("pod1", "az-a", "cluster-a", PREFIX,
                new StoreConfig("memory", Optional.empty()),
                Duration.ofDays(1), Duration.ofSeconds(3), "http://pod1:8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", Set.of(INDEX),
                new RetentionConfig(Duration.ofMinutes(1), Duration.ofHours(2),
                        Duration.ofSeconds(10), Duration.ofHours(3), Duration.ofDays(1)));
    }

    private static SequencerTransport noPeers() {
        return new SequencerTransport() {
            @Override
            public CommitDelta send(String endpoint, CommitRequest request) {
                throw new UnsupportedOperationException("no peer expected: " + endpoint);
            }

            @Override
            public void close() {
            }
        };
    }

    private static java.math.BigDecimal requests(String json, String block, String charge) {
        java.util.regex.Matcher m = java.util.regex.Pattern.compile(
                java.util.regex.Pattern.quote(block) + "\\{[^}]*\"" + charge + "\":([0-9.]+)")
                .matcher(json);
        assertThat(m.find()).as("%s.%s in %s", block, charge, json).isTrue();
        return new java.math.BigDecimal(m.group(1));
    }

    @Test
    void theRouteAnswersFromThePodsLedgerAndItsSharesSumToThePodsPuts() throws Exception {
        try (BinStore shared = StoreFactory.open(new StoreConfig("memory", Optional.empty()));
                Assembly assembly = Assembly.open(config(), shared, noPeers(),
                        Clock.systemUTC());
                FrontDoor door = FrontDoor.start(assembly, Clock.systemUTC())) {
            assembly.catalog().register(
                    new IndexRegistration(INDEX_UUID, INDEX, List.of(), 4, 4, 1, 1));
            for (int i = 0; i < 3; i++) {
                String id = "doc-" + i;
                assembly.ingest().append(PRINCIPAL, INDEX, 0, sink -> sink.accept(
                        new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                                id.getBytes(StandardCharsets.UTF_8))));
            }

            String json;
            try (var response = io.helidon.webclient.api.WebClient.builder()
                    .baseUri("http://127.0.0.1:" + door.port()).build()
                    .get(io.github.huyz0.os.biningester.http.AdminCostService.PATH)
                    .queryParam("top", "10").request()) {
                assertThat(response.status().code()).isEqualTo(200);
                json = response.entity().as(String.class);
            }

            assertThat(json).as("the index, by its registered name")
                    .contains("\"index\":\"" + INDEX + "\"");
            long dataPuts = assembly.putPurposeCounts().dataPuts();
            long commitPuts = assembly.putPurposeCounts().commitPuts();
            assertThat(dataPuts).as("the premise: the appends were written").isPositive();
            assertThat(requests(json, "\"requests\":", "dataPut")
                    .add(requests(json, "\"unattributed\":", "dataPut")))
                    .as("⚠️ THE ROUTE's DATA PUTS SUM TO THE POD's COUNTED DATA PUTS")
                    .isEqualByComparingTo(java.math.BigDecimal.valueOf(dataPuts));
            assertThat(requests(json, "\"requests\":", "commitPut")
                    .add(requests(json, "\"unattributed\":", "commitPut")))
                    .as("and its commit PUTs to the pod's counted commit PUTs")
                    .isEqualByComparingTo(java.math.BigDecimal.valueOf(commitPuts));
            assertThat(json).contains("\"dataPuts\":" + dataPuts,
                    "\"commitPuts\":" + commitPuts);
        }
    }
}
