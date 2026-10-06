// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.http.AdminCostService;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.security.Principal;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
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
 * M12.8 (M11.4 P1): one registration whose index UUID cannot be decoded made
 * the catalog's names, and so the whole {@code /admin/cost} report, throw --
 * while the write path fails only that index's writes. The route answers
 * {@code 200}, names the others, and lists the bad one by its raw id.
 */
class AdminCostMalformedRegistrationTest {

    private static final String PREFIX = "bins/cluster-a";
    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of("logs", "broken"));

    @Test
    void theRouteAnswersNamingTheOthersAndTheBadOneByItsId() throws Exception {
        try (var store = new MemoryBinStore();
                var assembly = Assembly.open(config(), store, noPeers(), Clock.systemUTC());
                var door = FrontDoor.start(assembly, Clock.systemUTC())) {
            assembly.catalog().register(new IndexRegistration(base64Url(UUID.randomUUID()),
                    "logs", List.of(), 4, 4, 1, 1));
            assembly.catalog().register(new IndexRegistration("not-a-uuid", "broken",
                    List.of(), 4, 4, 1, 1));
            assembly.ingest().append(PRINCIPAL, "logs", 0, sink -> sink.accept(
                    new SegmentRecord("d", OpType.INDEX, OptionalLong.of(1),
                            "d".getBytes(StandardCharsets.UTF_8))));

            HttpResponse<String> response = HttpClient.newHttpClient().send(HttpRequest
                    .newBuilder().uri(URI.create("http://127.0.0.1:" + door.port()
                            + AdminCostService.PATH + "?top=10")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).as("the decodable index is still named")
                    .contains("\"index\":\"logs\"")
                    .as("⚠️ AND THE BAD ONE BY ITS ID, not hidden (M12.8 review P1)")
                    .contains("\"undecodable\":[\"not-a-uuid\"]");
        }
    }

    private static ServerConfig config() {
        return new ServerConfig("pod1", "az-a", "cluster-a", PREFIX,
                new StoreConfig("memory", Optional.empty()),
                Duration.ofDays(1), Duration.ofSeconds(3), "http://pod1:8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", Set.of("logs", "broken"),
                RetentionConfig.defaults(), Optional.empty(), "uid-pod1",
                io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL,
                io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), true, java.util.Optional.empty(), PeerConfig.off(0));
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

    private static String base64Url(UUID uuid) {
        ByteBuffer b = ByteBuffer.allocate(16);
        b.putLong(uuid.getMostSignificantBits());
        b.putLong(uuid.getLeastSignificantBits());
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b.array());
    }
}
