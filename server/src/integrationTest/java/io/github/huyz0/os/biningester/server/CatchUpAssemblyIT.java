// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.CatchUpEndFrame;
import io.github.huyz0.os.biningester.format.CatchUpEventFrame;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Production catch-up route and composition-root proof (M8.24f). */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class CatchUpAssemblyIT {

    @TempDir
    Path dir;

    @Test
    void replayUsesTheProductionRouteAndMalformedVersionsDoNotReadTheStore() throws Exception {
        Path config = dir.resolve("node.properties");
        Files.writeString(config, String.join("\n",
                "pod.id=catchupnode",
                "pod.uid=uid-catchupnode",
                "pod.az=az-a",
                "trust.domain=cluster-a",
                "store.prefix=bins/cluster-a",
                "store.kind=memory",
                "endpoint=http://catchupnode:8080",
                "http.port=0",
                "ingest.interval-floor=PT5S",
                "producer.subject=producer-1",
                "producer.allowed-indices=logs",
                ""), StandardCharsets.UTF_8);

        UUID indexId = UUID.randomUUID();
        RunKey key = new RunKey(indexId, 3);
        RunKey otherKey = new RunKey(indexId, 2);
        ByteBuffer idBytes = ByteBuffer.allocate(16).putLong(indexId.getMostSignificantBits())
                .putLong(indexId.getLeastSignificantBits());
        String encodedIndexId = java.util.Base64.getUrlEncoder().withoutPadding()
                .encodeToString(idBytes.array());
        try (IngesterNode node = Main.run(config.toString())) {
            node.assembly().catalog().register(new IndexRegistration(
                    encodedIndexId, "logs", List.of(), 4, 4, 1, 1));
            HttpClient client = HttpClient.newHttpClient();
            URI base = URI.create("http://localhost:" + node.port());
            var firstWrite = client.sendAsync(HttpRequest.newBuilder(
                            base.resolve("/logs/_bulk?partition=3"))
                    .POST(HttpRequest.BodyPublishers.ofString(bulkBody("doc-1", "replay-me")))
                    .build(), HttpResponse.BodyHandlers.ofByteArray());
            var secondWrite = client.sendAsync(HttpRequest.newBuilder(
                            base.resolve("/logs/_bulk?partition=2"))
                    .POST(HttpRequest.BodyPublishers.ofString(bulkBody("doc-2", "shared-segment")))
                    .build(), HttpResponse.BodyHandlers.ofByteArray());
            assertThat(firstWrite.get(15, TimeUnit.SECONDS).statusCode()).isEqualTo(202);
            assertThat(secondWrite.get(15, TimeUnit.SECONDS).statusCode()).isEqualTo(202);

            var before = node.assembly().storeCounts();
            UUID requestId = UUID.randomUUID();
            byte[] request = new CatchUpRequestFrame(requestId,
                    List.of(new CatchUpRequestFrame.Stream(key, 0),
                            new CatchUpRequestFrame.Stream(otherKey, 0))).encode();
            HttpResponse<byte[]> replay = client.send(HttpRequest.newBuilder(
                            base.resolve("/sub/" + encodedIndexId + "/3"))
                    .POST(HttpRequest.BodyPublishers.ofByteArray(request)).build(),
                    HttpResponse.BodyHandlers.ofByteArray());

            assertThat(replay.statusCode()).isEqualTo(200);
            List<byte[]> frames = unframe(replay.body());
            assertThat(frames).hasSize(3);
            List<CatchUpEventFrame> events = List.of(
                    CatchUpEventFrame.decode(frames.get(0)),
                    CatchUpEventFrame.decode(frames.get(1)));
            assertThat(events).extracting(frame -> frame.event().key())
                    .containsExactlyInAnyOrder(key, otherKey);
            assertThat(events.get(0).event().segmentKey())
                    .isEqualTo(events.get(1).event().segmentKey());
            for (CatchUpEventFrame event : events) {
                RunKey eventKey = event.event().key();
                String expectedId = eventKey.equals(key) ? "doc-1" : "doc-2";
                String expectedPayload = eventKey.equals(key)
                        ? "{\"message\":\"replay-me\"}"
                        : "{\"message\":\"shared-segment\"}";
                assertThat(event.requestId()).isEqualTo(requestId);
                assertThat(event.event().firstOffset()).isZero();
                assertThat(event.event().recordCount()).isEqualTo(1);
                var reader = SegmentReader.open(event.event().inline());
                var run = reader.find(eventKey).orElseThrow();
                assertThat(reader.read(run)).singleElement().satisfies(record -> {
                    assertThat(record.id()).isEqualTo(expectedId);
                    assertThat(new String(record.payload(), StandardCharsets.UTF_8))
                            .isEqualTo(expectedPayload);
                });
            }
            assertThat(CatchUpEndFrame.decode(frames.get(2)).requestId()).isEqualTo(requestId);
            HttpResponse<byte[]> secondSurface = client.send(HttpRequest.newBuilder(
                            base.resolve("/ctl/catch-up"))
                    .POST(HttpRequest.BodyPublishers.ofByteArray(request)).build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            assertThat(secondSurface.statusCode())
                    .as("catch-up has no second unauthenticated URL")
                    .isEqualTo(404);
            var afterReplay = node.assembly().storeCounts();
            assertThat(afterReplay.gets() - before.gets()).isEqualTo(1);
            assertThat(afterReplay.lists() - before.lists()).isZero();

            byte[] unsupported = request.clone();
            unsupported[4] = 2;
            var beforeRefusals = node.assembly().storeCounts();
            HttpResponse<byte[]> unknownVersion = client.send(HttpRequest.newBuilder(
                            base.resolve("/sub/" + encodedIndexId + "/3"))
                    .POST(HttpRequest.BodyPublishers.ofByteArray(unsupported)).build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            HttpResponse<byte[]> malformed = client.send(HttpRequest.newBuilder(
                            base.resolve("/sub/" + encodedIndexId + "/3"))
                    .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[] {1, 2, 3})).build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            assertThat(unknownVersion.statusCode()).isEqualTo(400);
            assertThat(malformed.statusCode()).isEqualTo(400);
            var afterRefusals = node.assembly().storeCounts();
            assertThat(afterRefusals.gets() - beforeRefusals.gets()).isZero();
            assertThat(afterRefusals.lists() - beforeRefusals.lists()).isZero();
        }
    }

    @Test
    void productionRouteResumesAtPositivePersistedBatchStart() throws Exception {
        Path config = dir.resolve("resume-node.properties");
        Files.writeString(config, String.join("\n",
                "pod.id=catchupresume",
                "pod.uid=uid-catchupresume",
                "pod.az=az-a",
                "trust.domain=cluster-a",
                "store.prefix=bins/cluster-a",
                "store.kind=memory",
                "endpoint=http://catchupresume:8080",
                "http.port=0",
                "ingest.interval-floor=PT5S",
                "producer.subject=producer-1",
                "producer.allowed-indices=logs",
                ""), StandardCharsets.UTF_8);

        UUID indexId = UUID.randomUUID();
        RunKey key = new RunKey(indexId, 3);
        ByteBuffer idBytes = ByteBuffer.allocate(16).putLong(indexId.getMostSignificantBits())
                .putLong(indexId.getLeastSignificantBits());
        String encodedIndexId = java.util.Base64.getUrlEncoder().withoutPadding()
                .encodeToString(idBytes.array());
        try (IngesterNode node = Main.run(config.toString())) {
            node.assembly().catalog().register(new IndexRegistration(
                    encodedIndexId, "logs", List.of(), 4, 4, 1, 1));
            HttpClient client = HttpClient.newHttpClient();
            URI base = URI.create("http://localhost:" + node.port());
            HttpResponse<byte[]> write = client.send(HttpRequest.newBuilder(
                            base.resolve("/logs/_bulk?partition=3"))
                    .POST(HttpRequest.BodyPublishers.ofString(bulkBodyWithRecords(
                            "doc-0", "already-indexed", "doc-1", "resume-here", "doc-2", "resume-too")))
                    .build(), HttpResponse.BodyHandlers.ofByteArray());
            assertThat(write.statusCode()).isEqualTo(202);

            var before = node.assembly().storeCounts();
            UUID requestId = UUID.randomUUID();
            byte[] request = new CatchUpRequestFrame(requestId,
                    List.of(new CatchUpRequestFrame.Stream(key, 1))).encode();
            HttpResponse<byte[]> replay = client.send(HttpRequest.newBuilder(
                            base.resolve("/sub/" + encodedIndexId + "/3"))
                    .POST(HttpRequest.BodyPublishers.ofByteArray(request)).build(),
                    HttpResponse.BodyHandlers.ofByteArray());

            assertThat(replay.statusCode()).isEqualTo(200);
            List<byte[]> frames = unframe(replay.body());
            assertThat(frames).hasSize(2);
            CatchUpEventFrame event = CatchUpEventFrame.decode(frames.get(0));
            assertThat(event.requestId()).isEqualTo(requestId);
            assertThat(event.event().key()).isEqualTo(key);
            assertThat(event.event().firstOffset()).isEqualTo(1);
            assertThat(event.event().recordCount()).isEqualTo(2);
            var reader = SegmentReader.open(event.event().inline());
            var run = reader.find(key).orElseThrow();
            var records = reader.read(run);
            assertThat(records).hasSize(3);
            var resumed = records.subList((int) event.event().firstOffset(),
                    (int) event.event().firstOffset() + event.event().recordCount());
            assertThat(resumed).extracting(record -> record.id()).containsExactly("doc-1", "doc-2");
            assertThat(new String(resumed.get(0).payload(), StandardCharsets.UTF_8))
                    .isEqualTo("{\"message\":\"resume-here\"}");
            assertThat(new String(resumed.get(1).payload(), StandardCharsets.UTF_8))
                    .isEqualTo("{\"message\":\"resume-too\"}");
            assertThat(CatchUpEndFrame.decode(frames.get(1)).requestId()).isEqualTo(requestId);
            var after = node.assembly().storeCounts();
            assertThat(after.gets() - before.gets()).isEqualTo(1);
            assertThat(after.lists() - before.lists()).isZero();
        }
    }

    @Test
    void replayAcrossMultipleSegmentsStreamsPastEightMiB() throws Exception {
        Path config = dir.resolve("large-catchup-node.properties");
        Files.writeString(config, String.join("\n",
                "pod.id=catchuplarge",
                "pod.uid=uid-catchuplarge",
                "pod.az=az-a",
                "trust.domain=cluster-a",
                "store.prefix=bins/cluster-a",
                "store.kind=memory",
                "endpoint=http://catchuplarge:8080",
                "http.port=0",
                "ingest.interval-floor=PT5S",
                "producer.subject=producer-1",
                "producer.allowed-indices=logs",
                ""), StandardCharsets.UTF_8);

        UUID indexId = UUID.randomUUID();
        RunKey key = new RunKey(indexId, 3);
        ByteBuffer idBytes = ByteBuffer.allocate(16).putLong(indexId.getMostSignificantBits())
                .putLong(indexId.getLeastSignificantBits());
        String encodedIndexId = java.util.Base64.getUrlEncoder().withoutPadding()
                .encodeToString(idBytes.array());
        String largeValue = "x".repeat(4 << 20);
        try (IngesterNode node = Main.run(config.toString())) {
            node.assembly().catalog().register(new IndexRegistration(
                    encodedIndexId, "logs", List.of(), 4, 4, 1, 1));
            HttpClient client = HttpClient.newHttpClient();
            URI base = URI.create("http://localhost:" + node.port());
            for (String id : List.of("large-1", "large-2")) {
                String body = "{\"index\":{\"_id\":\"" + id + "\",\"_version\":1}}\n"
                        + "{\"message\":\"" + largeValue + "\"}\n";
                HttpResponse<byte[]> write = client.send(HttpRequest.newBuilder(
                                base.resolve("/logs/_bulk?partition=3"))
                        .POST(HttpRequest.BodyPublishers.ofString(body)).build(),
                        HttpResponse.BodyHandlers.ofByteArray());
                assertThat(write.statusCode()).isEqualTo(202);
            }

            var before = node.assembly().storeCounts();
            UUID requestId = UUID.randomUUID();
            byte[] request = new CatchUpRequestFrame(requestId,
                    List.of(new CatchUpRequestFrame.Stream(key, 0))).encode();
            HttpResponse<byte[]> replay = client.send(HttpRequest.newBuilder(
                            base.resolve("/sub/" + encodedIndexId + "/3"))
                    .POST(HttpRequest.BodyPublishers.ofByteArray(request)).build(),
                    HttpResponse.BodyHandlers.ofByteArray());

            assertThat(replay.statusCode()).isEqualTo(200);
            assertThat(replay.body().length).isGreaterThan(8 << 20);
            List<byte[]> frames = unframe(replay.body());
            assertThat(frames).hasSize(3);
            assertThat(CatchUpEventFrame.decode(frames.get(0)).event().firstOffset()).isZero();
            assertThat(CatchUpEventFrame.decode(frames.get(1)).event().firstOffset()).isEqualTo(1);
            assertThat(CatchUpEndFrame.decode(frames.get(2)).requestId()).isEqualTo(requestId);
            var after = node.assembly().storeCounts();
            assertThat(after.gets() - before.gets()).isEqualTo(2);
            assertThat(after.lists() - before.lists()).isZero();
        }
    }

    private static String bulkBody(String id, String message) {
        return "{\"index\":{\"_id\":\"" + id + "\",\"_version\":1}}\n"
                + "{\"message\":\"" + message + "\"}\n";
    }

    private static String bulkBodyWithRecords(String... idAndMessages) {
        if (idAndMessages.length == 0 || idAndMessages.length % 2 != 0) {
            throw new IllegalArgumentException("bulk records require id/message pairs");
        }
        StringBuilder body = new StringBuilder();
        for (int i = 0; i < idAndMessages.length; i += 2) {
            body.append(bulkBody(idAndMessages[i], idAndMessages[i + 1]));
        }
        return body.toString();
    }

    private static List<byte[]> unframe(byte[] body) {
        ByteBuffer input = ByteBuffer.wrap(body).order(ByteOrder.BIG_ENDIAN);
        List<byte[]> frames = new ArrayList<>();
        while (input.hasRemaining()) {
            assertThat(input.remaining()).isGreaterThanOrEqualTo(Integer.BYTES);
            int length = input.getInt();
            assertThat(length).isPositive().isLessThanOrEqualTo(input.remaining());
            byte[] frame = new byte[length];
            input.get(frame);
            frames.add(frame);
        }
        return List.copyOf(frames);
    }
}
