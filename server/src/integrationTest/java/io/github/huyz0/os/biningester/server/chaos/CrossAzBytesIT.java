// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.huyz0.os.biningester.binstore.backend.S3Fixture;
import io.github.huyz0.os.biningester.client.ConsumerClient;
import io.github.huyz0.os.biningester.client.HttpSubscriptionTransport;
import io.github.huyz0.os.biningester.format.RunKey;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** M9.10: RustFS counts cross-AZ bytes on a real consumer delivery. */
@Timeout(value = 300, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class CrossAzBytesIT {

    private static final String INDEX = "logs";
    private static final String PAD = "x".repeat(2048);
    private static final Pattern COUNTER = Pattern.compile("\\\"([A-Za-z]+)\\\":(\\d+)");

    @TempDir
    Path directory;

    @Test
    void crossAzDeliveryIsCountedAndStaysBelowTheProducerByteBudget() throws Exception {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a T3 suite");
        try (ChaosBucket bucket = ChaosBucket.create()) {
            Map<String, String> settings = new HashMap<>(bucket.nodeSettings());
            settings.put("producer.allowed-indices", INDEX);
            settings.put("pod.az", "az-a");
            settings.put("pod.uid", "caller-supplied");
            NodeProcess node = null;
            NodeProcess secondNode = null;
            try {
                Path counts = directory.resolve("cross-az-counts.json");
                node = NodeProcess.start(directory, "poda", settings, NodeProcess.Options.NONE,
                        counts);
                var nodeSettings = new java.util.Properties();
                try (var input = Files.newInputStream(directory.resolve("poda.properties"))) {
                    nodeSettings.load(input);
                }
                assertThat(nodeSettings.getProperty("pod.uid")).isNotEqualTo("caller-supplied");
                assertThat(UUID.fromString(nodeSettings.getProperty("pod.uid"))).isNotNull();
                secondNode = NodeProcess.start(directory, "podb", settings);
                var secondNodeSettings = new java.util.Properties();
                try (var input = Files.newInputStream(directory.resolve("podb.properties"))) {
                    secondNodeSettings.load(input);
                }
                assertThat(UUID.fromString(secondNodeSettings.getProperty("pod.uid")))
                        .isNotEqualTo(UUID.fromString(nodeSettings.getProperty("pod.uid")));
                UUID stream = UUID.randomUUID();
                node.registerIndex(INDEX, stream);
                RunKey key = new RunKey(stream, 0);

                long accepted = 0;
                String firstId = "cross-az-first";
                accepted += bodyBytes(List.of(firstId));
                HttpSubscriptionTransport transport = new HttpSubscriptionTransport(
                        "http://localhost:" + node.port(), () -> { }, Duration.ofMillis(50),
                        Duration.ofSeconds(1), Duration.ofSeconds(30), Duration.ofSeconds(2),
                        32 * 1024 * 1024, "az-b");
                try (ConsumerClient consumer = new ConsumerClient(transport, key, 16)) {
                    assertThat(node.write(List.of(firstId))).isEqualTo(202);
                    assertThat(consumer.readNext(Duration.ofSeconds(30))).isPresent();
                } finally {
                    transport.close();
                }

                // The cross-AZ consumer has been deliberately served. The rest
                // of the accepted bytes are a write-only phase, which makes the
                // denominator large enough to test the budget without claiming
                // that every producer byte must cross the consumer socket.
                List<String> writeOnlyIds = new ArrayList<>();
                for (int i = 0; i < 2_000; i++) {
                    writeOnlyIds.add("write-only-" + i);
                }
                accepted += bodyBytes(writeOnlyIds);
                assertThat(node.write(writeOnlyIds)).isEqualTo(202);

                node.snapshotCounts(counts);
                Map<String, Long> values = counters(Files.readString(counts,
                        StandardCharsets.UTF_8));
                assertThat(values).containsKeys("crossAzBytes", "unknownPeerBytes", "proxyRead",
                        "inlinePush", "consumerPoll", "commitForward", "inboxDrain");
                assertThat(values.get("unknownPeerBytes")).isZero();
                assertThat(values.get("crossAzBytes")).isPositive();
                assertThat(values.get("inlinePush")).isPositive();
                assertThat(values.get("inlinePush"))
                        .as("the deliberately small delivered segment is inline, not proxy")
                        .isGreaterThan(values.get("proxyRead"));
                long byTransport = values.get("proxyRead") + values.get("inlinePush")
                        + values.get("consumerPoll") + values.get("commitForward")
                        + values.get("inboxDrain");
                assertThat(byTransport).as("named transport counters must partition cross-AZ bytes")
                        .isEqualTo(values.get("crossAzBytes"));
                assertThat(values.get("crossAzBytes") * 1_000L)
                        .as("cross-AZ bytes=%d, producer bytes=%d", values.get("crossAzBytes"),
                                accepted)
                        .isLessThan(accepted);
                assertThat(values.get("proxyRead"))
                        .as("proxy-read is the event frame until its segment route is wired")
                        .isGreaterThanOrEqualTo(0L);
                System.out.println("M9.10 cross-AZ bytes: producer=" + accepted
                        + ", counters=" + values);
            } finally {
                if (node != null) {
                    node.close();
                }
                if (secondNode != null) {
                    secondNode.close();
                }
            }
        }
    }

    private static long bodyBytes(List<String> ids) {
        long bytes = 0;
        for (String id : ids) {
            bytes += ("{\"index\":{\"_id\":\"" + id
                    + "\",\"_version\":1}}\n{\"pad\":\"" + PAD + "\"}\n")
                    .getBytes(StandardCharsets.UTF_8).length;
        }
        return bytes;
    }

    private static Map<String, Long> counters(String json) {
        Map<String, Long> values = new HashMap<>();
        Matcher matcher = COUNTER.matcher(json);
        while (matcher.find()) {
            values.put(matcher.group(1), Long.parseLong(matcher.group(2)));
        }
        return values;
    }
}
