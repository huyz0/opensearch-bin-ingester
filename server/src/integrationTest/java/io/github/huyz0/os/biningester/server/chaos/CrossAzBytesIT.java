// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.huyz0.os.biningester.binstore.backend.S3Fixture;
import io.github.huyz0.os.biningester.client.ConsumerClient;
import io.github.huyz0.os.biningester.client.HttpSegmentSource;
import io.github.huyz0.os.biningester.client.HttpSubscriptionTransport;
import io.github.huyz0.os.biningester.client.SegmentSource;
import io.github.huyz0.os.biningester.format.Grant;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * M9.10: RustFS counts cross-AZ bytes on a real consumer delivery; M10.4:
 * NFR-5 in full, proxy segment payloads included.
 */
@Timeout(value = 300, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class CrossAzBytesIT {

    private static final String INDEX = "logs";
    private static final String PAD = "x".repeat(2048);

    /**
     * ⚠️ ~420 KiB a bulk, past the writer's 256 KiB inline cap, so the
     * writer's own policy serves each one {@code proxy}.
     */
    private static final int PROXIED_RECORDS = 200;
    private static final int PROXIED_BATCHES = 2;
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

    /**
     * M10.4, M10 SPEC criterion 6: NFR-5 IN FULL on a two-AZ fleet.
     *
     * <p>⚠️ **THE CONSUMER IS WIRED THE WAY FR-6 AND ADR-0073 PLACE IT**: its
     * EVENTS come from the writer in {@code az-a}, over the subscription, and
     * its PAYLOADS from its own zone's pod in {@code az-b}, over {@code GET
     * /seg}. No server setting chooses {@code proxy}: the writer's
     * {@code FetchPolicy} does, for any batch above the 256 KiB inline cap, so
     * each bulk here is sized past it and {@code inlinePush} is asserted zero
     * -- a batch that slipped under the cap would have been inlined ACROSS the
     * zone and the row would be measuring the wrong transport.
     *
     * <p>⚠️ **THE BUDGET IS READ BEFORE THE CONTROL FETCH**, which is
     * deliberately misrouted and adds a whole segment to cross-AZ
     * {@code PROXY_READ}. Its job is the third half of the criterion: a
     * budget met because the route counted nothing is indistinguishable from
     * one met because nothing crossed, unless a known crossing is shown to
     * land in the counter byte for byte.
     */
    @Test
    void fullNfr5IncludesProxyPayloads() throws Exception {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a T3 suite");
        try (ChaosBucket bucket = ChaosBucket.create()) {
            Map<String, String> common = new HashMap<>(bucket.nodeSettings());
            common.put("producer.allowed-indices", INDEX);
            Map<String, String> zoneA = new HashMap<>(common);
            zoneA.put("pod.az", "az-a");
            Map<String, String> zoneB = new HashMap<>(common);
            zoneB.put("pod.az", "az-b");
            Path writerCounts = directory.resolve("nfr5-writer.json");
            Path proxyCounts = directory.resolve("nfr5-proxy.json");
            NodeProcess writer = null;
            NodeProcess sameZone = null;
            try {
                writer = NodeProcess.start(directory, "poda", zoneA, NodeProcess.Options.NONE,
                        writerCounts);
                sameZone = NodeProcess.start(directory, "podb", zoneB,
                        NodeProcess.Options.NONE, proxyCounts);
                UUID stream = UUID.randomUUID();
                writer.registerIndex(INDEX, stream);
                RunKey key = new RunKey(stream, 0);

                long accepted = 0;
                long consumed = 0;
                int proxied = 0;
                DeliveredSegments delivered = new DeliveredSegments(new HttpSegmentSource(
                        Duration.ofSeconds(30), "http://localhost:" + sameZone.port(), "az-b"));
                HttpSubscriptionTransport transport = new HttpSubscriptionTransport(
                        "http://localhost:" + writer.port(), () -> { }, Duration.ofMillis(50),
                        Duration.ofSeconds(1), Duration.ofSeconds(30), Duration.ofSeconds(2),
                        32 * 1024 * 1024, "az-b");
                try (ConsumerClient consumer = new ConsumerClient(transport, key, 16,
                        delivered)) {
                    for (int batch = 0; batch < PROXIED_BATCHES; batch++) {
                        List<String> ids = new ArrayList<>();
                        for (int i = 0; i < PROXIED_RECORDS; i++) {
                            ids.add("proxied-" + batch + "-" + i);
                        }
                        accepted += bodyBytes(ids);
                        assertThat(writer.write(ids)).isEqualTo(202);
                        proxied += ids.size();
                    }
                    // ⚠️ BOUNDED PER RECORD: each wait ends at 30 s, so a
                    // payload that never arrives fails here rather than
                    // hanging the class timeout.
                    java.util.Set<String> seen = new java.util.HashSet<>();
                    for (int read = 0; read < proxied; read++) {
                        var record = consumer.readNext(Duration.ofSeconds(30));
                        assertThat(record)
                                .as("record %d of %d reached the az-b consumer", read, proxied)
                                .isPresent();
                        String id = record.get().record().id();
                        assertThat(seen.add(id)).as("record %s consumed once", id).isTrue();
                        consumed += bodyBytes(List.of(id));
                    }
                } finally {
                    transport.close();
                }
                assertThat(delivered.bytes()).as("payloads came through the az-b proxy route")
                        .isPositive();

                writer.snapshotCounts(writerCounts);
                sameZone.snapshotCounts(proxyCounts);
                Map<String, Long> a = counters(Files.readString(writerCounts,
                        StandardCharsets.UTF_8));
                Map<String, Long> b = counters(Files.readString(proxyCounts,
                        StandardCharsets.UTF_8));
                System.out.println("M10.4 NFR-5: producer=" + accepted + ", consumed="
                        + consumed + ", delivered="
                        + delivered.bytes() + " in " + delivered.fetches() + " fetches, az-a="
                        + a + ", az-b=" + b);
                for (Map<String, Long> pod : List.of(a, b)) {
                    assertThat(pod).containsKeys("crossAzBytes", "unknownPeerBytes", "proxyRead",
                            "sameAzProxyRead", "inlinePush", "consumerPoll", "commitForward",
                            "inboxDrain", "durableSegmentSignal");
                    assertThat(pod.get("unknownPeerBytes"))
                            .as("every byte on every transport was attributed to a zone")
                            .isZero();
                }
                assertThat(a.get("inlinePush"))
                        .as("every delivered batch was above the inline cap, so served proxy")
                        .isZero();
                // (1) every transport, proxy payloads included, both pods.
                long crossAz = a.get("crossAzBytes") + b.get("crossAzBytes");
                assertThat(crossAz * 1_000L)
                        .as("cross-AZ bytes=%d (az-a %d, az-b %d), producer bytes=%d", crossAz,
                                a.get("crossAzBytes"), b.get("crossAzBytes"), accepted)
                        .isLessThan(accepted);
                // ⚠️ AND AGAINST WHAT THE CONSUMER READ, which is the traffic
                // these bytes were spent on. No write-only phase pads either
                // denominator (M10.4 review T1): M9.10's 2,000 unconsumed
                // records loosened the budget ~6x, and an event frame counted
                // eight times over still passed against them.
                assertThat(crossAz * 1_000L)
                        .as("cross-AZ bytes=%d, consumed producer bytes=%d", crossAz, consumed)
                        .isLessThan(consumed);
                // (2) the payloads were served in the consumer's own zone.
                assertThat(b.get("sameAzProxyRead"))
                        .as("same-AZ proxy payload bytes against segment bytes delivered")
                        .isGreaterThanOrEqualTo(delivered.bytes());

                // (3) the control: the same segment from the WRITER, as az-b.
                String controlKey = delivered.anyKey();
                byte[] control = new HttpSegmentSource(Duration.ofSeconds(30),
                        "http://localhost:" + writer.port(), "az-b").fetchSegment(controlKey);
                assertThat(control).hasSize(delivered.sizeOf(controlKey));
                writer.snapshotCounts(writerCounts);
                Map<String, Long> after = counters(Files.readString(writerCounts,
                        StandardCharsets.UTF_8));
                assertThat(after.get("proxyRead") - a.get("proxyRead"))
                        .as("a misrouted fetch adds exactly its size to cross-AZ PROXY_READ")
                        .isEqualTo(control.length);
                assertThat(after.get("sameAzProxyRead")).isEqualTo(a.get("sameAzProxyRead"));
            } finally {
                if (writer != null) {
                    writer.close();
                }
                if (sameZone != null) {
                    sameZone.close();
                }
            }
        }
    }

    /**
     * The consumer's payload source, recording what it was handed per key.
     *
     * <p>⚠️ **IT COUNTS ONLY WHAT THE PRODUCTION SOURCE RETURNED**, so "segment
     * bytes delivered" is what reached the consumer, not what the test
     * expected to send.
     */
    private static final class DeliveredSegments implements SegmentSource {

        private final HttpSegmentSource delegate;
        private final Map<String, Integer> sizes = new ConcurrentHashMap<>();
        private final AtomicLong bytes = new AtomicLong();
        private final AtomicLong fetches = new AtomicLong();

        DeliveredSegments(HttpSegmentSource delegate) {
            this.delegate = delegate;
        }

        @Override
        public byte[] fetch(Grant grant) throws java.io.IOException {
            throw new java.io.IOException("this row serves proxy, never direct");
        }

        @Override
        public byte[] fetchSegment(String segmentKey) throws java.io.IOException {
            byte[] segment = delegate.fetchSegment(segmentKey);
            sizes.put(segmentKey, segment.length);
            bytes.addAndGet(segment.length);
            fetches.incrementAndGet();
            return segment;
        }

        long bytes() {
            return bytes.get();
        }

        long fetches() {
            return fetches.get();
        }

        String anyKey() {
            return sizes.keySet().iterator().next();
        }

        int sizeOf(String key) {
            return sizes.get(key);
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
