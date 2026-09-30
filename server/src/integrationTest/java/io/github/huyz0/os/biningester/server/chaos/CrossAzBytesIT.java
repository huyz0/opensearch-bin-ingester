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

    /**
     * ⚠️ ~230 KB a bulk, one stream: under the 256 KiB inline cap and above
     * the event floor for K = 1 (~166 KB; it is K x 166 KB for K streams).
     */
    private static final int SUB_CAP_RECORDS = 110;
    private static final int SUB_CAP_BATCHES = 3;
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
                // ⚠️ THE SECOND POD IS THE CONSUMER'S OWN ZONE (M10.33): since
                // ADR-0076 an az-b consumer is served `proxy`, never inline,
                // so it needs an az-b route to fetch its payload from.
                Map<String, String> zoneB = new HashMap<>(settings);
                zoneB.put("pod.az", "az-b");
                secondNode = NodeProcess.start(directory, "podb", zoneB);
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
                DeliveredSegments delivered = new DeliveredSegments(new HttpSegmentSource(
                        Duration.ofSeconds(30), "http://localhost:" + secondNode.port(), "az-b"));
                try (ConsumerClient consumer = new ConsumerClient(transport, key, 16,
                        delivered, io.github.huyz0.os.biningester.client.SegmentFetchRetry.standard(System::currentTimeMillis))) {
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
                assertThat(values.get("inlinePush"))
                        .as("a consumer in another zone is never inlined (ADR-0076)")
                        .isZero();
                assertThat(values.get("proxyRead"))
                        .as("the small delivered segment is proxy: its event frame crossed")
                        .isPositive();
                assertThat(delivered.fetches())
                        .as("and its payload came from the az-b pod's route")
                        .isPositive();
                long byTransport = values.get("proxyRead") + values.get("inlinePush")
                        + values.get("consumerPoll") + values.get("commitForward")
                        + values.get("inboxDrain");
                assertThat(byTransport).as("named transport counters must partition cross-AZ bytes")
                        .isEqualTo(values.get("crossAzBytes"));
                assertThat(values.get("crossAzBytes") * 1_000L)
                        .as("cross-AZ bytes=%d, producer bytes=%d", values.get("crossAzBytes"),
                                accepted)
                        .isLessThan(accepted);
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
                        delivered, io.github.huyz0.os.biningester.client.SegmentFetchRetry.standard(System::currentTimeMillis))) {
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
     * M10.33, ADR-0076: NFR-5 BELOW THE INLINE CAP, the case M10.4 sized past.
     *
     * <p>⚠️ **EACH BULK IS SUB-CAP**, ~230 KB against the writer's 256 KiB
     * inline cap, and every delivered segment is asserted under it -- so the
     * writer's size policy answers {@code inline} for every one. Before
     * ADR-0076 that answer went to the az-b consumer too, payload included,
     * and its cross-AZ bytes equalled its payload bytes (M10 review F1). Now
     * the writer tells it {@code proxy} with no bytes and the payload comes
     * from the az-b pod's {@code /seg}.
     *
     * <p>⚠️ **NOT A SMALL BULK, AND THAT IS THE MEASUREMENT, NOT A DODGE.**
     * What still crosses is ~166 B of event PER STREAM SESSION per segment
     * (499 B over 3 segments here), because one event goes to each
     * {@code id@RunKey} session. So 0.1% holds only above ~K x 166 KB, where K
     * is the number of cross-zone-served streams in the segment: an 8 MiB
     * segment of 400 such streams sends ~66 KB of events, 0.79%, about 8x
     * NFR-5. ⚠️ THIS CASE HAS ONE STREAM, K = 1, and proves the payload term
     * only; the per-stream floor is not closed by ADR-0076. The size is chosen
     * to sit between the K = 1 floor and the cap, which is the band F1 broke.
     */
    @Test
    void nfr5HoldsBelowTheInlineCap() throws Exception {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a T3 suite");
        try (ChaosBucket bucket = ChaosBucket.create()) {
            Map<String, String> common = new HashMap<>(bucket.nodeSettings());
            common.put("producer.allowed-indices", INDEX);
            Map<String, String> zoneA = new HashMap<>(common);
            zoneA.put("pod.az", "az-a");
            Map<String, String> zoneB = new HashMap<>(common);
            zoneB.put("pod.az", "az-b");
            Path writerCounts = directory.resolve("subcap-writer.json");
            Path proxyCounts = directory.resolve("subcap-proxy.json");
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

                long consumed = 0;
                int written = 0;
                DeliveredSegments delivered = new DeliveredSegments(new HttpSegmentSource(
                        Duration.ofSeconds(30), "http://localhost:" + sameZone.port(), "az-b"));
                HttpSubscriptionTransport transport = new HttpSubscriptionTransport(
                        "http://localhost:" + writer.port(), () -> { }, Duration.ofMillis(50),
                        Duration.ofSeconds(1), Duration.ofSeconds(30), Duration.ofSeconds(2),
                        32 * 1024 * 1024, "az-b");
                try (ConsumerClient consumer = new ConsumerClient(transport, key, 16,
                        delivered, io.github.huyz0.os.biningester.client.SegmentFetchRetry.standard(System::currentTimeMillis))) {
                    for (int batch = 0; batch < SUB_CAP_BATCHES; batch++) {
                        List<String> ids = new ArrayList<>();
                        for (int i = 0; i < SUB_CAP_RECORDS; i++) {
                            ids.add("sub-cap-" + batch + "-" + i);
                        }
                        assertThat(writer.write(ids)).isEqualTo(202);
                        written += ids.size();
                    }
                    java.util.Set<String> seen = new java.util.HashSet<>();
                    for (int read = 0; read < written; read++) {
                        var record = consumer.readNext(Duration.ofSeconds(30));
                        assertThat(record)
                                .as("record %d of %d reached the az-b consumer", read, written)
                                .isPresent();
                        String id = record.get().record().id();
                        assertThat(seen.add(id)).as("record %s consumed once", id).isTrue();
                        consumed += bodyBytes(List.of(id));
                    }
                } finally {
                    transport.close();
                }

                writer.snapshotCounts(writerCounts);
                sameZone.snapshotCounts(proxyCounts);
                Map<String, Long> a = counters(Files.readString(writerCounts,
                        StandardCharsets.UTF_8));
                Map<String, Long> b = counters(Files.readString(proxyCounts,
                        StandardCharsets.UTF_8));
                System.out.println("M10.33 sub-cap NFR-5: consumed=" + consumed + ", delivered="
                        + delivered.bytes() + " in " + delivered.fetches() + " fetches "
                        + delivered.sizes + ", az-a=" + a + ", az-b=" + b);
                // (1) the payloads came from the az-b pod, and every one was sub-cap.
                assertThat(delivered.fetches())
                        .as("the az-b consumer fetched its payloads over the az-b /seg")
                        .isPositive();
                assertThat(delivered.sizes.values())
                        .as("every delivered segment is under the 256 KiB inline cap")
                        .allSatisfy(size -> assertThat(size).isLessThan(256 * 1024));
                assertThat(b.get("sameAzProxyRead"))
                        .as("same-AZ proxy payload bytes against segment bytes delivered")
                        .isGreaterThanOrEqualTo(delivered.bytes());
                for (Map<String, Long> pod : List.of(a, b)) {
                    assertThat(pod.get("unknownPeerBytes")).isZero();
                }
                assertThat(a.get("inlinePush"))
                        .as("the writer inlined nothing ACROSS the zone")
                        .isZero();
                // (2) NFR-5 against what the consumer read.
                long crossAz = a.get("crossAzBytes") + b.get("crossAzBytes");
                assertThat(crossAz * 1_000L)
                        .as("cross-AZ bytes=%d (az-a %d, az-b %d), consumed producer bytes=%d",
                                crossAz, a.get("crossAzBytes"), b.get("crossAzBytes"), consumed)
                        .isLessThan(consumed);
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
     * M10.34: four cross-zone partitions WRITTEN together. ⚠️ Not four streams
     * per segment: how many share a segment is the writer's to decide, and the
     * measured run packed two (K = 2, ADR-0080; M12.20, M10.34 P1).
     */
    private static final int STREAMS = 4;

    /**
     * M10.34: bulks per stream, each ~230 KB; a measured segment carried ~460 KB,
     * one bulk from each of two streams (M12.20, M10.34 P1).
     */
    private static final int K_BATCHES = 2;
    private static final int K_RECORDS = 110;

    /**
     * ADR-0080's stated bound: cross-zone bytes per (segment, cross-zone
     * stream) -- the per-stream event, ~166 B measured at K = 1 (ADR-0076).
     */
    private static final long EVENT_BOUND = 200;

    private static List<String> ids(int batch, int partition) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < K_RECORDS; i++) {
            ids.add("k-" + batch + "-" + partition + "-" + i);
        }
        return ids;
    }

    /**
     * M10.34, ADR-0080: NFR-5's per-stream EVENT floor, measured at K > 1.
     *
     * <p>⚠️ **SEVERAL CROSS-ZONE STREAMS SHARE EACH SEGMENT.** Four
     * partitions' bulks are written together into a writer whose flush
     * interval is held at one second, so a segment carries more than one run;
     * four az-b consumers, one per partition, each hold their own session, and
     * each is sent the event of every segment its stream is in. What crosses
     * the zone is then one event per (segment, cross-zone stream) -- ADR-0076's
     * floor, ~166 B each at K = 1 -- and the case asserts K > 1 happened and
     * both halves of ADR-0080's stated bound: cross-AZ bytes at most
     * {@link #EVENT_BOUND} per (segment, cross-zone stream), and under 0.1% of
     * consumed bytes for segments above K x that bound x 1000. ⚠️ HOW MANY
     * STREAMS A SEGMENT GETS is the writer's to decide; the measured run
     * (ADR-0080) packed them two to a segment.
     */
    @Test
    void theEventFloorIsPerCrossZoneStreamAtKOverOne() throws Exception {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a T3 suite");
        try (ChaosBucket bucket = ChaosBucket.create()) {
            Map<String, String> common = new HashMap<>(bucket.nodeSettings());
            common.put("producer.allowed-indices", INDEX);
            Map<String, String> zoneA = new HashMap<>(common);
            zoneA.put("pod.az", "az-a");
            // ⚠️ ONE SECOND, FLOOR AND CEILING: bulks sent together share a
            // flush, so a segment carries several streams (asserted below).
            zoneA.put("ingest.interval-floor", "PT1S");
            zoneA.put("ingest.interval-ceiling", "PT1S");
            Map<String, String> zoneB = new HashMap<>(common);
            zoneB.put("pod.az", "az-b");
            Path writerCounts = directory.resolve("k4-writer.json");
            Path proxyCounts = directory.resolve("k4-proxy.json");
            NodeProcess writer = null;
            NodeProcess sameZone = null;
            List<HttpSubscriptionTransport> transports = new ArrayList<>();
            List<ConsumerClient> consumers = new ArrayList<>();
            try {
                writer = NodeProcess.start(directory, "poda", zoneA, NodeProcess.Options.NONE,
                        writerCounts);
                sameZone = NodeProcess.start(directory, "podb", zoneB,
                        NodeProcess.Options.NONE, proxyCounts);
                UUID stream = UUID.randomUUID();
                writer.registerIndex(INDEX, stream);
                DeliveredSegments delivered = new DeliveredSegments(new HttpSegmentSource(
                        Duration.ofSeconds(30), "http://localhost:" + sameZone.port(), "az-b"));
                for (int partition = 0; partition < STREAMS; partition++) {
                    HttpSubscriptionTransport transport = new HttpSubscriptionTransport(
                            "http://localhost:" + writer.port(), () -> { },
                            Duration.ofMillis(50), Duration.ofSeconds(1), Duration.ofSeconds(30),
                            Duration.ofSeconds(2), 32 * 1024 * 1024, "az-b");
                    transports.add(transport);
                    consumers.add(new ConsumerClient(transport, new RunKey(stream, partition), 16,
                            delivered, io.github.huyz0.os.biningester.client.SegmentFetchRetry.standard(System::currentTimeMillis)));
                }

                long consumed = 0;
                int perStream = 0;
                for (int batch = 0; batch < K_BATCHES; batch++) {
                    NodeProcess target = writer;
                    int b = batch;
                    List<java.util.concurrent.CompletableFuture<Integer>> bulks =
                            new ArrayList<>();
                    for (int partition = 0; partition < STREAMS; partition++) {
                        int p = partition;
                        bulks.add(java.util.concurrent.CompletableFuture.supplyAsync(
                                () -> target.write(p, ids(b, p))));
                    }
                    for (var bulk : bulks) {
                        assertThat(bulk.get(60, TimeUnit.SECONDS)).isEqualTo(202);
                    }
                    perStream += K_RECORDS;
                }
                for (int partition = 0; partition < STREAMS; partition++) {
                    java.util.Set<String> seen = new java.util.HashSet<>();
                    for (int read = 0; read < perStream; read++) {
                        var record = consumers.get(partition).readNext(Duration.ofSeconds(30));
                        assertThat(record).as("partition %d, record %d", partition, read)
                                .isPresent();
                        String id = record.get().record().id();
                        assertThat(seen.add(id)).isTrue();
                        consumed += bodyBytes(List.of(id));
                    }
                }
                for (ConsumerClient consumer : consumers) {
                    consumer.close();
                }
                consumers.clear();

                writer.snapshotCounts(writerCounts);
                sameZone.snapshotCounts(proxyCounts);
                Map<String, Long> a = counters(Files.readString(writerCounts,
                        StandardCharsets.UTF_8));
                Map<String, Long> b = counters(Files.readString(proxyCounts,
                        StandardCharsets.UTF_8));
                long servedStreams = delivered.fetches();
                long crossAz = a.get("crossAzBytes") + b.get("crossAzBytes");
                System.out.println("M10.34 partitions=" + STREAMS + ", K (streams per segment)="
                        + (delivered.sizes.isEmpty() ? 0 : servedStreams / delivered.sizes.size())
                        + " NFR-5: consumed=" + consumed
                        + ", segments=" + delivered.sizes.size() + ", (segment, stream) "
                        + "deliveries=" + servedStreams + ", crossAz=" + crossAz + " ("
                        + (servedStreams == 0 ? 0 : crossAz / servedStreams) + " B each), az-a="
                        + a + ", az-b=" + b);
                assertThat(a.get("inlinePush")).as("nothing inlined across the zone").isZero();
                assertThat(crossAz)
                        .as("⚠️ ADR-0080's STATED BOUND: at most %d B per (segment, cross-zone "
                                + "stream), %d deliveries", EVENT_BOUND, servedStreams)
                        .isLessThanOrEqualTo(EVENT_BOUND * servedStreams);
                assertThat(crossAz * 1_000L)
                        .as("and NFR-5, the segments being above K x %d KB", EVENT_BOUND)
                        .isLessThan(consumed);
                // ⚠️ THE PREMISE LAST, so a payload served across the zone fails on
                // the bound it breaks -- an inline delivery makes no fetch, and
                // checked first this would report "K was 1" instead.
                assertThat(servedStreams)
                        .as("⚠️ THE PREMISE, K > 1: more (segment, stream) deliveries than "
                                + "segments")
                        .isGreaterThan(delivered.sizes.size());
            } finally {
                for (ConsumerClient consumer : consumers) {
                    consumer.close();
                }
                for (HttpSubscriptionTransport transport : transports) {
                    transport.close();
                }
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
