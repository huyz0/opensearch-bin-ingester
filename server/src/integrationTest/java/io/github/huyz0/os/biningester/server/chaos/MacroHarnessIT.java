// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.huyz0.os.biningester.bench.BulkBatch;
import io.github.huyz0.os.biningester.bench.LoadGenerator;
import io.github.huyz0.os.biningester.bench.MacroRunResults;
import io.github.huyz0.os.biningester.bench.ProcessStoreCounts;
import io.github.huyz0.os.biningester.bench.SizeProfile;
import io.github.huyz0.os.biningester.bench.WorkloadSpec;
import io.github.huyz0.os.biningester.binstore.backend.S3Fixture;
import io.github.huyz0.os.biningester.client.ConsumerClient;
import io.github.huyz0.os.biningester.client.ConsumerRecord;
import io.github.huyz0.os.biningester.client.HttpSubscriptionTransport;
import io.github.huyz0.os.biningester.format.RunKey;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** M9.4: the assembled RustFS fleet produces one cost-and-latency artifact. */
@Timeout(value = 300, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class MacroHarnessIT {

    private static final String INDEX = "bench-index-0";
    private static final Pattern COUNT = Pattern.compile("\\\"(puts|gets|lists|stats|deletes)\\\":(\\d+)");
    private static final Pattern ID = Pattern.compile("\\\"_id\\\":\\\"([^\\\"]+)\\\"");
    private static final Pattern ENCODED = Pattern.compile("\\\"encoded\\\":\\\"([^\\\"]+)\\\"");

    @TempDir
    Path directory;

    @Test
    void macroRunWritesCountsAnd202ToConsumerHistogram() throws Exception {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a T3 suite");
        try (ChaosBucket bucket = ChaosBucket.create()) {
            Map<String, String> settings = new HashMap<>(bucket.nodeSettings());
            settings.put("producer.allowed-indices", INDEX);
            Path countsA = directory.resolve("pod-a-counts.json");
            Path countsB = directory.resolve("pod-b-counts.json");
            NodeProcess first = null;
            NodeProcess second = null;
            NodeProcess plain = null;
            try {
                first = NodeProcess.start(directory, "poda", settings, NodeProcess.Options.NONE, countsA);
                second = NodeProcess.start(directory, "podb", settings, NodeProcess.Options.NONE, countsB);
                plain = NodeProcess.start(directory, "podc", settings, NodeProcess.Options.NONE);
                assertThat(first.macroPath()).isNotBlank();
                assertThat(second.macroPath()).isNotBlank().isNotEqualTo(first.macroPath());
                assertThat(plain.macroPath()).isNull();
                assertThat(plain.status("/_macro/store-counts/not-enabled")).isEqualTo(404);
                assertThat(plain.status(first.macroPath())).isEqualTo(404);
                assertThat(plain.status(second.macroPath())).isEqualTo(404);

                UUID stream = UUID.randomUUID();
                first.registerIndex(INDEX, stream);
                RunKey key = new RunKey(stream, 0);
                Histogram latency = new Histogram(TimeUnit.SECONDS.toNanos(60), 3);
                List<Long> acknowledged = new ArrayList<>();
                List<String> expectedIds = new ArrayList<>();
                LoadGenerator generator = new LoadGenerator(
                        new WorkloadSpec(41L, 1, 1, SizeProfile.SMALL_200B, 4));
                    int expected = 12;

                HttpSubscriptionTransport transport = new HttpSubscriptionTransport(
                        "http://localhost:" + first.port(), () -> { }, Duration.ofMillis(50),
                        Duration.ofSeconds(1), Duration.ofSeconds(30), Duration.ofSeconds(2));
                try (ConsumerClient consumer = new ConsumerClient(transport, key, 256)) {
                    for (int batch = 0; batch < expected / 4; batch++) {
                        var generated = generator.nextBatch();
                        long requestStarted = System.nanoTime();
                        assertThat(first.write(generated)).isEqualTo(202);
                        long responseReturned = System.nanoTime();
                        long sent = responseReturned;
                        assertThat(sent).isGreaterThanOrEqualTo(responseReturned)
                                .isGreaterThan(requestStarted);
                        expectedIds.addAll(ids(generated));
                        acknowledged.addAll(java.util.Collections.nCopies(
                                generated.documentCount(), sent));
                    }

                    List<ConsumerRecord> deliveredRecords = new ArrayList<>();
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
                    while (deliveredRecords.size() < expected && System.nanoTime() < deadline) {
                        var next = consumer.readNext(Duration.ofSeconds(5));
                        if (next.isPresent()) {
                            deliveredRecords.add(next.get());
                            latency.recordValue(Math.max(1,
                                    System.nanoTime() - acknowledged.get(deliveredRecords.size() - 1)));
                        }
                    }
                    assertThat(deliveredRecords).hasSize(expected);
                    assertThat(deliveredRecords).extracting(record -> record.record().id())
                            .containsExactlyElementsOf(expectedIds);
                    assertThat(deliveredRecords).extracting(ConsumerRecord::offset)
                            .isSorted();
                } finally {
                    transport.close();
                }

                first.snapshotCounts(countsA);
                second.snapshotCounts(countsB);
                first.terminate();
                second.terminate();
                plain.terminate();

                ProcessStoreCounts podaCounts = readCounts(countsA);
                ProcessStoreCounts podbCounts = readCounts(countsB);
                assertThat(podaCounts.puts()).isPositive();
                Path result = directory.resolve("macro-" + UUID.randomUUID() + ".json");
                String results = new MacroRunResults("macro-rustfs", Map.of(
                        "poda", podaCounts, "podb", podbCounts), latency).toJson();
                Files.writeString(result, results, StandardCharsets.UTF_8);
                String json = Files.readString(result, StandardCharsets.UTF_8);
                assertThat(json).contains("\"podId\":\"poda\"")
                        .contains("\"podId\":\"podb\"")
                        .contains("\"type\":\"HdrHistogram\"")
                        .contains("\"totalCount\":12");
                assertCountJson(json, "poda", podaCounts);
                assertCountJson(json, "podb", podbCounts);
                Matcher encoded = ENCODED.matcher(json);
                assertThat(encoded.find()).isTrue();
                Histogram decoded = Histogram.decodeFromCompressedByteBuffer(
                        ByteBuffer.wrap(Base64.getDecoder().decode(encoded.group(1))), 0);
                assertThat(decoded.getTotalCount()).isEqualTo(latency.getTotalCount());
                assertThat(decoded.getMinValue()).isEqualTo(latency.getMinValue());
                assertThat(decoded.getMaxValue()).isEqualTo(latency.getMaxValue());
                assertThat(decoded.getValueAtPercentile(50.0))
                        .isEqualTo(latency.getValueAtPercentile(50.0));
                assertThat(decoded.getValueAtPercentile(95.0))
                        .isEqualTo(latency.getValueAtPercentile(95.0));
                assertThat(decoded.getValueAtPercentile(99.0))
                        .isEqualTo(latency.getValueAtPercentile(99.0));
                assertThat(latency.getMinValue()).isPositive();
                assertThat(latency.getMaxValue()).isGreaterThanOrEqualTo(latency.getMinValue());
                assertThat(latency.getMaxValue()).isGreaterThan(latency.getMinValue());
                System.out.println("M9.4 macro results: " + result);
            } finally {
                if (first != null) {
                    first.close();
                }
                if (second != null) {
                    second.close();
                }
                if (plain != null) {
                    plain.close();
                }
            }
        }
    }

    private static List<String> ids(BulkBatch batch) {
        Matcher matcher = ID.matcher(new String(batch.body(), StandardCharsets.UTF_8));
        List<String> ids = new ArrayList<>();
        while (matcher.find()) {
            ids.add(matcher.group(1));
        }
        return ids;
    }

    private static ProcessStoreCounts readCounts(Path path) throws Exception {
        String json = Files.readString(path, StandardCharsets.UTF_8);
        Map<String, Long> values = new HashMap<>();
        Matcher matcher = COUNT.matcher(json);
        while (matcher.find()) {
            values.put(matcher.group(1), Long.parseLong(matcher.group(2)));
        }
        assertThat(values).containsKeys("puts", "gets", "lists", "stats", "deletes");
        return new ProcessStoreCounts(values.get("puts"), values.get("gets"), values.get("lists"),
                values.get("stats"), values.get("deletes"));
    }

    private static void assertCountJson(String json, String podId, ProcessStoreCounts counts) {
        assertThat(json).contains("\"podId\":\"" + podId + "\",\"puts\":" + counts.puts()
                + ",\"gets\":" + counts.gets() + ",\"lists\":" + counts.lists()
                + ",\"stats\":" + counts.stats() + ",\"deletes\":" + counts.deletes());
    }
}
