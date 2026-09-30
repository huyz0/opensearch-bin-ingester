// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.huyz0.os.biningester.binstore.backend.S3Fixture;
import io.github.huyz0.os.biningester.client.ConsumerClient;
import io.github.huyz0.os.biningester.client.HttpSubscriptionTransport;
import io.github.huyz0.os.biningester.format.RunKey;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** M9.11: producer-202 to consumer delivery stays below three interval ceilings. */
@Timeout(value = 15, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class VisibilityLatencyIT {

    private static final String INDEX = "logs";
    private static final List<Duration> CEILINGS = List.of(
            Duration.ofMillis(250), Duration.ofSeconds(1), Duration.ofSeconds(5));
    private static final int RECORDS = 10_000;
    private static final int RECORDS_PER_WRITE = 1_000;

    @TempDir
    Path directory;

    @Test
    void consumerVisibilityP99StaysBelowThreeIntervalCeilings() throws Exception {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a T3 suite");
        int records = Integer.getInteger("m9.11.records", RECORDS);
        assertThat(records).isGreaterThanOrEqualTo(RECORDS);

        List<PointResult> points = new ArrayList<>();
        for (Duration ceiling : CEILINGS) {
            points.add(runPoint(ceiling, records));
        }

        for (PointResult point : points) {
            long p99 = point.latency().getValueAtPercentile(99.0);
            long bound = 3L * point.ceiling().toNanos();
            assertThat(p99)
                    .as("p99=%d ns, ceiling=%s, bound=%d ns, records=%d", p99,
                            point.ceiling(), bound, point.latency().getTotalCount())
                    .isLessThan(bound);
            System.out.println("M9.11 visibility latency: ceiling=" + point.ceiling()
                    + ", records=" + point.latency().getTotalCount()
                    + ", p50=" + point.latency().getValueAtPercentile(50.0)
                    + " ns, p99=" + p99 + " ns, max=" + point.latency().getMaxValue()
                    + " ns (RustFS loopback lower bound)");
        }
    }

    private PointResult runPoint(Duration ceiling, int records) throws Exception {
        try (ChaosBucket bucket = ChaosBucket.create()) {
            Map<String, String> settings = new HashMap<>(bucket.nodeSettings());
            settings.put("producer.allowed-indices", INDEX);
            settings.put("ingest.interval-floor", ceiling.toString());
            settings.put("ingest.interval-ceiling", ceiling.toString());
            settings.put("ingest.max-segment-bytes", Long.toString(64L << 20));
            NodeProcess node = null;
            try {
                node = NodeProcess.start(directory, "pod" + ceiling.toMillis(), settings);
                UUID stream = UUID.randomUUID();
                node.registerIndex(INDEX, stream);
                RunKey key = new RunKey(stream, 0);
                Histogram latency = new Histogram(TimeUnit.SECONDS.toNanos(60), 3);
                Map<String, Long> acknowledged = new HashMap<>(records);
                List<String> expectedIds = new ArrayList<>(records);
                List<DeliverySample> deliveries = Collections.synchronizedList(
                        new ArrayList<>(records));
                HttpSubscriptionTransport transport = new HttpSubscriptionTransport(
                        "http://localhost:" + node.port(), () -> { }, Duration.ofMillis(50),
                        Duration.ofSeconds(1), Duration.ofSeconds(30), Duration.ofSeconds(2));
                try (ConsumerClient consumer = new ConsumerClient(transport, key, 256, null, io.github.huyz0.os.biningester.client.SegmentFetchRetry.standard(System::currentTimeMillis))) {
                    FutureTask<Void> reader = new FutureTask<>(() -> {
                        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                        while (deliveries.size() < records && System.nanoTime() < deadline) {
                            var next = consumer.readNext(Duration.ofSeconds(5));
                            if (next.isPresent()) {
                                deliveries.add(new DeliverySample(next.get().record().id(),
                                        System.nanoTime()));
                            }
                        }
                        return null;
                    });
                    Thread.ofVirtual().start(reader);
                    for (int batch = 0; batch < records / RECORDS_PER_WRITE; batch++) {
                        String prefix = "m9-" + ceiling.toMillis() + "-" + batch;
                        int status = node.write(prefix, RECORDS_PER_WRITE,
                                ceiling.plusSeconds(10));
                        long accepted = System.nanoTime();
                        assertThat(status).as("202 for ceiling %s", ceiling).isEqualTo(202);
                        for (int i = 0; i < RECORDS_PER_WRITE; i++) {
                            String id = prefix + '-' + i;
                            expectedIds.add(id);
                            acknowledged.put(id, accepted);
                        }
                    }
                    reader.get(60, TimeUnit.SECONDS);
                } finally {
                    transport.close();
                }

                List<String> deliveredIds = deliveries.stream().map(DeliverySample::id).toList();
                assertThat(deliveredIds).containsExactlyElementsOf(expectedIds);
                for (DeliverySample delivery : deliveries) {
                    Long accepted = acknowledged.get(delivery.id());
                    assertThat(accepted).as("record delivered was acknowledged").isNotNull();
                    latency.recordValue(Math.max(1, delivery.deliveredAt() - accepted));
                }
                assertThat(latency.getTotalCount()).isEqualTo(records);
                return new PointResult(ceiling, latency);
            } finally {
                if (node != null) {
                    node.close();
                }
            }
        }
    }

    private record DeliverySample(String id, long deliveredAt) { }

    private record PointResult(Duration ceiling, Histogram latency) { }
}
