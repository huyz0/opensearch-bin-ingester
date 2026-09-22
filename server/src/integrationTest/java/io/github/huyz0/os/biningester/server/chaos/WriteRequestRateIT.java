// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.huyz0.os.biningester.bench.BulkBatch;
import io.github.huyz0.os.biningester.bench.LoadGenerator;
import io.github.huyz0.os.biningester.bench.SizeProfile;
import io.github.huyz0.os.biningester.bench.WorkloadSpec;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.binstore.StoreCounts;
import io.github.huyz0.os.biningester.binstore.backend.S3Fixture;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** M9.8: measured write request rate on three concurrent RustFS pods. */
@Timeout(value = 1, unit = TimeUnit.HOURS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class WriteRequestRateIT {

    private static final long MIB = 1024L * 1024L;
    private static final long MAX_SEGMENT_BYTES = 8L * MIB;
    private static final int PRODUCER_COUNT = 24;
    private static final Pattern COUNT = Pattern.compile("\\\"(puts|gets|lists|stats|deletes)\\\":(\\d+)");
    private static final List<RatePoint> POINTS = List.of(
            new RatePoint(40), new RatePoint(80), new RatePoint(160));

    @TempDir
    Path directory;

    @Test
    void sizeTriggeredFleetStaysBelowTheWriteRequestBudgetAtThreeRates() throws Exception {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a T3 suite");
        Duration duration = Duration.parse(System.getProperty("m9.8.pointDuration",
                System.getenv().getOrDefault("M9_8_POINT_DURATION", "PT5M")));
        List<RateResult> results = new ArrayList<>();
        for (RatePoint point : POINTS) {
            results.add(runPoint(point, duration));
        }

        assertThat(results).allSatisfy(result -> {
            assertThat(result.bytesWritten()).as("the point must write data").isPositive();
            assertThat(result.sizeTriggeredBytes())
                    .as("point %.0f MiB/s must actually fill 8 MiB segments", result.targetMiBPerSecond())
                    .isGreaterThanOrEqualTo((long) (result.bytesWritten() * 0.75));
            assertThat(result.requestsPerMiB())
                    .as("point %.0f MiB/s: puts=%d bytes=%d", result.targetMiBPerSecond(),
                            result.counts().puts(), result.bytesWritten())
                    .isLessThan(0.30);
            assertThat(result.counts().lists()).as("hot-path LISTs are forbidden").isZero();
        });

        System.out.println("M9.8 write request points: " + results);
    }

    private RateResult runPoint(RatePoint point, Duration duration) throws Exception {
        try (ChaosBucket bucket = ChaosBucket.create()) {
            Map<String, String> common = new HashMap<>(bucket.nodeSettings());
            common.put("producer.allowed-indices", "bench-index-0");
            // RustFS on this Windows rig sustains about 30 MiB/s with the
            // benchmark's 256-document shape. A 250 ms floor therefore cannot
            // fill an 8 MiB segment; 1 s makes the size-triggered operating
            // point reachable without changing the production default.
            common.put("ingest.interval-floor", "PT1S");
            common.put("ingest.max-segment-bytes", Long.toString(MAX_SEGMENT_BYTES));

            NodeProcess first = null;
            NodeProcess second = null;
            NodeProcess third = null;
            try {
                first = start(common, "poda", "az-a", directory);
                second = start(common, "podb", "az-b", directory);
                third = start(common, "podc", "az-c", directory);
                List<NodeProcess> fleet = List.of(first, second, third);
                first.registerIndex("bench-index-0", UUID.randomUUID());
                StoreCounts before = snapshotCounts(fleet, directory);
                // M3.6 established that one producer advances only one bulk
                // chunk per flush interval. Use the repository's 24-producer
                // load shape so this test measures the size-triggered regime,
                // rather than that known single-producer ceiling.
                long bytes = writeConcurrently(List.of(first), point, duration);
                StoreCounts counts = minus(snapshotCounts(fleet, directory), before);
                long segmentBytes = 0;
                long sizeTriggeredBytes = 0;
                for (String key : bucket.segments()) {
                    ObjectStat stat = bucket.observer().stat(key).orElseThrow();
                    segmentBytes += stat.size();
                    if (stat.size() >= MAX_SEGMENT_BYTES * 9 / 10) {
                        sizeTriggeredBytes += stat.size();
                    }
                }
                assertThat(segmentBytes).as("the bucket must contain data segments").isPositive();
                return new RateResult(point.mibPerSecond(), bytes, sizeTriggeredBytes, counts,
                        (double) counts.puts() / (bytes / (double) MIB));
            } finally {
                close(first);
                close(second);
                close(third);
            }
        }
    }

    private static NodeProcess start(Map<String, String> common, String pod, String az,
            Path directory) throws Exception {
        Map<String, String> settings = new HashMap<>(common);
        settings.put("pod.az", az);
        return NodeProcess.start(directory, pod, settings, NodeProcess.Options.NONE,
                directory.resolve(pod + "-counts.json"));
    }

    private static long writeConcurrently(List<NodeProcess> nodes, RatePoint point,
            Duration duration) throws Exception {
        long deadline = System.nanoTime() + duration.toNanos();
        var executor = Executors.newVirtualThreadPerTaskExecutor();
        try {
            List<Future<Long>> futures = new ArrayList<>();
            for (int i = 0; i < PRODUCER_COUNT; i++) {
                NodeProcess node = nodes.get(i % nodes.size());
                long seed = 0x9e3779b97f4a7c15L + i;
                futures.add(executor.submit(() -> produce(node, seed,
                        point.bytesPerSecond() / PRODUCER_COUNT, deadline)));
            }
            long total = 0;
            for (Future<Long> future : futures) {
                try {
                    total += future.get(duration.plusSeconds(30).toSeconds(), TimeUnit.SECONDS);
                } catch (ExecutionException failed) {
                    throw unwrap(failed);
                } catch (TimeoutException timedOut) {
                    throw new AssertionError("producer did not stop at the point deadline", timedOut);
                }
            }
            return total;
        } finally {
            executor.shutdownNow();
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                throw new AssertionError("producer executor did not stop after the point");
            }
        }
    }

    private static long produce(NodeProcess node, long seed, long bytesPerSecond,
            long deadline)
            throws Exception {
        LoadGenerator generator = new LoadGenerator(
                new WorkloadSpec(seed, 1, 1, SizeProfile.MIXED_LONG_TAIL, 256));
        long nextSend = System.nanoTime();
        long bytes = 0;
        while (System.nanoTime() < deadline) {
            BulkBatch batch = generator.nextBatch();
            int status = node.write(batch);
            if (status != 202) {
                throw new AssertionError(node.podId() + " returned " + status + " from _bulk");
            }
            bytes += batch.body().length;
            nextSend += Math.max(1L, (long) (batch.body().length * 1_000_000_000.0
                    / (double) bytesPerSecond));
            long remaining = nextSend - System.nanoTime();
            if (remaining > 0) {
                LockSupport.parkNanos(remaining);
            }
        }
        return bytes;
    }

    private static StoreCounts snapshotCounts(List<NodeProcess> nodes, Path directory)
            throws Exception {
        StoreCounts total = new StoreCounts(0, 0, 0, 0, 0);
        for (NodeProcess node : nodes) {
            Path path = directory.resolve(node.podId() + "-counts.json");
            node.snapshotCounts(path);
            total = plus(total, readCounts(path));
        }
        return total;
    }

    private static StoreCounts readCounts(Path path) throws Exception {
        Map<String, Long> values = new HashMap<>();
        Matcher matcher = COUNT.matcher(Files.readString(path, StandardCharsets.UTF_8));
        while (matcher.find()) {
            values.put(matcher.group(1), Long.parseLong(matcher.group(2)));
        }
        assertThat(values).containsKeys("puts", "gets", "lists", "stats", "deletes");
        return new StoreCounts(values.get("puts"), values.get("gets"), values.get("lists"),
                values.get("stats"), values.get("deletes"));
    }

    private static StoreCounts plus(StoreCounts left, StoreCounts right) {
        return new StoreCounts(left.puts() + right.puts(), left.gets() + right.gets(),
                left.lists() + right.lists(), left.stats() + right.stats(),
                left.deletes() + right.deletes());
    }

    private static StoreCounts minus(StoreCounts left, StoreCounts right) {
        return new StoreCounts(left.puts() - right.puts(), left.gets() - right.gets(),
                left.lists() - right.lists(), left.stats() - right.stats(),
                left.deletes() - right.deletes());
    }

    private static Exception unwrap(ExecutionException failed) {
        Throwable cause = failed.getCause();
        return cause instanceof Exception exception
                ? exception
                : new Exception("producer failed", cause);
    }

    private static void close(NodeProcess node) {
        if (node != null) {
            node.close();
        }
    }

    private record RatePoint(double mibPerSecond) {
        long bytesPerSecond() {
            return (long) (mibPerSecond * MIB);
        }
    }

    private record RateResult(double targetMiBPerSecond, long bytesWritten,
            long sizeTriggeredBytes, StoreCounts counts, double requestsPerMiB) {}
}
