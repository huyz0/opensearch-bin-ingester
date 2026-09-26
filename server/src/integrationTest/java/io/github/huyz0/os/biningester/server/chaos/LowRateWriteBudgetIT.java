// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.huyz0.os.biningester.binstore.StoreCounts;
import io.github.huyz0.os.biningester.binstore.backend.S3Fixture;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** M9.56: low-rate data and commit writes are bounded per interval, not per second. */
@Timeout(value = 1, unit = TimeUnit.HOURS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LowRateWriteBudgetIT {

    private static final Duration SHORT_CEILING = Duration.ofMillis(250);
    private static final Duration LONG_CEILING = Duration.ofSeconds(5);
    private static final Pattern COUNT = Pattern.compile("\\\"([A-Za-z]+)\\\":(\\d+)");
    private static final Duration LEASE_RENEW_INTERVAL = Duration.ofSeconds(3);
    private static final Duration CHECKPOINT_INTERVAL = Duration.ofSeconds(60);

    @TempDir
    Path directory;

    @Test
    void lowRateWritesStayWithinTwoPutsPerIntervalAtBothCeilings() throws Exception {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a T3 suite");
        Duration duration = Duration.parse(System.getProperty("m9.56.pointDuration",
                System.getenv().getOrDefault("M9_56_POINT_DURATION", "PT5M")));

        LowRateResult shortPoint = runPoint(SHORT_CEILING, duration);
        LowRateResult longPoint = runPoint(LONG_CEILING, duration);

        System.out.println("M9.56 low-rate write points: " + List.of(shortPoint, longPoint));
        assertBoundedPerInterval(shortPoint);
        assertBoundedPerInterval(longPoint);
        assertThat(shortPoint.counts().lists()).as("250 ms low-rate LISTs").isZero();
        assertThat(longPoint.counts().lists()).as("5 s low-rate LISTs").isZero();
        // At 250 ms the correct interval-bound behaviour is intentionally more
        // than two requests per second. This is why the budget must name the
        // interval, rather than accidentally baking the dial into NFR-1.
        assertThat(shortPoint.counts().puts())
                .as("a per-second bound would reject correct 250 ms behaviour")
                .isGreaterThan(2L * shortPoint.elapsed().toSeconds());

    }

    private LowRateResult runPoint(Duration ceiling, Duration duration) throws Exception {
        try (ChaosBucket bucket = ChaosBucket.create()) {
            Map<String, String> settings = new HashMap<>(bucket.nodeSettings());
            settings.put("producer.allowed-indices", "logs");
            settings.put("ingest.interval-floor", ceiling.toString());
            settings.put("ingest.interval-ceiling", ceiling.toString());
            settings.put("lease.ttl", "PT10S");
            settings.put("lease.renew-interval", LEASE_RENEW_INTERVAL.toString());
            NodeProcess node = null;
            try {
                node = NodeProcess.start(directory, "pod" + ceiling.toMillis(), settings,
                        NodeProcess.Options.NONE,
                        directory.resolve("counts-" + ceiling.toMillis() + ".json"));
                node.registerIndex("logs", java.util.UUID.randomUUID());
                StoreSnapshot before = snapshot(node, directory.resolve("before-" + ceiling.toMillis()
                        + ".json"));
                long started = System.nanoTime();
                int sequence = 0;
                long deadline = started + duration.toNanos();
                while (System.nanoTime() < deadline) {
                    String id = "low-" + ceiling.toMillis() + '-' + sequence++;
                    int status = ceiling.equals(LONG_CEILING)
                            ? node.write(id, 1, Duration.ofSeconds(10))
                            : node.write(id, 1);
                    assertThat(status).as("low-rate bulk status").isEqualTo(202);
                }
                StoreSnapshot after = snapshot(node,
                        directory.resolve("after-" + ceiling.toMillis() + ".json"));
                Duration elapsed = Duration.ofNanos(System.nanoTime() - started);
                return new LowRateResult(ceiling, elapsed,
                        minus(after.counts(), before.counts()),
                        minus(after.purposePuts(), before.purposePuts()));
            } finally {
                if (node != null) {
                    node.close();
                }
            }
        }
    }

    private static void assertBoundedPerInterval(LowRateResult result) {
        long intervals = (result.elapsed().toNanos() + result.ceiling().toNanos() - 1)
                / result.ceiling().toNanos();
        assertThat(result.purposePuts().dataAndCommitPuts())
                .as("data+commit puts=%d, total puts=%d, ceiling=%s, elapsed=%s",
                        result.purposePuts().dataAndCommitPuts(), result.counts().puts(),
                        result.ceiling(), result.elapsed())
                .isLessThanOrEqualTo(2L * intervals + 2);
        long leaseRenewals = result.elapsed().toNanos()
                / LEASE_RENEW_INTERVAL.toNanos() + 1;
        assertThat(result.purposePuts().leasePuts())
                .as("lease puts=%d, renew interval=%s, elapsed=%s",
                        result.purposePuts().leasePuts(), LEASE_RENEW_INTERVAL, result.elapsed())
                .isLessThanOrEqualTo(leaseRenewals);
        long checkpointWindows = (result.elapsed().toNanos()
                + CHECKPOINT_INTERVAL.toNanos() - 1) / CHECKPOINT_INTERVAL.toNanos();
        assertThat(result.purposePuts().checkpointPuts())
                .as("checkpoint puts=%d, checkpoint interval=%s, elapsed=%s",
                        result.purposePuts().checkpointPuts(), CHECKPOINT_INTERVAL,
                        result.elapsed())
                .isLessThanOrEqualTo(2L * checkpointWindows + 2);
        assertThat(result.purposePuts().total()).isEqualTo(result.counts().puts());
    }

    private static StoreSnapshot snapshot(NodeProcess node, Path path) throws Exception {
        node.snapshotCounts(path);
        Map<String, Long> values = new HashMap<>();
        Matcher matcher = COUNT.matcher(Files.readString(path, StandardCharsets.UTF_8));
        while (matcher.find()) {
            values.put(matcher.group(1), Long.parseLong(matcher.group(2)));
        }
        assertThat(values).containsKeys("puts", "gets", "lists", "stats", "deletes",
                "dataPuts", "commitPuts", "checkpointPuts", "leasePuts", "otherPuts");
        StoreCounts counts = new StoreCounts(values.get("puts"), values.get("gets"),
                values.get("lists"), values.get("stats"), values.get("deletes"));
        PutPurposeCounts purposePuts = new PutPurposeCounts(values.get("dataPuts"),
                values.get("commitPuts"), values.get("checkpointPuts"),
                values.get("leasePuts"), values.get("otherPuts"));
        assertThat(purposePuts.total()).isEqualTo(counts.puts());
        return new StoreSnapshot(counts, purposePuts);
    }

    private static StoreCounts minus(StoreCounts left, StoreCounts right) {
        return new StoreCounts(left.puts() - right.puts(), left.gets() - right.gets(),
                left.lists() - right.lists(), left.stats() - right.stats(),
                left.deletes() - right.deletes());
    }

    private static PutPurposeCounts minus(PutPurposeCounts left, PutPurposeCounts right) {
        return new PutPurposeCounts(left.dataPuts() - right.dataPuts(),
                left.commitPuts() - right.commitPuts(),
                left.checkpointPuts() - right.checkpointPuts(),
                left.leasePuts() - right.leasePuts(), left.otherPuts() - right.otherPuts());
    }

    private record StoreSnapshot(StoreCounts counts, PutPurposeCounts purposePuts) {}

    private record PutPurposeCounts(long dataPuts, long commitPuts, long checkpointPuts,
            long leasePuts, long otherPuts) {
        long total() {
            return dataPuts + commitPuts + checkpointPuts + leasePuts + otherPuts;
        }

        long dataAndCommitPuts() {
            return dataPuts + commitPuts;
        }
    }

    private record LowRateResult(Duration ceiling, Duration elapsed, StoreCounts counts,
            PutPurposeCounts purposePuts) {}
}
