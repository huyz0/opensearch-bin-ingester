// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.huyz0.os.biningester.binstore.backend.S3Fixture;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * The harness's network half does what its verbs say, observed from outside
 * the process (M8.23).
 *
 * <p>⚠️ **EVERY PARTITION AND SKEW ROW RESTS ON THESE.** A cut that let bytes
 * through would make a partition row pass because nothing was partitioned,
 * and a skew applied only through the {@code Clock} seam would leave a wall
 * clock read elsewhere untouched, which is the mutation M8.17 exists for.
 */
@Timeout(value = 600, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ChaosNetworkIT {

    @TempDir
    Path dir;

    @BeforeAll
    static void container() {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a chaos suite");
    }

    private static int write(NodeProcess node, String id) {
        try {
            return node.write(id, 1);
        } catch (RuntimeException noAnswer) {
            return -1;
        }
    }

    private static void awaitAck(NodeProcess node, String id, Duration within) throws Exception {
        long deadline = System.nanoTime() + within.toNanos();
        while (write(node, id) != 202) {
            assertThat(System.nanoTime()).as("%s never acked again", node.podId())
                    .isLessThan(deadline);
            Thread.sleep(500);
        }
    }

    @Test
    void aSTORECutStopsTheAcksAndTheBucketSEESNothingUntilItHEALS() throws Exception {
        URI rustfs = URI.create(S3Fixture.endpoint());
        try (ChaosBucket bucket = ChaosBucket.create();
                ChaosProxy store = new ChaosProxy(rustfs.getHost(), rustfs.getPort())) {
            Map<String, String> settings = new HashMap<>(bucket.nodeSettings());
            settings.put("store.endpoint", "http://localhost:" + store.port());
            try (NodeProcess node = NodeProcess.start(dir, "pod0", settings)) {
                node.registerLogs(UUID.randomUUID());
                assertThat(write(node, "before")).as("the premise: it writes").isEqualTo(202);
                int segments = bucket.segments().size();

                store.cut();
                int during = write(node, "during");
                int stillThere = bucket.segments().size();
                store.heal();

                assertThat(during).as("⚠️ NO ACK WHILE THE STORE IS UNREACHABLE")
                        .isNotEqualTo(202);
                assertThat(stillThere).as("⚠️ AND NOTHING REACHED THE BUCKET")
                        .isEqualTo(segments);
                awaitAck(node, "after", Duration.ofSeconds(120));
            }
        }
    }

    @Test
    void aPEERCutIsolatesANodeFromItsPeersINBOUNDOnly() throws Exception {
        try (ChaosBucket bucket = ChaosBucket.create()) {
            NodeProcess.Options proxied = new NodeProcess.Options(Duration.ZERO, true);
            UUID index = UUID.randomUUID();
            try (NodeProcess leader = NodeProcess.start(
                            Files.createDirectories(dir.resolve("a")), "pod0",
                            bucket.nodeSettings(), proxied);
                    NodeProcess follower = NodeProcess.start(
                            Files.createDirectories(dir.resolve("b")), "pod1",
                            bucket.nodeSettings(), proxied)) {
                leader.registerLogs(index);
                follower.registerLogs(index);
                assertThat(write(leader, "takes-the-term")).isEqualTo(202);
                assertThat(bucket.lease().orElseThrow().holderPodId()).isEqualTo("pod0");
                assertThat(write(follower, "forwarded")).as("the premise: forwarding works")
                        .isEqualTo(202);

                leader.peers().cut();

                assertThat(write(follower, "cut-off"))
                        .as("⚠️ THE FOLLOWER CANNOT REACH THE LEADER").isNotEqualTo(202);
                assertThat(write(leader, "still-writes"))
                        .as("⚠️ WHILE THE LEADER STILL REACHES THE STORE ITSELF")
                        .isEqualTo(202);
                leader.peers().heal();
                awaitAck(follower, "healed", Duration.ofSeconds(60));
            }
        }
    }

    @Test
    void aSKEWMovesTheWallClockTheWholePROCESSReads() throws Exception {
        // ⚠️ OBSERVED IN WHAT THE PROCESS WRITES: the lease expiry is its own
        // clock plus the TTL, so a process 5 min fast writes an expiry about
        // 5 min further out than a true clock would.
        Duration skew = Duration.ofMinutes(5);
        try (ChaosBucket bucket = ChaosBucket.create();
                NodeProcess node = NodeProcess.start(dir, "pod0", bucket.nodeSettings(),
                        new NodeProcess.Options(skew, false))) {
            node.registerLogs(UUID.randomUUID());
            assertThat(write(node, "takes-the-term")).isEqualTo(202);

            long ahead = bucket.leaseExpiry().getAsLong() - System.currentTimeMillis();

            System.out.println("M8.23 a +5 min process wrote a lease expiring " + ahead
                    + " ms from true now");
            assertThat(ahead).as("⚠️ THE SKEW, PLUS AT MOST ONE TTL (10 s)")
                    .isBetween(skew.toMillis(), skew.plusSeconds(15).toMillis());
        }
    }

    @Test
    void theAGENTMovesEveryDIRECTWallClockReadNotOnlyTheSEAM() throws Exception {
        // ⚠️ REVIEW MEASURED THE CASE ABOVE PASSING WITH THE DIRECT REDIRECTS
        // DELETED: the lease expiry goes through the Clock the ingester was
        // handed. These are the reads a comparison could make without it.
        Duration skew = Duration.ofMinutes(5);
        Process probe = new ProcessBuilder(java.util.List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-javaagent:" + NodeProcess.agentJar(dir) + "=" + skew,
                "-cp", System.getProperty("java.class.path"),
                SkewProbe.class.getName())).redirectErrorStream(true).start();
        String output = new String(probe.getInputStream().readAllBytes(),
                java.nio.charset.StandardCharsets.UTF_8);
        long trueNow = System.currentTimeMillis();
        assertThat(probe.waitFor(60, TimeUnit.SECONDS)).isTrue();

        java.util.Map<String, Long> reads = new java.util.HashMap<>();
        for (String line : output.split("\n")) {
            String[] parts = line.trim().split(" ");
            if (parts.length == 2 && parts[1].matches("-?\\d+")) {
                reads.put(parts[0], Long.parseLong(parts[1]));
            }
        }
        assertThat(reads).as("the probe's output:%n%s", output)
                .containsOnlyKeys("currentTimeMillis", "Instant.now", "Clock.systemUTC");
        reads.forEach((read, value) -> assertThat(trueNow - value)
                .as("⚠️ %s RAN %d ms BEHIND TRUE NOW; A +5 min SKEW PUTS IT ~5 min AHEAD",
                        read, trueNow - value)
                .isBetween(-skew.toMillis() - 5_000, -skew.toMillis() + 5_000));
    }
}
