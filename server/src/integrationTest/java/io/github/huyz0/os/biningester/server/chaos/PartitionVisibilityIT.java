// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.huyz0.os.biningester.binstore.backend.S3Fixture;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Measures the visibility cost of ADR-0058's durable-intent path after an AZ
 * partition heals. The inbox is the clocked object: when it is empty, the
 * leaseholder has applied one serialised drain and deleted the applied
 * intents. The chain audit then proves that the records acked during the cut
 * became visible rather than merely proving that the inbox was emptied.
 */
@Timeout(value = 600, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class PartitionVisibilityIT {

    private static final Duration INTERVAL = Duration.ofMillis(250);
    private static final int[] INBOX_SIZES = {10, 1_000};
    private static final int FOLLOWERS = 4;

    @TempDir
    Path dir;

    @BeforeAll
    static void container() {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a chaos suite");
    }

    @Test
    void durableIntentAcksBecomeVisibleAfterOneHealDrainWithinTheBound() throws Exception {
        for (int inboxSize : INBOX_SIZES) {
            measure(inboxSize);
        }
    }

    private void measure(int inboxSize) throws Exception {
        try (ChaosBucket bucket = ChaosBucket.create()) {
            Map<String, String> settings = new HashMap<>(bucket.nodeSettings());
            settings.put("ingest.interval-floor", INTERVAL.toString());
            settings.put("ingest.interval-ceiling", INTERVAL.toString());
            // Keep the partitioned forward short so a thousand independent
            // intents can be produced on the real socket without spending
            // the whole test at the production ten-second lease timeout.
            settings.put("lease.ttl", "PT1S");
            settings.put("lease.renew-interval", "PT0.25S");
            // One small record crosses the trigger on its own, making each
            // durable intent a separate flush without waiting for a timer.
            settings.put("ingest.max-segment-bytes", "1");
            NodeProcess leader = NodeProcess.start(Files.createDirectories(dir.resolve(
                    "leader-" + inboxSize)), "pod0", settings,
                    new NodeProcess.Options(Duration.ZERO, true));
            List<NodeProcess> followers = new ArrayList<>();
            try {
                int followerCount = inboxSize == 10 ? 1 : Math.min(FOLLOWERS, inboxSize);
                for (int i = 0; i < followerCount; i++) {
                    NodeProcess follower = NodeProcess.start(Files.createDirectories(dir.resolve(
                            "follower-" + inboxSize + "-" + i)), "pod" + (i + 1), settings);
                    follower.registerLogs(java.util.UUID.randomUUID());
                    followers.add(follower);
                }
                assertThat(bucket.lease().orElseThrow().holderPodId())
                        .as("the premise: the leader owns the slot").isEqualTo("pod0");

                leader.peers().cut();
                Set<String> expected = writeDuringPartition(followers, inboxSize);
                List<String> intents = intentKeys(bucket);
                assertThat(expected).as("every partitioned write was acked").hasSize(inboxSize);
                assertThat(intents).as("one durable intent per flush").hasSize(inboxSize);

                long healedAt = System.nanoTime();
                if (!Boolean.getBoolean("m9.12.disableHealDrain")) {
                    leader.peers().heal();
                    assertThat(followers.get(0).write("after-heal-" + inboxSize, 1,
                            Duration.ofSeconds(60)))
                            .as("the first post-heal write triggers the drain")
                            .isEqualTo(202);
                }

                long bound = Math.max(2, (inboxSize + 99) / 100);
                long deadline = healedAt + (bound * INTERVAL.toNanos())
                        + TimeUnit.SECONDS.toNanos(5);
                // ⚠️ AWAITILITY, NOT A SLEEP LOOP (M10.9, testing.md rule 15):
                // the store is a real external system with no clock to inject.
                // A timeout falls through to the assertion below, which names
                // what is left in the inbox and prints the leader's log.
                try {
                    await().pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(25))
                            .atMost(Duration.ofNanos(Math.max(1, deadline - System.nanoTime())))
                            .until(() -> intentKeys(bucket).isEmpty());
                } catch (ConditionTimeoutException drainIncomplete) {
                    // reported by the assertion below
                }
                long elapsed = System.nanoTime() - healedAt;
                List<String> remaining = intentKeys(bucket);
                assertThat(remaining)
                        .withFailMessage("inbox still contains %s; leader log:%n%s", remaining,
                                leader.log())
                        .as("one heal causes one complete drain")
                        .isEmpty();
                assertThat(TimeUnit.NANOSECONDS.toMillis(elapsed))
                        .as("M=%d drain latency; bound=%d interval ceilings", inboxSize, bound)
                        .isLessThan(INTERVAL.toMillis() * bound);

                ChainAudit audit = ChainAudit.of(bucket);
                assertThat(audit.violations())
                        .as("the durable-intent drain assigns no overlapping offsets")
                        .isEmpty();
                Map<String, Integer> committed = KillSequencerMidCommitIT.committedIds(bucket,
                        audit.committedSegments());
                assertThat(committed.keySet())
                        .as("every durable-intent ack is visible after the drain")
                        .containsAll(expected);
                assertThat(committed.values())
                        .as("every durable-intent ack has exactly one offset assignment")
                        .allMatch(count -> count == 1);
                System.out.printf("M9.12 inbox=%d drain=%d ms bound=%d intervals%n", inboxSize,
                        TimeUnit.NANOSECONDS.toMillis(elapsed), bound);
            } finally {
                followers.forEach(NodeProcess::close);
                leader.close();
            }
        }
    }

    private static Set<String> writeDuringPartition(List<NodeProcess> followers, int count)
            throws Exception {
        Set<String> acked = java.util.concurrent.ConcurrentHashMap.newKeySet();
        AtomicInteger failures = new AtomicInteger();
        List<String> failureReasons = java.util.Collections.synchronizedList(new ArrayList<>());
        List<Thread> writers = new ArrayList<>(followers.size());
        for (int node = 0; node < followers.size(); node++) {
            int followerIndex = node;
            NodeProcess follower = followers.get(node);
            writers.add(Thread.ofVirtual().start(() -> {
                for (int sequence = followerIndex; sequence < count;
                        sequence += followers.size()) {
                    try {
                        if (follower.write("during-" + count + "-" + followerIndex + "-"
                                + sequence, 1, Duration.ofSeconds(60)) == 202) {
                            acked.add("during-" + count + "-" + followerIndex + "-" + sequence
                                    + "-0");
                        } else {
                            failureReasons.add("non-202");
                            failures.incrementAndGet();
                        }
                    } catch (Exception failed) {
                        failureReasons.add(failed.toString());
                        failures.incrementAndGet();
                    }
                }
            }));
        }
        for (Thread writer : writers) {
            writer.join(TimeUnit.SECONDS.toMillis(120));
        }
        assertThat(failures)
                .withFailMessage("partition write failures: %s", failureReasons)
                .as("partitioned writes must not fail instead of using the inbox")
                .hasValue(0);
        return new HashSet<>(acked);
    }

    private static List<String> intentKeys(ChaosBucket bucket) throws Exception {
        List<String> intents = new ArrayList<>();
        for (String key : bucket.keys(ChaosBucket.PREFIX + "/ctl/inbox/0/")) {
            if (key.endsWith(".intent")) {
                intents.add(key);
            }
        }
        return intents;
    }
}
