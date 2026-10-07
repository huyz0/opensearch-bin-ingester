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

                // ⚠️ NO REPLAY OF WHAT TIMED OUT DURING THE CUT (M13.45): held
                // connections were all delivered at the heal, and every drain and
                // forward abandoned during the partition reached the leader ahead
                // of the trigger's -- one of them applying its deferred intent by
                // luck. A connect into a real partition never completes.
                leader.peers().dropAbandonedAtHeal();
                leader.peers().cut();
                long writesStartedAt = System.nanoTime();
                Set<String> expected = writeDuringPartition(followers, inboxSize);
                long writesMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - writesStartedAt);
                List<String> intents = intentKeys(bucket);
                assertThat(expected).as("every partitioned write was acked").hasSize(inboxSize);
                assertThat(intents).as("one durable intent per flush").hasSize(inboxSize);

                // ⚠️ M13.3: pod1's newest intent before the heal, so a newer one
                // seen right after the trigger's 202 is the trigger itself,
                // deferred rather than forwarded.
                String triggerPod = "pod1";
                long lastSeqBeforeHeal = maxFlushSeq(intents, triggerPod);
                long healedAt = System.nanoTime();
                // ⚠️ TIMED APART FROM THE DRAIN (M11.23): the bound's clock starts
                // here, before heal() and the trigger write, so a slow trigger
                // write's 202 would otherwise read as a slow drain.
                long triggerMillis = -1;
                String triggerDeferred = "not-run";
                int intentsAtTrigger = -1;
                if (!Boolean.getBoolean("m9.12.disableHealDrain")) {
                    leader.peers().heal();
                    assertThat(followers.get(0).write("after-heal-" + inboxSize, 1,
                            Duration.ofSeconds(60)))
                            .as("the first post-heal write triggers the drain")
                            .isEqualTo(202);
                    triggerMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - healedAt);
                    List<String> atTrigger = intentKeys(bucket);
                    intentsAtTrigger = atTrigger.size();
                    triggerDeferred = maxFlushSeq(atTrigger, triggerPod) > lastSeqBeforeHeal
                            ? "yes" : "not-seen";
                }

                long bound = Math.max(2, (inboxSize + 99) / 100);
                long deadline = healedAt + (bound * INTERVAL.toNanos())
                        + TimeUnit.SECONDS.toNanos(5);
                // ⚠️ Awaitility, not Thread.sleep: the inbox is a real external
                // system (testing.md). A timeout is left to the assertion below,
                // which names what remained and carries the leader's log.
                try {
                    await().pollDelay(Duration.ZERO).pollInterval(Duration.ofMillis(25))
                            .atMost(Duration.ofNanos(Math.max(1, deadline - System.nanoTime())))
                            .until(() -> intentKeys(bucket).isEmpty());
                } catch (ConditionTimeoutException drainNotFinished) {
                    // reported by the assertions below
                }
                long elapsed = System.nanoTime() - healedAt;
                List<String> remaining = intentKeys(bucket);
                System.out.printf("M13.3 inbox=%d writes=%d ms trigger202=%d ms drainEnd=%d ms"
                        + " remaining=%d triggerDeferred=%s intentsAtTrigger=%d%n", inboxSize,
                        writesMillis, triggerMillis, TimeUnit.NANOSECONDS.toMillis(elapsed),
                        remaining.size(), triggerDeferred, intentsAtTrigger);
                // ⚠️ NO `trigger202 <= drainEnd` ASSERTION (M13.3 review T1): both
                // clocks start at `healedAt` and `elapsed` is read after the
                // trigger's 202, so it could not fail -- and polled concurrently an
                // M=10 inbox empties BEFORE its forwarded trigger's 202, so it is
                // not a property either. The M13.3 line reports what each includes.
                // ⚠️ M13.78 asks a deferred trigger's drain again every renew
                // interval, so the inbox empties (it stranded the trigger at
                // M=1,000 in every run before). The LATENCY below stays red at
                // M=1,000 until M13.79: the first 1,000-intent drain itself
                // takes 2.8-5.6 s against 2,500 ms.
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
            writers.add(Thread.ofVirtual().name("writer-follower-" + node).start(() -> {
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
        // ⚠️ ONE DEADLINE FOR ALL WRITERS, NOT 120 s PER JOIN (M11.23): each
        // partitioned write waits out the 1 s TTL-bound forward before its
        // intent, so a writer's 250 writes take ~255 s. Sequential 120 s joins
        // returned while a writer whose join had already timed out was still
        // writing, and its last ack surfaced as "expected 1000 but was 999".
        // The deadline is the old joins' worst case, four times 120 s.
        long writersDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(480);
        for (Thread writer : writers) {
            long left = writersDeadline - System.nanoTime();
            if (left > 0) {
                writer.join(Duration.ofNanos(left));
            }
        }
        assertThat(writers.stream().filter(Thread::isAlive).map(Thread::getName).toList())
                .as("partition writers still running at the deadline; acked %d of %d",
                        acked.size(), count)
                .isEmpty();
        assertThat(failures)
                .withFailMessage("partition write failures: %s", failureReasons)
                .as("partitioned writes must not fail instead of using the inbox")
                .hasValue(0);
        return new HashSet<>(acked);
    }

    /**
     * The highest flush sequence among {@code pod}'s intents, or -1. The key is
     * {@code .../<pod>/<incarnation>/<16 hex digits>.intent} (Inbox.keyFor).
     */
    private static long maxFlushSeq(List<String> intents, String pod) {
        long max = -1;
        for (String key : intents) {
            String[] parts = key.split("/");
            if (parts.length >= 3 && parts[parts.length - 3].equals(pod)) {
                String seq = parts[parts.length - 1].replace(".intent", "");
                max = Math.max(max, Long.parseUnsignedLong(seq, 16));
            }
        }
        return max;
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
