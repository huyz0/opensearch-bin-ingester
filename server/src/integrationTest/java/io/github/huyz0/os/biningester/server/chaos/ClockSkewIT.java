// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.huyz0.os.biningester.binstore.backend.S3Fixture;
import io.github.huyz0.os.biningester.format.Lease;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * One pod's clock 5 min off: safety by epoch, not by clock, and the test
 * says WHICH liveness property degrades (M8.17, FR-11, research 08 §9).
 *
 * <p>⚠️ **THE SKEW IS THE PROCESS's** ({@link SkewAgent}), not a skewed
 * {@code Clock} handed through the seam, so a wall-clock read anywhere in the
 * ingester or its libraries sees it.
 *
 * <p>⚠️ **WHAT A CLOCK CAN AND CANNOT DO HERE.** A lease's expiry is written by
 * its holder's clock and judged by everyone else's, so a skewed pod changes
 * WHEN a term looks over. That is liveness. It cannot change what a commit
 * may write: the chain's slots are conditional writes and a successor SEALs
 * its predecessor, both counters. So each case asserts the chain is clean and
 * nothing acked is lost, and asserts the specific liveness effect a skew of
 * that sign predicts.
 */
@Timeout(value = 900, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ClockSkewIT {

    private static final Duration SKEW = Duration.ofMinutes(5);
    private static final Duration LEASE_TTL = Duration.ofSeconds(10);

    @TempDir
    Path dir;

    @BeforeAll
    static void container() {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a chaos suite");
    }

    private Map<String, String> settings(ChaosBucket bucket) {
        Map<String, String> settings = new HashMap<>(bucket.nodeSettings());
        settings.put("ingest.interval-floor", "PT0.05S");
        settings.put("lease.ttl", LEASE_TTL.toString());
        settings.put("lease.renew-interval", "PT2S");
        return settings;
    }

    @Test
    void aSLOWPodCannotKEEPTheTermAndNOTHINGIsLostOrReassigned() throws Exception {
        // ⚠️ THE PREDICTION: a pod 5 min slow writes expiries already 5 min
        // in the past by everyone else's clock, so the others take its term on
        // their first commit and it is fenced. Liveness: it cannot lead.
        // Safety: untouched, because fencing is the SEAL, not the clock.
        UUID index = UUID.randomUUID();
        List<NodeProcess> nodes = new CopyOnWriteArrayList<>();
        Set<String> acked = ConcurrentHashMap.newKeySet();
        AtomicBoolean stop = new AtomicBoolean();
        List<Thread> producers = new ArrayList<>();
        try (ChaosBucket bucket = ChaosBucket.create()) {
            NodeProcess slow = NodeProcess.start(Files.createDirectories(dir.resolve("slow")),
                    "pod0", settings(bucket), new NodeProcess.Options(SKEW.negated(), false));
            nodes.add(slow);
            slow.registerLogs(index);
            assertThat(slow.write("slow-first", 1)).as("the premise: the slow pod leads")
                    .isEqualTo(202);
            Lease slowTerm = bucket.lease().orElseThrow();
            assertThat(slowTerm.holderPodId()).isEqualTo("pod0");
            assertThat(slowTerm.expiresAtMillis())
                    .as("the premise: its lease already looks expired to a true clock")
                    .isLessThan(System.currentTimeMillis());

            for (int i = 1; i <= 2; i++) {
                NodeProcess node = NodeProcess.start(
                        Files.createDirectories(dir.resolve("n" + i)), "pod" + i,
                        settings(bucket));
                node.registerLogs(index);
                nodes.add(node);
            }
            for (int p = 0; p < 6; p++) {
                int producer = p;
                producers.add(Thread.ofVirtual().start(() ->
                        KillSequencerMidCommitIT.produce(producer, nodes, acked, stop)));
            }
            long ackWaitStarted = System.nanoTime();
            long ackWaitLists = bucket.observerCounts().lists();
            await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(120))
                    .untilAsserted(() -> assertThat(acked).hasSizeGreaterThanOrEqualTo(600));
            assertObservedListRate(bucket, ackWaitLists, ackWaitStarted);
            Lease later = bucket.lease().orElseThrow();
            stop.set(true);
            for (Thread producer : producers) {
                producer.join(TimeUnit.SECONDS.toMillis(30));
            }
            for (NodeProcess node : nodes) {
                node.terminate();
            }
            ChainAudit audit = ChainAudit.of(bucket);
            Set<String> committed = KillSequencerMidCommitIT
                    .committedIds(bucket, audit.committedSegments()).keySet();
            Set<String> lost = new HashSet<>(acked);
            lost.removeAll(committed);

            System.out.println("M8.17 slow pod: epochs " + audit.epochs() + ", term now "
                    + later.holderPodId() + "@" + later.epoch() + ", acked " + acked.size()
                    + ", lost " + lost.size());
            assertThat(later.holderPodId())
                    .as("⚠️ LIVENESS, AS PREDICTED: THE SLOW POD LOST THE TERM IT HELD")
                    .isNotEqualTo("pod0");
            assertThat(audit.violations()).as("⚠️ SAFETY BY EPOCH: I1, I2, I5, THE LINK")
                    .isEmpty();
            assertThat(lost).as("⚠️ AND NOTHING ACKED WAS LOST").isEmpty();
        } finally {
            stop.set(true);
            nodes.forEach(NodeProcess::close);
        }
    }

    @Test
    void aFASTLeaderWhoDIESHoldsTheTermPASTItsTTLAndEveryAckMeanwhileIsANDURABLEIntent()
            throws Exception {
        // ⚠️ THE PREDICTION: a pod 5 min fast writes expiries 5 min further
        // out than a true clock would, so when it dies the others wait for
        // that expiry rather than its TTL. Liveness: failover is delayed by the
        // skew. Safety (ADR-0058): a write acked while no one can commit is acked on a
        // durable intent, which a leaseholder's drain applies. The wait
        // is NOT sat out -- 20 s past the TTL is the assertion.
        UUID index = UUID.randomUUID();
        List<NodeProcess> nodes = new CopyOnWriteArrayList<>();
        try (ChaosBucket bucket = ChaosBucket.create()) {
            NodeProcess fast = NodeProcess.start(Files.createDirectories(dir.resolve("fast")),
                    "pod0", settings(bucket), new NodeProcess.Options(SKEW, false));
            nodes.add(fast);
            fast.registerLogs(index);
            assertThat(fast.write("fast-first", 1)).isEqualTo(202);
            NodeProcess follower = NodeProcess.start(
                    Files.createDirectories(dir.resolve("f")), "pod1", settings(bucket));
            nodes.add(follower);
            follower.registerLogs(index);
            assertThat(follower.write("forwarded", 1)).as("the premise: forwarding works")
                    .isEqualTo(202);
            Lease fastTerm = bucket.lease().orElseThrow();
            assertThat(fastTerm.holderPodId()).isEqualTo("pod0");

            fast.kill();
            long killed = System.nanoTime();
            Set<String> stuckAcked = new java.util.HashSet<>();
            Duration observation = LEASE_TTL.plusSeconds(20);
            long observationStarted = System.nanoTime();
            long listsBefore = bucket.observerCounts().lists();
            await().pollInterval(Duration.ofSeconds(1)).during(observation)
                    .atMost(observation.plusSeconds(2)).untilAsserted(() -> {
                try {
                    String tag = "stuck-" + System.nanoTime();
                    if (follower.write(tag, 1) == 202) {
                        stuckAcked.add(tag + "-0");
                    }
                } catch (RuntimeException noAnswer) {
                    // expected while nobody can commit
                }
                Lease current = bucket.lease().orElseThrow();
                assertThat(current.holderPodId())
                        .as("the fast leader keeps the held term throughout TTL+20s")
                        .isEqualTo(fastTerm.holderPodId());
                assertThat(current.epoch()).isEqualTo(fastTerm.epoch());
            });
            assertObservedListRate(bucket, listsBefore, observationStarted);
            Lease after = bucket.lease().orElseThrow();
            follower.terminate();
            ChainAudit audit = ChainAudit.of(bucket);
            Set<String> committed = KillSequencerMidCommitIT
                    .committedIds(bucket, audit.committedSegments()).keySet();

            System.out.println("M8.17 fast pod: " + TimeUnit.NANOSECONDS.toSeconds(
                    System.nanoTime() - killed) + " s after its death the term is still "
                    + after.holderPodId() + "@" + after.epoch() + "; acks meanwhile "
                    + stuckAcked.size());
            assertThat(after.epoch())
                    .as("⚠️ LIVENESS, AS PREDICTED: NO TAKEOVER WITHIN THE TTL + 20 s, because "
                            + "the dead pod's expiry is its fast clock's")
                    .isEqualTo(fastTerm.epoch());
            // ⚠️ SINCE ADR-0058 AN ACK WHILE NO ONE CAN COMMIT IS ALLOWED -- on a
            // DURABLE INTENT, applied when a leaseholder drains. So the safety
            // claim is that every such ack is named by an intent in the store.
            Set<String> inIntents = new java.util.HashSet<>();
            for (String key : bucket.keys(ChaosBucket.PREFIX + "/ctl/inbox/0/")) {
                if (key.endsWith(".intent")) {
                    String segment = io.github.huyz0.os.biningester.format.CommitRequestFrame.decode(bucket.get(key))
                            .segmentKey();
                    inIntents.addAll(KillSequencerMidCommitIT
                            .committedIds(bucket, Set.of(segment)).keySet());
                }
            }
            assertThat(inIntents)
                    .as("⚠️ SAFETY: EVERY ACK WHILE NO ONE COULD COMMIT IS BACKED BY A DURABLE "
                            + "INTENT (ADR-0058), so a leaseholder's drain will apply it")
                    .containsAll(stuckAcked);
            assertThat(audit.violations()).as("⚠️ SAFETY BY EPOCH: I1, I2, I5, THE LINK")
                    .isEmpty();
            assertThat(committed).as("⚠️ AND WHAT WAS ACKED BEFORE THE DEATH SURVIVES IT")
                    .contains("fast-first-0", "forwarded-0");
        } finally {
            nodes.forEach(NodeProcess::close);
        }
    }

    @Test
    void aLIVEFastPodTakesTheTermONCEAndHoldsItAndNOTHINGIsLost() throws Exception {
        // ⚠️ THE PREDICTION, measured rather than assumed (M8.53): a pod 5 min
        // fast judges a healthy leader's expiry already past, so its first
        // commit takes the term. It then writes expiries 5 min further out than
        // a true clock would, so nobody takes it BACK: one takeover, not one
        // per renew interval. The liveness cost is the dead-fast-leader case
        // above, not churn. Safety, as ever, is the SEAL.
        UUID index = UUID.randomUUID();
        List<NodeProcess> nodes = new CopyOnWriteArrayList<>();
        Set<String> acked = ConcurrentHashMap.newKeySet();
        AtomicBoolean stop = new AtomicBoolean();
        List<Thread> producers = new ArrayList<>();
        try (ChaosBucket bucket = ChaosBucket.create()) {
            NodeProcess honest = NodeProcess.start(Files.createDirectories(dir.resolve("h")),
                    "pod0", settings(bucket));
            nodes.add(honest);
            honest.registerLogs(index);
            assertThat(honest.write("honest-first", 1)).isEqualTo(202);
            Lease first = bucket.lease().orElseThrow();
            assertThat(first.holderPodId()).as("the premise: the honest pod leads")
                    .isEqualTo("pod0");

            NodeProcess fast = NodeProcess.start(Files.createDirectories(dir.resolve("f")),
                    "pod1", settings(bucket), new NodeProcess.Options(SKEW, false));
            nodes.add(fast);
            fast.registerLogs(index);
            for (int p = 0; p < 4; p++) {
                int producer = p;
                producers.add(Thread.ofVirtual().start(() ->
                        KillSequencerMidCommitIT.produce(producer, nodes, acked, stop)));
            }
            // ⚠️ SEVERAL RENEW INTERVALS, so churn would show as epochs
            long observationStarted = System.nanoTime();
            long listsBefore = bucket.observerCounts().lists();
            await().pollInterval(Duration.ofSeconds(1)).atMost(Duration.ofSeconds(30))
                    .untilAsserted(() -> assertStableFastTerm(bucket, first));
            Duration observation = LEASE_TTL.multipliedBy(3);
            await().pollInterval(Duration.ofSeconds(1)).during(observation)
                    .atMost(observation.plusSeconds(2))
                    .untilAsserted(() -> assertStableFastTerm(bucket, first));
            assertObservedListRate(bucket, listsBefore, observationStarted);
            Lease after = bucket.lease().orElseThrow();
            stop.set(true);
            for (Thread producer : producers) {
                producer.join(TimeUnit.SECONDS.toMillis(30));
            }
            for (NodeProcess node : nodes) {
                node.terminate();
            }
            ChainAudit audit = ChainAudit.of(bucket);
            Set<String> committed = KillSequencerMidCommitIT
                    .committedIds(bucket, audit.committedSegments()).keySet();
            Set<String> lost = new HashSet<>(acked);
            lost.removeAll(committed);

            System.out.println("M8.53 live fast pod: epochs " + audit.epochs() + ", term now "
                    + after.holderPodId() + "@" + after.epoch() + " (started "
                    + first.holderPodId() + "@" + first.epoch() + "), acked " + acked.size()
                    + ", lost " + lost.size());
            assertThat(after.holderPodId())
                    .as("⚠️ LIVENESS, AS PREDICTED: THE FAST POD TOOK THE TERM").isEqualTo("pod1");
            assertThat(after.epoch() - first.epoch())
                    .as("⚠️ ONCE, NOT ONCE PER RENEW INTERVAL: its expiries look valid to "
                            + "everyone else for the skew and more")
                    .isEqualTo(1);
            assertThat(audit.violations()).as("⚠️ SAFETY BY EPOCH").isEmpty();
            assertThat(lost).as("⚠️ AND NOTHING ACKED WAS LOST").isEmpty();
            assertThat(acked).as("the premise: it was under load").hasSizeGreaterThan(20);
        } finally {
            stop.set(true);
            nodes.forEach(NodeProcess::close);
        }
    }

    private static void assertStableFastTerm(ChaosBucket bucket, Lease initial) throws Exception {
        Lease current = bucket.lease().orElseThrow();
        assertThat(current.holderPodId()).isEqualTo("pod1");
        assertThat(current.epoch() - initial.epoch()).isEqualTo(1);
    }

    private static void assertObservedListRate(ChaosBucket bucket, long listsBefore,
            long startedNanos) {
        long elapsedNanos = System.nanoTime() - startedNanos;
        long allowance = (long) Math.ceil(elapsedNanos / (double) TimeUnit.SECONDS.toNanos(1)) + 1;
        long lists = bucket.observerCounts().lists() - listsBefore;
        assertThat(lists).as("RustFS observer LIST rate over %.3f seconds", elapsedNanos / 1e9)
                .isLessThanOrEqualTo(allowance);
    }
}
