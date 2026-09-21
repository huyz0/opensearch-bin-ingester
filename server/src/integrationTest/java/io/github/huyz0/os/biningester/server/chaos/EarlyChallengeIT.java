// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.huyz0.os.biningester.binstore.backend.S3Fixture;
import io.github.huyz0.os.biningester.format.ChainEntry;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.Lease;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
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
 * NFR-9: offset visibility resumes in under 5 s after the sequencer dies,
 * because the {@code EndpointSlice} watch drives an early challenge (M8.13,
 * M8 criterion 10).
 *
 * <p>⚠️ **ASSERTED AGAINST THE WATCH, NOT AGAINST THE TTL.** The TTL here is
 * 20 s, four times the bound, so expiry cannot be what met it. Two more
 * things make it hold if the TTL is ever tuned down:
 * <ul>
 * <li>the new term's first delta lands while the killed leader's lease is
 *     still unexpired, which only a challenge can do;
 * <li>the same kill with no watch configured takes at least the TTL minus
 *     one renew interval. That is the latest the killed leader can have
 *     renewed, so it is the earliest its lease can lapse.
 * </ul>
 * Without these two, a short TTL would let NFR-9 read as met by expiry,
 * which is what M5.21 moved this requirement to M8 to escape.
 *
 * <p>⚠️ **THE TEST REMOVES THE ENDPOINT, AS THE CONTROLLER WOULD**
 * ({@link FakeKubeApi}), right after the kill, so what is measured is the
 * ingester's reaction to the event, not the controller's own latency.
 */
@Timeout(value = 600, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class EarlyChallengeIT {

    private static final Duration LEASE_TTL = Duration.ofSeconds(20);
    private static final Duration RENEW = Duration.ofSeconds(2);
    private static final Duration NFR_9 = Duration.ofSeconds(5);
    private static final int NODES = 3;
    private static final int PRODUCERS = 6;

    @TempDir
    Path dir;

    @BeforeAll
    static void container() {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a chaos suite");
    }

    /** What one kill measured. */
    private record Failover(Duration lag, Lease before, Lease after, long firstNewDeltaAtMillis) {
    }

    @Test
    void withTheWATCHVisibilityResumesInUNDER5sBeforeTheOldLeaseCOULDHaveExpired()
            throws Exception {
        try (FakeKubeApi kube = new FakeKubeApi()) {
            Failover failover = killTheLeader(kube);

            System.out.println("M8.13 with the watch: visibility resumed "
                    + failover.lag().toMillis() + " ms after the kill (NFR-9 "
                    + NFR_9.toMillis() + " ms, TTL " + LEASE_TTL.toMillis() + " ms); epoch "
                    + failover.before().epoch() + " -> " + failover.after().epoch());
            assertThat(failover.lag()).as("⚠️ NFR-9: UNDER 5 s").isLessThan(NFR_9);
            assertThat(failover.firstNewDeltaAtMillis())
                    .as("⚠️ THE CHALLENGE, NOT EXPIRY: the new term committed while the killed "
                            + "leader's lease was still unexpired")
                    .isLessThan(failover.before().expiresAtMillis());
            assertThat(failover.after().epoch()).isGreaterThan(failover.before().epoch());
        }
    }

    @Test
    void withoutTheWATCHTheSameKillWaitsOUTTheTTL() throws Exception {
        Failover failover = killTheLeader(null);

        System.out.println("M8.13 without the watch: visibility resumed "
                + failover.lag().toMillis() + " ms after the kill (TTL "
                + LEASE_TTL.toMillis() + " ms)");
        assertThat(failover.lag())
                .as("⚠️ NO EVIDENCE, NO CHALLENGE: at least the TTL less one renew interval, "
                        + "the earliest the killed leader's lease can lapse")
                .isGreaterThanOrEqualTo(LEASE_TTL.minus(RENEW));
    }

    private Failover killTheLeader(FakeKubeApi kube) throws Exception {
        UUID index = UUID.randomUUID();
        List<NodeProcess> nodes = new CopyOnWriteArrayList<>();
        Set<String> acked = ConcurrentHashMap.newKeySet();
        AtomicBoolean stop = new AtomicBoolean();
        List<Thread> producers = new ArrayList<>();
        try (ChaosBucket bucket = ChaosBucket.create()) {
            Map<String, String> settings = new HashMap<>(bucket.nodeSettings());
            settings.put("ingest.interval-floor", "PT0.05S");
            settings.put("lease.ttl", LEASE_TTL.toString());
            settings.put("lease.renew-interval", RENEW.toString());
            if (kube != null) {
                settings.putAll(kube.nodeSettings());
            }
            for (int i = 0; i < NODES; i++) {
                String pod = "pod" + i;
                if (kube != null) {
                    kube.ready(pod, "10.0.0." + (i + 1));
                }
                NodeProcess node = NodeProcess.start(
                        Files.createDirectories(dir.resolve((kube == null ? "off" : "on") + i)),
                        pod, settings);
                node.registerLogs(index);
                nodes.add(node);
            }
            if (kube != null) {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (kube.watchers() < NODES) {
                    assertThat(System.nanoTime()).as("every node watches").isLessThan(deadline);
                    Thread.sleep(50);
                }
            }
            for (int p = 0; p < PRODUCERS; p++) {
                int producer = p;
                producers.add(Thread.ofVirtual().start(() ->
                        KillSequencerMidCommitIT.produce(producer, nodes, acked, stop)));
            }
            KillSequencerMidCommitIT.awaitAcks(acked, 300);

            Lease before = bucket.lease().orElseThrow();
            NodeProcess leader = nodes.stream()
                    .filter(n -> n.podId().equals(before.holderPodId())).findFirst()
                    .orElseThrow();
            leader.killNow();
            long killed = System.nanoTime();
            if (kube != null) {
                kube.remove(leader.podId());
            }
            nodes.remove(leader);

            Lease after;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (true) {
                after = bucket.lease().orElseThrow();
                if (after.epoch() > before.epoch()
                        && hasDelta(bucket, after.epoch())) {
                    break;
                }
                assertThat(System.nanoTime()).as("visibility never resumed").isLessThan(deadline);
                Thread.sleep(20);
            }
            Duration lag = Duration.ofNanos(System.nanoTime() - killed);
            long firstNewDeltaAt = System.currentTimeMillis();
            stop.set(true);
            for (Thread producer : producers) {
                producer.join(TimeUnit.SECONDS.toMillis(30));
            }
            leader.close();
            return new Failover(lag, before, after, firstNewDeltaAt);
        } finally {
            stop.set(true);
            nodes.forEach(NodeProcess::close);
        }
    }

    private static boolean hasDelta(ChaosBucket bucket, long epoch) throws Exception {
        String chain = ChaosBucket.PREFIX + "/ctl/log/0/" + String.format("%016x", epoch) + "/";
        for (String key : bucket.keys(chain)) {
            if (key.endsWith(".delta")
                    && ChainEntry.decode(bucket.get(key)) instanceof CommitDelta) {
                return true;
            }
        }
        return false;
    }
}
