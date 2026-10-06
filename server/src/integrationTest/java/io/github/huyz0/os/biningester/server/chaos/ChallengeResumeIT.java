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
 * The challenge half of measurement M1: a leader frozen by {@code SIGSTOP}
 * loses its term with the {@code EndpointSlice} watch fed, then resumes
 * believing it still holds it (M8.55, FR-11, NFR-9).
 *
 * <p>⚠️ **THE ROW EXPECTED THE PAUSE ABSORBED TO BE THE PROBE's FAILURE WINDOW**,
 * not the TTL M8.27 measured: a frozen pod fails its readiness probe, the
 * controller drops it from the {@code EndpointSlice}, and the watch (M8.13)
 * would let a follower take the term while the frozen lease is unexpired.
 * {@link FakeKubeApi} plays the controller, removing the endpoint after a
 * notional probe window, so what is measured is the ingester's reaction.
 *
 * <p>⚠️ **M8.55 MEASURED THAT IT DID NOT**: the takeover landed at about the
 * TTL even with the watch fed, because the follower's flush was parked in a
 * forward to the frozen leader until the peer-commit timeout, and the evidence
 * was read only by the election that followed it. M8.57 made the forward read
 * the evidence while it waits, and the pause is now asserted under NFR-9's 5 s.
 *
 * <p>⚠️ **{@code EarlyChallengeIT} KILLS ITS LEADER; THIS ONE COMES BACK.** A
 * resumed leader's renewer and commit path wake mid-flight with a term they
 * no longer hold. The property is that once the successor has SEALED that
 * term, nothing more lands under it -- an in-flight commit may still win a
 * slot BELOW the seal, which is what the seal exists to allow.
 */
@Timeout(value = 600, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class ChallengeResumeIT {

    /** The TTL that ships, read from the configuration a node parses: not restated. */
    private static final Duration TTL = io.github.huyz0.os.biningester.server.ServerProperties.parse(Map.of(
            "pod.id", "pod0", "pod.uid", "uid-pod0", "pod.az", "az-a", "trust.domain", "cluster-a", "store.prefix", "p",
            "store.kind", "memory", "endpoint", "http://localhost:0", "http.port", "0",
            "producer.subject", "producer-1", "producer.allowed-indices", "logs")).leaseTtl();

    /**
     * A notional readiness-probe failure window, well inside the TTL, so a
     * takeover driven by the evidence would land long before one driven by it.
     */
    private static final Duration PROBE_WINDOW = TTL.dividedBy(5);

    @TempDir
    Path dir;

    @BeforeAll
    static void container() {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a chaos suite");
    }

    @Test
    void aPAUSEDLeaderLOSESItsTermAndOnRESUMECommitsNOTHINGUnderIt() throws Exception {
        UUID index = UUID.randomUUID();
        List<NodeProcess> nodes = new CopyOnWriteArrayList<>();
        Set<String> acked = ConcurrentHashMap.newKeySet();
        AtomicBoolean stop = new AtomicBoolean();
        try (FakeKubeApi kube = new FakeKubeApi(); ChaosBucket bucket = ChaosBucket.create()) {
            Map<String, String> settings = new HashMap<>(bucket.nodeSettings());
            settings.put("ingest.interval-floor", "PT0.05S");
            settings.putAll(kube.nodeSettings());
            kube.ready("pod0", "10.0.0.1");
            kube.ready("pod1", "10.0.0.2");
            NodeProcess leader = NodeProcess.start(
                    Files.createDirectories(dir.resolve("l")), "pod0", settings);
            nodes.add(leader);
            leader.registerLogs(index);
            assertThat(leader.write("first", 1)).isEqualTo(202);
            NodeProcess follower = NodeProcess.start(
                    Files.createDirectories(dir.resolve("f")), "pod1", settings);
            nodes.add(follower);
            follower.registerLogs(index);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (kube.watchers() < 2) {
                assertThat(System.nanoTime()).as("both nodes watch").isLessThan(deadline);
                Thread.sleep(50);
            }
            Thread viaFollower = Thread.ofVirtual().start(() ->
                    KillSequencerMidCommitIT.produce(0, List.of(follower), acked, stop));
            KillSequencerMidCommitIT.awaitAcks(acked, 20);
            Lease before = bucket.lease().orElseThrow();
            assertThat(before.holderPodId()).as("the premise: pod0 leads").isEqualTo("pod0");

            leader.pause();
            long paused = System.nanoTime();
            Thread.sleep(PROBE_WINDOW.toMillis());
            kube.remove("pod0");
            Lease after;
            deadline = System.nanoTime() + TTL.multipliedBy(3).toNanos();
            while ((after = bucket.lease().orElseThrow()).epoch() == before.epoch()) {
                assertThat(System.nanoTime()).as("no takeover at all").isLessThan(deadline);
                Thread.sleep(20);
            }
            Duration absorbed = Duration.ofNanos(System.nanoTime() - paused);

            // ⚠️ THE SNAPSHOT WAITS FOR THE SUCCESSOR's FIRST COMMIT, which
            // lands only after its SEAL of the old chain. Taken at the lease
            // change instead, it raced the seal: MEASURED, a leader resumed
            // there won slot 3 before the seal took 4 -- legal, and exactly
            // what the seal is for. After the seal, nothing more may land.
            deadline = System.nanoTime() + TTL.multipliedBy(3).toNanos();
            while (deltasUnder(bucket, after.epoch()).isEmpty()) {
                assertThat(System.nanoTime()).as("the successor never committed")
                        .isLessThan(deadline);
                Thread.sleep(20);
            }
            Set<Long> oldDeltas = deltasUnder(bucket, before.epoch());

            kube.ready("pod0", "10.0.0.1");
            leader.resume();
            Thread viaResumed = Thread.ofVirtual().start(() ->
                    KillSequencerMidCommitIT.produce(1, List.of(leader), acked, stop));
            Thread.sleep(TTL.toMillis());
            stop.set(true);
            viaFollower.join(TimeUnit.SECONDS.toMillis(30));
            viaResumed.join(TimeUnit.SECONDS.toMillis(30));
            assertThat(viaFollower.isAlive()).as("follower producer completed").isFalse();
            assertThat(viaResumed.isAlive()).as("resumed-leader producer completed").isFalse();
            Lease completed = bucket.lease().orElseThrow();
            assertThat(completed.epoch())
                    .as("exactly one takeover through stable completion, including after the earlier read")
                    .isEqualTo(before.epoch() + 1);
            for (NodeProcess node : nodes) {
                node.terminate();
            }

            System.out.println("M8.55: a " + PROBE_WINDOW.toMillis() + " ms probe window "
                    + "absorbed a pause of " + absorbed.toMillis() + " ms (TTL "
                    + TTL.toMillis() + " ms); epoch " + before.epoch() + " -> "
                    + after.epoch());
            // ⚠️ NFR-9, AND M8.57's BOUND. M8.55 MEASURED ~TTL + 0.3 s here: the
            // follower's flush sat in a forward to the frozen leader until the
            // peer-commit timeout, which IS the TTL, and the watch's evidence
            // was read only by the election after it. The forward now reads it
            // while it waits.
            assertThat(absorbed)
                    .as("⚠️ NFR-9: THE PROBE WINDOW PLUS A CHALLENGE, UNDER 5 s -- not the "
                            + "%s TTL", TTL)
                    .isLessThan(Duration.ofSeconds(5));
            assertThat(after.epoch())
                    .as("EXACTLY ONE takeover").isEqualTo(before.epoch() + 1);
            assertThat(deltasUnder(bucket, before.epoch()))
                    .as("⚠️ THE RESUMED LEADER COMMITTED NOTHING UNDER THE TERM IT LOST, once "
                            + "its successor had sealed it")
                    .isEqualTo(oldDeltas);
            ChainAudit audit = ChainAudit.of(bucket);
            assertThat(audit.violations()).as("⚠️ SAFETY").isEmpty();
            Set<String> lost = new HashSet<>(acked);
            lost.removeAll(KillSequencerMidCommitIT
                    .committedIds(bucket, audit.committedSegments()).keySet());
            assertThat(lost).as("⚠️ NOTHING ACKED IS LOST").isEmpty();
        } finally {
            stop.set(true);
            nodes.forEach(NodeProcess::close);
        }
    }

    /** The sequences of every COMMIT delta under {@code epoch}: seals are not commits. */
    private static Set<Long> deltasUnder(ChaosBucket bucket, long epoch) throws Exception {
        String chain = ChaosBucket.PREFIX + "/ctl/log/0/" + String.format("%016x", epoch) + "/";
        Set<Long> sequences = new HashSet<>();
        for (String key : bucket.keys(chain)) {
            if (key.endsWith(".delta")
                    && ChainEntry.decode(bucket.get(key)) instanceof CommitDelta delta) {
                sequences.add(delta.sequence());
            }
        }
        return sequences;
    }
}
