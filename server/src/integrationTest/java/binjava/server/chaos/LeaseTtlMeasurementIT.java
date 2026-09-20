// SPDX-License-Identifier: Apache-2.0
package binjava.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import binjava.binstore.backend.MinioFixture;
import binjava.format.Lease;
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
 * Measurement M1: the SHIPPED lease TTL under {@code SIGSTOP} pauses of chosen
 * lengths, reported as numbers (M8.27, FR-11, M8 criterion 19).
 *
 * <p>⚠️ **A {@code SIGSTOP} OF A CHOSEN LENGTH IS A GC PAUSE**, every thread
 * frozen, the renewer included. No setting is overridden: this measures what
 * ships.
 *
 * <p>⚠️ **WHAT DECIDES THE TAKEOVER IS THE FORWARD, NOT THE RENEW PHASE.** A
 * follower elects only once a commit it forwarded to the frozen leader fails,
 * and that forward waits out the peer-commit timeout, which IS the TTL (M8.12).
 * So a takeover lands about one TTL after the first forward following the
 * stop, whenever in its renew cycle the lease was: measured at TTL + ~0.3 s in
 * every sample, where a phase-driven takeover would spread over
 * {@code TTL - renew .. TTL}.
 *
 * <p>⚠️ **IT REPORTS NUMBERS AND ASSERTS ONLY A DIRECTION**: a pause shorter than
 * the TTL causes NO takeover, one longer causes EXACTLY ONE, and none fires
 * before the TTL. The tolerance itself is printed, not pinned: that would pin
 * the JVM, which is not ours to pin.
 *
 * <p>⚠️ **THE CHALLENGE HALF IS NOT MEASURED HERE, AND M8.55 OWNS IT.** An early
 * challenge needs {@code EndpointSlice} evidence (M8.13), and nothing here
 * produces any: in a cluster a long enough {@code SIGSTOP} fails the pod's
 * readiness probe, and the watch would then take the term BEFORE the TTL.
 */
@Timeout(value = 900, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LeaseTtlMeasurementIT {

    /** The TTL that ships, read from the configuration a node parses: not restated. */
    private static final Duration TTL = shipped().leaseTtl();

    /** The renew interval that ships. */
    private static final Duration RENEW = shipped().leaseRenewInterval();

    private static binjava.server.ServerConfig shipped() {
        return binjava.server.ServerProperties.parse(Map.of("pod.id", "pod0", "pod.az", "az-a",
                "trust.domain", "cluster-a", "store.prefix", "p", "store.kind", "memory",
                "endpoint", "http://localhost:0", "http.port", "0",
                "producer.subject", "producer-1", "producer.allowed-indices", "logs"));
    }

    @TempDir
    Path dir;

    @BeforeAll
    static void container() {
        assumeTrue(MinioFixture.dockerAvailable(), "no Docker daemon: this is a chaos suite");
    }

    /** What one pause did. */
    private record Outcome(Duration pause, long epochsMoved, long takeoverMillis, int lost) {
    }

    private Outcome pauseTheLeaderFor(Duration pause, String run) throws Exception {
        UUID index = UUID.randomUUID();
        List<NodeProcess> nodes = new CopyOnWriteArrayList<>();
        Set<String> acked = ConcurrentHashMap.newKeySet();
        AtomicBoolean stop = new AtomicBoolean();
        try (ChaosBucket bucket = ChaosBucket.create()) {
            Map<String, String> settings = new HashMap<>(bucket.nodeSettings());
            settings.put("ingest.interval-floor", "PT0.05S");
            NodeProcess leader = NodeProcess.start(
                    Files.createDirectories(dir.resolve(run + "l")), "pod0", settings);
            nodes.add(leader);
            leader.registerLogs(index);
            assertThat(leader.write(run + "-first", 1)).isEqualTo(202);
            NodeProcess follower = NodeProcess.start(
                    Files.createDirectories(dir.resolve(run + "f")), "pod1", settings);
            nodes.add(follower);
            follower.registerLogs(index);
            // ⚠️ THROUGH THE FOLLOWER ONLY: its commits are what would elect it,
            // and a request to the frozen leader would only wait out a timeout.
            Thread producer = Thread.ofVirtual().start(() ->
                    KillSequencerMidCommitIT.produce(0, List.of(follower), acked, stop));
            KillSequencerMidCommitIT.awaitAcks(acked, 20);
            Lease before = bucket.lease().orElseThrow();
            assertThat(before.holderPodId()).as("the premise: pod0 leads").isEqualTo("pod0");

            leader.pause();
            long paused = System.nanoTime();
            long takeover = -1;
            while (System.nanoTime() - paused < pause.toNanos()) {
                if (takeover < 0 && bucket.lease().orElseThrow().epoch() > before.epoch()) {
                    takeover = System.nanoTime() - paused;
                }
                Thread.sleep(100);
            }
            leader.resume();
            // settle: a resumed leader that could retake the term does it here
            long settle = System.nanoTime() + RENEW.multipliedBy(2).toNanos();
            while (System.nanoTime() < settle) {
                if (takeover < 0 && bucket.lease().orElseThrow().epoch() > before.epoch()) {
                    takeover = System.nanoTime() - paused;
                }
                Thread.sleep(100);
            }
            Lease after = bucket.lease().orElseThrow();
            stop.set(true);
            producer.join(TimeUnit.SECONDS.toMillis(30));
            for (NodeProcess node : nodes) {
                node.terminate();
            }
            ChainAudit audit = ChainAudit.of(bucket);
            assertThat(audit.violations()).as("⚠️ SAFETY, after a %s pause", pause).isEmpty();
            Set<String> lost = new HashSet<>(acked);
            lost.removeAll(KillSequencerMidCommitIT
                    .committedIds(bucket, audit.committedSegments()).keySet());
            return new Outcome(pause, after.epoch() - before.epoch(),
                    takeover < 0 ? -1 : TimeUnit.NANOSECONDS.toMillis(takeover), lost.size());
        } finally {
            stop.set(true);
            nodes.forEach(NodeProcess::close);
        }
    }

    @Test
    void theSHIPPEDTtlTakesOverONLYForAPauseItCannotAbsorb() throws Exception {
        List<Outcome> outcomes = new ArrayList<>();
        // ⚠️ DERIVED FROM THE TTL THAT SHIPS, both sides of it and close to it:
        // fixed pauses let a raised TTL slide past every "longer" sample, and
        // then nothing longer than the TTL was ever asserted.
        List<Duration> pauses = List.of(TTL.multipliedBy(2).dividedBy(10),
                TTL.multipliedBy(4).dividedBy(10), TTL.multipliedBy(6).dividedBy(10),
                TTL.multipliedBy(8).dividedBy(10), TTL.minusSeconds(1), TTL.plusSeconds(1),
                TTL.plusSeconds(2), TTL.multipliedBy(2));
        assertThat(pauses).as("the premise: samples on BOTH sides of the TTL")
                .anyMatch(p -> p.compareTo(TTL) < 0).anyMatch(p -> p.compareTo(TTL) > 0);
        for (Duration pause : pauses) {
            outcomes.add(pauseTheLeaderFor(pause, "p" + pause.toMillis()));
        }
        StringBuilder report = new StringBuilder("M8.27 measurement M1 at TTL " + TTL
                + ", renew " + RENEW + ":");
        for (Outcome o : outcomes) {
            report.append("\n  pause ").append(o.pause().toSeconds()).append(" s: ")
                    .append(o.epochsMoved()).append(" takeover(s)")
                    .append(o.takeoverMillis() < 0 ? ""
                            : ", first " + o.takeoverMillis() + " ms after the SIGSTOP")
                    .append(", lost ").append(o.lost());
        }
        System.out.println(report);

        for (Outcome o : outcomes) {
            assertThat(o.lost()).as("⚠️ NOTHING ACKED IS LOST, after %s", o.pause()).isZero();
            if (o.pause().compareTo(TTL) < 0) {
                assertThat(o.epochsMoved())
                        .as("⚠️ A %s PAUSE, SHORTER THAN THE TTL, IS ABSORBED", o.pause())
                        .isZero();
            } else {
                assertThat(o.epochsMoved())
                        .as("⚠️ A %s PAUSE, LONGER THAN THE TTL: EXACTLY ONE takeover",
                                o.pause())
                        .isEqualTo(1);
                assertThat(o.takeoverMillis())
                        .as("⚠️ AND NOT BEFORE THE TTL: a takeover on silence alone would be "
                                + "a failover per GC pause")
                        .isGreaterThanOrEqualTo(TTL.toMillis());
            }
        }
    }
}
