// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.huyz0.os.biningester.binstore.backend.S3Fixture;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.format.RunEntry;
import io.github.huyz0.os.biningester.format.SegmentReader;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.nio.file.Files;
import java.nio.file.Path;
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
 * Killing the sequencer mid-commit, over and over: I1-I5 hold, every SEAL
 * succeeds, and no offset is ever reassigned (M8.11, FR-11, NFR-11, M8
 * criterion 8).
 *
 * <p>⚠️ **M4 ASSERTED I1-I5 ACROSS 1,000 SIMULATION SEEDS; THIS IS THE SAME
 * CLAIM ACROSS REAL PROCESS DEATHS**, which is the difference between a model
 * of the protocol and the protocol. Every judgement is made from the bucket
 * by {@link ChainAudit}, after every process that wrote it is gone.
 *
 * <p>⚠️ **THE KILL IS SYNCHRONISED TO A COMMIT LANDING, NOT TIMED.** The
 * leader is identified from the lease at the store. It is NOT assumed: a kill
 * of a follower proves nothing and passes. The test then watches that
 * leader's chain, and sends SIGKILL the moment a new delta appears in it, so
 * the successor must seal a chain whose last delta has just landed and
 * continue its offsets from after it. Whether the kill also beats the ack
 * varies from run to run: a duplicated id is a batch whose commit landed
 * and whose producer was never told, so it retried. The count is reported,
 * and runs of this tree have shown both 0 and 100.
 */
@Timeout(value = 900, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class KillSequencerMidCommitIT {

    private static final int NODES = 3;
    private static final int ROUNDS = 3;
    private static final int PRODUCERS = 6;
    private static final int BATCH = 50;

    @TempDir
    Path dir;

    @BeforeAll
    static void container() {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a chaos suite");
    }

    @Test
    void killingTheSEQUENCERMidCommitKEEPSI1ToI5AndEverySEALSucceeds() throws Exception {
        UUID index = UUID.randomUUID();
        List<NodeProcess> nodes = new CopyOnWriteArrayList<>();
        Set<String> acked = ConcurrentHashMap.newKeySet();
        AtomicBoolean stop = new AtomicBoolean();
        List<String> killed = new ArrayList<>();
        List<Thread> producers = new ArrayList<>();
        try (ChaosBucket bucket = ChaosBucket.create()) {
            Map<String, String> settings = new HashMap<>(bucket.nodeSettings());
            settings.put("ingest.interval-floor", "PT0.05S");
            // ⚠️ SHORT, so a takeover follows a kill in seconds rather than the
            // default TTL; the protocol is the same at any TTL.
            settings.put("lease.ttl", "PT4S");
            settings.put("lease.renew-interval", "PT1S");
            for (int i = 0; i < NODES; i++) {
                NodeProcess node = NodeProcess.start(Files.createDirectories(dir.resolve("n" + i)),
                        "pod" + i, settings);
                node.registerLogs(index);
                nodes.add(node);
            }
            for (int p = 0; p < PRODUCERS; p++) {
                int producer = p;
                producers.add(Thread.ofVirtual().start(() ->
                        produce(producer, nodes, acked, stop)));
            }

            for (int round = 0; round < ROUNDS; round++) {
                awaitAcks(acked, acked.size() + 500);
                // ⚠️ ACKS NO LONGER MEAN A LIVE TERM (ADR-0058): followers ack on
                // intents while a killed leader's lease runs out. So the round
                // waits for the lease to name a running node.
                awaitLiveTerm(bucket, nodes);
                Lease lease = bucket.lease().orElseThrow();
                assertThat(lease.expiresAtMillis())
                        .as("the premise: a live term before round %d", round)
                        .isGreaterThan(System.currentTimeMillis());
                NodeProcess leader = nodes.stream()
                        .filter(n -> n.podId().equals(lease.holderPodId())).findFirst()
                        .orElseThrow(() -> new AssertionError("no running node holds the "
                                + "term; the lease names " + lease.holderPodId()));
                String chain = ChaosBucket.PREFIX + "/ctl/log/0/"
                        + String.format("%016x", lease.epoch()) + "/";
                int before = bucket.keys(chain).size();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (bucket.keys(chain).size() == before) {
                    assertThat(System.nanoTime()).as("no commit landed in round %d", round)
                            .isLessThan(deadline);
                }
                leader.killNow();
                killed.add(leader.podId() + "@epoch" + lease.epoch());
                nodes.remove(leader);
                leader.close();
                NodeProcess replacement = NodeProcess.start(
                        Files.createDirectories(dir.resolve("r" + round)), "podr" + round,
                        settings);
                replacement.registerLogs(index);
                nodes.add(replacement);
            }
            awaitAcks(acked, acked.size() + 500);
            // ⚠️ AND THE LAST KILL's SUCCESSOR, AND ITS DRAIN, BEFORE THE AUDIT: the
            // acks above may all be deferred intents (ADR-0058), which only a
            // live term's seal and drain turn into committed records.
            awaitLiveTerm(bucket, nodes);
            long drainDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (bucket.keys(ChaosBucket.PREFIX + "/ctl/inbox/0/").stream()
                    .anyMatch(k -> k.endsWith(".intent"))) {
                assertThat(System.nanoTime()).as("the inbox never drained")
                        .isLessThan(drainDeadline);
                Thread.sleep(100);
            }
            stop.set(true);
            for (Thread producer : producers) {
                producer.join(TimeUnit.SECONDS.toMillis(60));
            }
            for (NodeProcess node : nodes) {
                node.terminate();
            }

            ChainAudit audit = ChainAudit.of(bucket);
            Map<String, Integer> committed = committedIds(bucket, audit.committedSegments());
            Set<String> lost = new HashSet<>(acked);
            lost.removeAll(committed.keySet());
            System.out.println("M8.11 killed " + killed + "; epochs " + audit.epochs()
                    + "; seals " + audit.seals() + "; acked " + acked.size()
                    + "; committed ids " + committed.size() + "; duplicated "
                    + committed.values().stream().filter(n -> n > 1).count());

            assertThat(audit.violations()).as("⚠️ I1, I2, I5 AND THE SEAL/CONTINUE LINK")
                    .isEmpty();
            assertThat(audit.seals())
                    .as("⚠️ EVERY KILLED LEADER'S CHAIN WAS SEALED BY ITS SUCCESSOR")
                    .isGreaterThanOrEqualTo(ROUNDS);
            assertThat(lost)
                    .as("⚠️ I4 AND THE ACK HALF OF I5: every acked record is in a segment the "
                            + "EFFECTIVE history names -- not past a SEAL, not dropped")
                    .isEmpty();
        } finally {
            stop.set(true);
            nodes.forEach(NodeProcess::close);
        }
    }

    /**
     * One producer: batches of unique ids, each retried until some node acks
     * it, across whichever nodes are alive. At-least-once, as a real producer
     * is.
     */
    static void produce(int producer, List<NodeProcess> nodes, Set<String> acked,
            AtomicBoolean stop) {
        for (int seq = 0; !stop.get(); seq++) {
            List<String> ids = new ArrayList<>();
            for (int i = 0; i < BATCH; i++) {
                ids.add("p" + producer + "-" + seq + "-" + i);
            }
            for (int attempt = 0; !stop.get(); attempt++) {
                List<NodeProcess> alive = List.copyOf(nodes);
                NodeProcess target = alive.get((producer + attempt) % alive.size());
                try {
                    if (target.write(ids) == 202) {
                        acked.addAll(ids);
                        break;
                    }
                } catch (RuntimeException connectionGone) {
                    // the node died under the request: try another
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }

    static void awaitAcks(Set<String> acked, int target) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(120);
        while (acked.size() < target) {
            assertThat(System.nanoTime()).as("acks stalled at %d", acked.size())
                    .isLessThan(deadline);
            Thread.sleep(100);
        }
    }

    /** Waits until the lease is unexpired and names a running node. */
    private static void awaitLiveTerm(ChaosBucket bucket, List<NodeProcess> nodes)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        while (true) {
            Lease current = bucket.lease().orElseThrow();
            if (current.expiresAtMillis() > System.currentTimeMillis()
                    && nodes.stream().anyMatch(n -> n.podId().equals(current.holderPodId()))) {
                return;
            }
            assertThat(System.nanoTime()).as("no live term").isLessThan(deadline);
            Thread.sleep(100);
        }
    }

    /** Every id in every committed segment, with how often it appears. */
    static Map<String, Integer> committedIds(ChaosBucket bucket, Set<String> segments)
            throws Exception {
        Map<String, Integer> found = new HashMap<>();
        for (String key : segments) {
            SegmentReader reader = SegmentReader.open(bucket.get(key));
            for (RunEntry entry : reader.directory()) {
                for (SegmentRecord record : reader.read(entry)) {
                    found.merge(record.id(), 1, Integer::sum);
                }
            }
        }
        return found;
    }
}
