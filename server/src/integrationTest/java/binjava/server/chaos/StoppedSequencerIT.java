// SPDX-License-Identifier: Apache-2.0
package binjava.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import binjava.binstore.backend.MinioFixture;
import binjava.format.ChainEntry;
import binjava.format.CommitDelta;
import binjava.format.Lease;
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
 * {@code SIGSTOP} on the sequencer: the gray failure (M8.12, FR-11, NFR-11,
 * M8 criterion 9).
 *
 * <p>⚠️ **THE ROW RESEARCH 08 §9 SAYS FINDS REAL BUGS.** A stopped process
 * renews no lease and answers no commit, yet it is not dead: on {@code
 * SIGCONT} it resumes mid-thought, still believing it leads. So three things
 * are asserted, all at the store:
 * <ol>
 * <li>visibility resumes, meaning a delta lands under a new epoch, within
 *     {@code leaseTtl} + 5 s. That is stated relative to the CONFIGURED TTL,
 *     because criterion 19 is what measures the TTL, and the measured
 *     number is reported;
 * <li>the new epoch is strictly greater;
 * <li>the resumed process commits NOTHING under its old epoch: that chain
 *     gains no delta after the pause. This is epoch fencing under the one
 *     failure that produces a live stale leader, which crash-stop testing
 *     cannot reach.
 * </ol>
 *
 * <p>⚠️ **LEASEHOLDER IDENTITY IS ASSERTED BEFORE THE STOP**, from the lease
 * object, because a stopped follower proves nothing and passes.
 */
@Timeout(value = 600, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class StoppedSequencerIT {

    private static final Duration LEASE_TTL = Duration.ofSeconds(4);
    private static final int NODES = 3;
    private static final int PRODUCERS = 6;
    private static final int BATCH = 50;

    @TempDir
    Path dir;

    @BeforeAll
    static void container() {
        assumeTrue(MinioFixture.dockerAvailable(), "no Docker daemon: this is a chaos suite");
    }

    @Test
    void aSTOPPEDSequencerIsReplacedWithinTheTTLAndCommitsNOTHINGWhenItWAKES()
            throws Exception {
        UUID index = UUID.randomUUID();
        List<NodeProcess> nodes = new CopyOnWriteArrayList<>();
        Set<String> acked = ConcurrentHashMap.newKeySet();
        AtomicBoolean stop = new AtomicBoolean();
        List<Thread> producers = new ArrayList<>();
        try (ChaosBucket bucket = ChaosBucket.create()) {
            Map<String, String> settings = new HashMap<>(bucket.nodeSettings());
            settings.put("ingest.interval-floor", "PT0.05S");
            settings.put("lease.ttl", LEASE_TTL.toString());
            settings.put("lease.renew-interval", "PT1S");
            for (int i = 0; i < NODES; i++) {
                NodeProcess node = NodeProcess.start(
                        Files.createDirectories(dir.resolve("n" + i)), "pod" + i, settings);
                node.registerLogs(index);
                nodes.add(node);
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
            assertThat(before.expiresAtMillis())
                    .as("⚠️ THE PREMISE: THE PROCESS ABOUT TO BE STOPPED HOLDS A LIVE TERM")
                    .isGreaterThan(System.currentTimeMillis());
            String oldChain = chainOf(before.epoch());

            leader.pause();
            long stopped = System.nanoTime();
            int deltasAtStop = deltas(bucket, oldChain);
            Lease after;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (true) {
                after = bucket.lease().orElseThrow();
                if (after.epoch() > before.epoch() && deltas(bucket, chainOf(after.epoch())) > 0) {
                    break;
                }
                assertThat(System.nanoTime()).as("visibility never resumed").isLessThan(deadline);
                Thread.sleep(50);
            }
            Duration lag = Duration.ofNanos(System.nanoTime() - stopped);

            // ⚠️ AND THEN IT WAKES, while producers are still sending to it.
            leader.resume();
            KillSequencerMidCommitIT.awaitAcks(acked, acked.size() + 300);
            int deltasAfterWaking = deltas(bucket, oldChain);
            stop.set(true);
            for (Thread producer : producers) {
                producer.join(TimeUnit.SECONDS.toMillis(60));
            }
            for (NodeProcess node : nodes) {
                node.terminate();
            }
            ChainAudit audit = ChainAudit.of(bucket);
            Set<String> committed = KillSequencerMidCommitIT
                    .committedIds(bucket, audit.committedSegments()).keySet();

            System.out.println("M8.12 visibility resumed " + lag.toMillis() + " ms after SIGSTOP"
                    + " (bound " + LEASE_TTL.plusSeconds(5).toMillis() + " ms); epoch "
                    + before.epoch() + " -> " + after.epoch() + "; stopped leader "
                    + leader.podId() + "; old chain deltas " + deltasAtStop + " -> "
                    + deltasAfterWaking + " (a delta landing while frozen is sealed in)");
            assertThat(lag).as("⚠️ VISIBILITY RESUMES WITHIN leaseTtl + 5 s")
                    .isLessThan(LEASE_TTL.plusSeconds(5));
            assertThat(after.epoch()).as("⚠️ UNDER A STRICTLY GREATER EPOCH")
                    .isGreaterThan(before.epoch());
            List<ChainEntry> old = audit.entries(before.epoch());
            int seal = -1;
            for (int i = 0; i < old.size(); i++) {
                if (old.get(i) instanceof binjava.format.Seal) {
                    seal = i;
                    break;
                }
            }
            assertThat(seal).as("the successor sealed the old chain").isGreaterThanOrEqualTo(0);
            // ⚠️ AFTER ITS SEAL, NOT AFTER THE PAUSE. MEASURED: a delta PUT
            // sent just before SIGSTOP can land while the process is frozen,
            // before the successor seals -- and that one IS committed, in the
            // sealed prefix. What the woken leader must not do is write PAST
            // the SEAL, which is what a fencing failure looks like at the store.
            assertThat(old.subList(seal + 1, old.size()))
                    .as("⚠️ THE WOKEN PROCESS COMMITTED NOTHING UNDER ITS OLD EPOCH")
                    .noneMatch(entry -> entry instanceof CommitDelta);
            if (!audit.violations().isEmpty()) {
                audit.epochs().forEach(e -> System.out.println(audit.describe(e)));
            }
            assertThat(audit.violations()).isEmpty();
            assertThat(committed).as("and nothing acked was lost").containsAll(acked);
        } finally {
            stop.set(true);
            nodes.forEach(NodeProcess::close);
        }
    }

    private static String chainOf(long epoch) {
        return ChaosBucket.PREFIX + "/ctl/log/0/" + String.format("%016x", epoch) + "/";
    }

    /** How many commit deltas the chain holds. SEAL and CONTINUE are not counted. */
    private static int deltas(ChaosBucket bucket, String chain) throws Exception {
        int deltas = 0;
        for (String key : bucket.keys(chain)) {
            if (key.endsWith(".delta")
                    && ChainEntry.decode(bucket.get(key)) instanceof CommitDelta) {
                deltas++;
            }
        }
        return deltas;
    }
}
