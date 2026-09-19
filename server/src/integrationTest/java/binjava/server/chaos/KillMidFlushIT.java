// SPDX-License-Identifier: Apache-2.0
package binjava.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import binjava.binstore.backend.MinioFixture;
import binjava.format.RunEntry;
import binjava.format.SegmentReader;
import binjava.format.SegmentRecord;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * RPO 0: {@code SIGKILL} mid-flush loses no acked record (M8.9, NFR-8, M8
 * criterion 6).
 *
 * <p>⚠️ **THE KILL IS SYNCHRONISED TO AN EVENT, NEVER TIMED, AND THERE ARE
 * TWO EVENTS, BECAUSE THERE ARE TWO WINDOWS.** A kill at a random moment lands
 * between flushes almost every time and proves nothing.
 * <ul>
 * <li>{@link Trigger#AT_THE_ACK}: the kill is sent by the producer at the
 *     instant it sees an ack. That is the one moment an ack that ran ahead of
 *     its PUT is exposed, and it is how the mutation this row exists for,
 *     acking before the PUT completes, is caught. ⚠️ For a correct node this
 *     window is EMPTY: it PUTs and commits before it acks, so no run of this
 *     kind can land inside it, and none is asserted to.
 * <li>{@link Trigger#AT_THE_PUT}: the producer sends one more batch and
 *     watches the bucket, and the kill is sent the moment a new segment
 *     appears there. It lands inside a real flush when the segment's records
 *     are in the bucket but were never acked, which is the window a correct
 *     node does have: PUT done, commit or ack not yet. ⚠️ THAT IS MEASURED AT
 *     THE STORE, AFTER THE KILL, and at least one run must show it. The count
 *     is reported.
 * </ul>
 *
 * <p>⚠️ **DUPLICATES ARE PERMITTED, AND BOUNDED.** At-least-once with
 * {@code _id} dedup is the contract (research 08 F2), so a duplicate is not a
 * failure. But no producer here retries, so a record written many times is a
 * defect that dedup would hide while the bill grows: no id may appear more
 * than twice.
 */
@Timeout(value = 600, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class KillMidFlushIT {

    /** How each run's kill is synchronised. */
    enum Trigger { AT_THE_ACK, AT_THE_PUT }

    private static final List<Trigger> RUNS = List.of(Trigger.AT_THE_ACK, Trigger.AT_THE_PUT,
            Trigger.AT_THE_ACK, Trigger.AT_THE_PUT, Trigger.AT_THE_PUT, Trigger.AT_THE_PUT);
    private static final int PRODUCERS = 8;
    private static final int BATCH = 100;
    private static final int ACKED_BEFORE_KILL = 1_000;

    @TempDir
    Path dir;

    @BeforeAll
    static void container() {
        assumeTrue(MinioFixture.dockerAvailable(), "no Docker daemon: this is a chaos suite");
    }

    /** What one run saw. */
    private record Run(Trigger trigger, Set<String> acked, Map<String, Integer> found) {

        /** ⚠️ Records in the bucket that no producer was ever told were durable. */
        boolean landedBetweenAPutAndItsAck() {
            return found.keySet().stream().anyMatch(id -> !acked.contains(id));
        }

        long duplicates() {
            return found.values().stream().filter(n -> n > 1).count();
        }
    }

    @Test
    void aSIGKILLMidFlushLOSESNoAckedRecord() throws Exception {
        List<Run> runs = new ArrayList<>();
        for (int i = 0; i < RUNS.size(); i++) {
            runs.add(oneRun(i, RUNS.get(i)));
        }

        long inside = runs.stream().filter(r -> r.trigger() == Trigger.AT_THE_PUT)
                .filter(Run::landedBetweenAPutAndItsAck).count();
        System.out.println("M8.9 kills that landed between a PUT and its ack: " + inside
                + " of " + RUNS.stream().filter(t -> t == Trigger.AT_THE_PUT).count()
                + "; per run " + runs.stream().map(r -> r.trigger() + "(acked "
                        + r.acked().size() + ", in window " + r.landedBetweenAPutAndItsAck()
                        + ", duplicated " + r.duplicates() + ")").toList());
        for (Run r : runs) {
            assertThat(r.acked().size()).as("the premise: >= 1,000 acked before the kill")
                    .isGreaterThanOrEqualTo(ACKED_BEFORE_KILL);
            Set<String> lost = new HashSet<>(r.acked());
            lost.removeAll(r.found().keySet());
            assertThat(lost).as("⚠️ RPO 0: EVERY ACKED RECORD IS IN THE BUCKET AFTER A KILL %s",
                    r.trigger()).isEmpty();
            assertThat(r.found().values()).as("⚠️ AT-LEAST-ONCE, NOT MANY-TIMES")
                    .allMatch(n -> n <= 2);
        }
        assertThat(inside)
                .as("⚠️ AT LEAST ONE KILL LANDED INSIDE A FLUSH, measured at the store: records "
                        + "in the bucket that were never acked. A suite whose kills all landed "
                        + "between flushes shows RPO 0 at moments when nothing was at risk")
                .isGreaterThanOrEqualTo(1);
    }

    private Run oneRun(int run, Trigger trigger) throws Exception {
        Path runDir = Files.createDirectories(dir.resolve("run-" + run));
        try (ChaosBucket bucket = ChaosBucket.create()) {
            Map<String, String> settings = new HashMap<>(bucket.nodeSettings());
            // ⚠️ SHORT, so flushes follow each other closely.
            settings.put("ingest.interval-floor", "PT0.05S");
            NodeProcess node = NodeProcess.start(runDir, "pod1", settings);
            Set<String> acked = ConcurrentHashMap.newKeySet();
            AtomicBoolean killed = new AtomicBoolean();
            CountDownLatch done = new CountDownLatch(PRODUCERS);
            try {
                node.registerLogs(UUID.randomUUID());
                for (int t = 0; t < PRODUCERS; t++) {
                    int producer = t;
                    Thread.ofVirtual().start(() -> {
                        try {
                            for (int seq = 0; !killed.get(); seq++) {
                                List<String> ids = ids(run, producer, String.valueOf(seq));
                                int status;
                                try {
                                    status = node.write(ids);
                                } catch (RuntimeException connectionGone) {
                                    return;
                                }
                                if (status != 202) {
                                    continue;
                                }
                                acked.addAll(ids);
                                if (acked.size() >= ACKED_BEFORE_KILL
                                        && killed.compareAndSet(false, true)) {
                                    if (trigger == Trigger.AT_THE_ACK) {
                                        node.killNow();
                                    } else {
                                        killAtTheNextPut(node, bucket, acked,
                                                ids(run, producer, "next"));
                                    }
                                }
                            }
                        } catch (Exception failed) {
                            throw new IllegalStateException(failed);
                        } finally {
                            done.countDown();
                        }
                    });
                }
                assertThat(done.await(240, TimeUnit.SECONDS))
                        .as("the producers never reached the kill").isTrue();
            } finally {
                node.close();
            }
            return new Run(trigger, Set.copyOf(acked), readAll(bucket));
        }
    }

    private static List<String> ids(int run, int producer, String seq) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < BATCH; i++) {
            ids.add("r" + run + "-p" + producer + "-" + seq + "-" + i);
        }
        return ids;
    }

    /**
     * Sends one more batch, and kills the node the moment a new segment is in
     * the bucket.
     *
     * <p>⚠️ **THE SIGNAL IS THE STORE'S, NOT A CLOCK'S.** The segment count is
     * read before the batch goes out, and the kill follows the first listing
     * that shows more. Bounded, so a node that never flushes fails the row
     * rather than hanging it.
     */
    private static void killAtTheNextPut(NodeProcess node, ChaosBucket bucket, Set<String> acked,
            List<String> next) throws Exception {
        int before = bucket.segments().size();
        Thread.ofVirtual().start(() -> {
            try {
                if (node.write(next) == 202) {
                    acked.addAll(next);
                }
            } catch (RuntimeException gone) {
                // the kill landed first, which is the point
            }
        });
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (bucket.segments().size() == before) {
            if (System.nanoTime() > deadline) {
                node.killNow();
                throw new AssertionError("no new segment reached the bucket within 10 s");
            }
        }
        node.killNow();
    }

    /** Every id in every segment in the bucket, with how many times it appears. */
    private static Map<String, Integer> readAll(ChaosBucket bucket) throws Exception {
        Map<String, Integer> found = new HashMap<>();
        for (String key : bucket.segments()) {
            byte[] bytes;
            try (var in = bucket.observer().get(key)) {
                bytes = in.readAllBytes();
            }
            SegmentReader reader = SegmentReader.open(bytes);
            for (RunEntry entry : reader.directory()) {
                for (SegmentRecord record : reader.read(entry)) {
                    found.merge(record.id(), 1, Integer::sum);
                }
            }
        }
        return found;
    }
}
