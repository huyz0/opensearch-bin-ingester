// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.huyz0.os.biningester.binstore.backend.S3Fixture;
import io.github.huyz0.os.biningester.format.RunEntry;
import io.github.huyz0.os.biningester.format.SegmentReader;
import io.github.huyz0.os.biningester.format.SegmentRecord;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a chaos suite");
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
            CountDownLatch putSenderReady = new CountDownLatch(1);
            CountDownLatch releasePutSender = new CountDownLatch(1);
            AtomicBoolean snapshotStarted = new AtomicBoolean();
            CountDownLatch joinObserved = new CountDownLatch(1);
            AtomicReference<Throwable> producerFailure = new AtomicReference<>();
            List<Thread> putSenders = new CopyOnWriteArrayList<>();
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
                                        killAtTheNextPut(node, bucket, acked, putSenders,
                                                putSenderReady, releasePutSender,
                                                ids(run, producer, "next"));
                                    }
                                }
                            }
                        } catch (Exception | AssertionError failed) {
                            producerFailure.compareAndSet(null, failed);
                        } finally {
                            done.countDown();
                        }
                    });
                }
                assertThat(done.await(240, TimeUnit.SECONDS))
                        .as("the producers never reached the kill").isTrue();
                assertThat(producerFailure.get())
                        .as("producer and trigger failures reach the test thread").isNull();
                if (!putSenders.isEmpty()) {
                    assertThat(putSenderReady.await(30, TimeUnit.SECONDS))
                            .as("the post-PUT sender never reached its response")
                            .isTrue();
                    assertThat(putSenders.getFirst().isAlive())
                            .as("the post-PUT sender remains in flight after producers finish")
                            .isTrue();
                    Thread joiningThread = Thread.currentThread();
                    CountDownLatch releaseObserverDone = new CountDownLatch(1);
                    Thread releaseOnJoin = Thread.ofVirtual().start(() -> {
                        try {
                            while (!snapshotStarted.get()) {
                                if (joiningThread.getState() == Thread.State.TIMED_WAITING) {
                                    joinObserved.countDown();
                                    releasePutSender.countDown();
                                    return;
                                }
                                Thread.yield();
                            }
                            releasePutSender.countDown();
                        } finally {
                            releaseObserverDone.countDown();
                        }
                    });
                    putSenders.getFirst().join(TimeUnit.SECONDS.toMillis(30));
                    assertThat(putSenders.getFirst().isAlive())
                            .as("the post-PUT sender completes before the ack snapshot")
                            .isFalse();
                    snapshotStarted.set(true);
                    assertThat(releaseObserverDone.await(30, TimeUnit.SECONDS))
                            .as("the sender release observer terminates").isTrue();
                    assertThat(releaseOnJoin.isAlive())
                            .as("the sender release observer terminates").isFalse();
                    assertThat(joinObserved.getCount())
                            .as("the post-PUT sender was released only after the ack snapshot thread entered join")
                            .isZero();
                }
                releasePutSender.countDown();
                for (Thread sender : putSenders) {
                    assertThat(sender.isAlive())
                            .as("the post-PUT sender completed before the ack snapshot")
                            .isFalse();
                }
            } finally {
                snapshotStarted.set(true);
                releasePutSender.countDown();
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
            List<Thread> senders, CountDownLatch senderReady, CountDownLatch releaseSender,
            List<String> next) throws Exception {
        String dataPrefix = ChaosBucket.PREFIX + "/data/";
        Set<String> before = Set.copyOf(bucket.keysOnePage(dataPrefix));
        Thread sender = Thread.ofVirtual().start(() -> {
            boolean accepted = false;
            try {
                accepted = node.write(next) == 202;
            } catch (RuntimeException gone) {
                // the kill landed first, which is the point
            } finally {
                senderReady.countDown();
            }
            try {
                if (!releaseSender.await(30, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("post-PUT sender release timed out");
                }
                if (accepted) {
                    acked.addAll(next);
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        });
        senders.add(sender);
        AtomicReference<String> visibleSegment = new AtomicReference<>();
        await().pollInterval(Duration.ofMillis(1)).atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    List<String> segments = bucket.keysOnePage(dataPrefix);
                    List<String> added = segments.stream().filter(key -> !before.contains(key))
                            .toList();
                    assertThat(added).as("a new segment reaches the bucket").isNotEmpty();
                    visibleSegment.compareAndSet(null, added.getFirst());
                });
        node.killNow();
        long listsBefore = bucket.observerCounts().lists();
        long observationStarted = System.nanoTime();
        await().pollInterval(Duration.ofSeconds(1)).during(Duration.ofSeconds(2))
                .atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                        assertThat(bucket.keysOnePage(dataPrefix)).contains(visibleSegment.get()));
        assertObservedListRate(bucket, listsBefore, observationStarted);
    }

    private static void assertObservedListRate(ChaosBucket bucket, long listsBefore,
            long startedNanos) {
        long elapsedNanos = System.nanoTime() - startedNanos;
        long allowance = (long) Math.ceil(elapsedNanos / (double) TimeUnit.SECONDS.toNanos(1)) + 1;
        long lists = bucket.observerCounts().lists() - listsBefore;
        assertThat(lists).as("RustFS observer LIST rate over %.3f seconds", elapsedNanos / 1e9)
                .isLessThanOrEqualTo(allowance);
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
