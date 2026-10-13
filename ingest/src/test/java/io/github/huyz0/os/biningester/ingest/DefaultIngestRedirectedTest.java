// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitAnswer;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * A flush uses the leader's commit answer (M13.34l; ADR-0081 §8, ADR-0082 §7):
 * each append acked from whichever delta committed its stream -- a paused
 * run's in a later one -- and a redirected run's appends re-sent through the
 * fast path with exactly their records, acked where those are written; one
 * push per delta.
 */
class DefaultIngestRedirectedTest {

    private static final RunKey ONE = new RunKey(IngestTestSupport.LOGS, 1);
    private static final RunKey TWO = new RunKey(IngestTestSupport.LOGS, 2);
    private static final RunKey THREE = new RunKey(IngestTestSupport.LOGS, 3);

    /** A leader whose answer commits ONE at 50, THREE at 70 in a later delta, redirects TWO. */
    private static final class Answering implements Sequencer {
        @Override
        public CommitDelta commitAll(List<CommitRequest> requests) {
            throw new AssertionError("a flush asks for the answer");
        }

        @Override
        public CommitAnswer commitAnswering(CommitRequest request) {
            SegmentCommit.Attribution a = new SegmentCommit.Attribution(request.podId(),
                    request.incarnationId(), request.flushSeq());
            return new CommitAnswer(List.of(
                    new CommitDelta(4, List.of(new SegmentCommit(request.segmentKey(),
                            List.of(new RunCommit(ONE, request.recordCounts().get(ONE), 50)), a))),
                    new CommitDelta(5, List.of(new SegmentCommit(request.segmentKey(),
                            List.of(new RunCommit(THREE, request.recordCounts().get(THREE), 70)),
                            a)))), List.of(TWO));
        }

        @Override
        public void close() {
        }
    }

    /** A leader redirecting every run of every request. */
    private static final class RedirectingAll implements Sequencer {
        @Override
        public CommitDelta commitAll(List<CommitRequest> requests) {
            throw new AssertionError("a flush asks for the answer");
        }

        @Override
        public CommitAnswer commitAnswering(CommitRequest request) {
            return new CommitAnswer(List.of(),
                    new ArrayList<>(new java.util.TreeSet<>(request.recordCounts().keySet())));
        }

        @Override
        public void close() {
        }
    }

    /** A fast path assigning offsets from 10 in the order it is asked, or NOT_FAST. */
    private static final class Fast implements FastPath {
        final List<List<SegmentRecord>> got = new CopyOnWriteArrayList<>();
        long next = 10;
        boolean notFast;

        @Override
        public synchronized Optional<AppendResult> append(RunKey stream,
                List<SegmentRecord> records) {
            got.add(List.copyOf(records));
            if (notFast) {
                return Optional.empty();
            }
            AppendResult assigned = new AppendResult(records.size(), next,
                    next + records.size() - 1);
            next += records.size();
            return Optional.of(assigned);
        }
    }

    private static SegmentRecord doc(String id) {
        return new SegmentRecord(id, io.github.huyz0.os.biningester.format.OpType.INDEX,
                java.util.OptionalLong.of(1), new byte[] {1});
    }

    private static CompletableFuture<AppendResult> append(DefaultIngest ingest, int partition,
            List<SegmentRecord> records) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return ingest.append(IngestTestSupport.PRINCIPAL, "logs", partition, (byte) 0,
                        records::forEach, () -> { });
            } catch (Exception failed) {
                throw new java.util.concurrent.CompletionException(failed);
            }
        });
    }

    @Test
    void eachAPPENDIsAckedFromItsDeltaAndARedirectedOneThroughTheFastPath() throws Exception {
        Fast fast = new Fast();
        try (DefaultIngest ingest = IngestTestSupport.ingestWithSequencer(
                new CountingBinStore(new MemoryBinStore()), new Answering())) {
            ingest.routeFast(index -> false, fast);
            List<SegmentRecord> two = IngestTestSupport.docs(2);
            CompletableFuture<AppendResult> one = IngestTestSupport.appendAsync(ingest,
                    "logs", 1, 3);
            CompletableFuture<AppendResult> redirected = CompletableFuture.supplyAsync(() -> {
                try {
                    return ingest.append(IngestTestSupport.PRINCIPAL, "logs", 2, (byte) 0,
                            two::forEach, () -> { });
                } catch (Exception failed) {
                    throw new java.util.concurrent.CompletionException(failed);
                }
            });
            CompletableFuture<AppendResult> three = IngestTestSupport.appendAsync(ingest,
                    "logs", 3, 1);
            IngestTestSupport.awaitPending(ingest, 3);
            ingest.flushNow();

            assertThat(one.get(10, TimeUnit.SECONDS)).isEqualTo(new AppendResult(3, 50, 52));
            assertThat(three.get(10, TimeUnit.SECONDS)).as("from the later delta")
                    .isEqualTo(new AppendResult(1, 70, 70));
            assertThat(redirected.get(10, TimeUnit.SECONDS)).as("where the fast path wrote it")
                    .isEqualTo(new AppendResult(2, 10, 11));
            assertThat(fast.got).singleElement().satisfies(records -> assertThat(records)
                    .usingRecursiveFieldByFieldElementComparator()
                    .containsExactlyElementsOf(two));
        }
    }

    @Test
    void twoAPPENDSOfARedirectedStreamAreResentInOrderEachWithItsOwnRecords()
            throws Exception {
        // ⚠️ its review round 1, R1 and T1: the second append's records start
        // past the first's in the run, and its fast write follows the first's.
        Fast fast = new Fast();
        try (DefaultIngest ingest = IngestTestSupport.ingestWithSequencer(
                new CountingBinStore(new MemoryBinStore()), new RedirectingAll())) {
            ingest.routeFast(index -> false, fast);
            CompletableFuture<AppendResult> first = append(ingest, 2,
                    List.of(doc("a"), doc("b")));
            IngestTestSupport.awaitPending(ingest, 1);
            CompletableFuture<AppendResult> second = append(ingest, 2, List.of(doc("c")));
            IngestTestSupport.awaitPending(ingest, 2);
            ingest.flushNow();

            assertThat(first.get(10, TimeUnit.SECONDS)).isEqualTo(new AppendResult(2, 10, 11));
            assertThat(second.get(10, TimeUnit.SECONDS)).isEqualTo(new AppendResult(1, 12, 12));
            assertThat(fast.got).extracting(records -> records.stream()
                    .map(SegmentRecord::id).toList())
                    .containsExactly(List.of("a", "b"), List.of("c"));
        }
    }

    @Test
    void aREDIRECTEDAppendTheFastPathRefusesIsFailedNeverAcked() throws Exception {
        // ⚠️ its review round 1, T2.
        Fast fast = new Fast();
        fast.notFast = true;
        try (DefaultIngest ingest = IngestTestSupport.ingestWithSequencer(
                new CountingBinStore(new MemoryBinStore()), new RedirectingAll())) {
            ingest.routeFast(index -> false, fast);
            CompletableFuture<AppendResult> refused = append(ingest, 2, List.of(doc("a")));
            IngestTestSupport.awaitPending(ingest, 1);
            ingest.flushNow();

            assertThatThrownBy(() -> refused.get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(java.io.IOException.class)
                    .hasStackTraceContaining("NOT_FAST");
        }
    }

    @Test
    void aCOMMITTEDAppendIsAckedWhileARedirectedOnesQuorumIsPending() throws Exception {
        // ⚠️ its review round 1, P1/T1: never behind a redirect's quorum; and the
        // redirected append is answered before the flush returns.
        java.util.concurrent.CountDownLatch quorum = new java.util.concurrent.CountDownLatch(1);
        FastPath slow = (stream, records) -> {
            try {
                quorum.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                throw new java.io.IOException(interrupted);
            }
            return Optional.of(new AppendResult(records.size(), 10, 10 + records.size() - 1));
        };
        try (DefaultIngest ingest = IngestTestSupport.ingestWithSequencer(
                new CountingBinStore(new MemoryBinStore()), new Answering())) {
            ingest.routeFast(index -> false, slow);
            CompletableFuture<AppendResult> one = IngestTestSupport.appendAsync(ingest,
                    "logs", 1, 3);
            CompletableFuture<AppendResult> two = append(ingest, 2, List.of(doc("a")));
            CompletableFuture<AppendResult> three = IngestTestSupport.appendAsync(ingest,
                    "logs", 3, 1);
            IngestTestSupport.awaitPending(ingest, 3);
            CompletableFuture<Void> flushed = IngestTestSupport.flushAsync(ingest);

            assertThat(one.get(10, TimeUnit.SECONDS)).as("acked while the quorum waits")
                    .isEqualTo(new AppendResult(3, 50, 52));
            assertThat(two).isNotDone();
            assertThat(flushed).as("the flush waits for it").isNotDone();
            quorum.countDown();
            flushed.get(10, TimeUnit.SECONDS);

            assertThat(two).isCompletedWithValue(new AppendResult(1, 10, 10));
            assertThat(three.get(10, TimeUnit.SECONDS)).isEqualTo(new AppendResult(1, 70, 70));
        }
    }

    @Test
    void aREDIRECTEDAppendWhoseFastWriteFailsIsFailedWithIt() throws Exception {
        // ⚠️ its review round 1, T2.
        FastPath failing = (stream, records) -> {
            throw new java.io.IOException("the leader is unreachable");
        };
        try (DefaultIngest ingest = IngestTestSupport.ingestWithSequencer(
                new CountingBinStore(new MemoryBinStore()), new RedirectingAll())) {
            ingest.routeFast(index -> false, failing);
            CompletableFuture<AppendResult> refused = append(ingest, 2, List.of(doc("a")));
            IngestTestSupport.awaitPending(ingest, 1);
            ingest.flushNow();

            assertThatThrownBy(() -> refused.get(10, TimeUnit.SECONDS))
                    .hasRootCauseMessage("the leader is unreachable");
        }
    }

    @Test
    void eachDELTAIsPushedAndTheSegmentSignalledDurableOnce() throws Exception {
        // ⚠️ its review rounds 1 and 2, T3/T2.
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> pushed = new CopyOnWriteArrayList<>();
        List<String> durable = new CopyOnWriteArrayList<>();
        try (var subOne = hub.subscribe(ONE, SubscriptionHub.assembling(pushed::add));
                var subThree = hub.subscribe(THREE, SubscriptionHub.assembling(pushed::add));
                DefaultIngest ingest = new DefaultIngest(
                        IngestTestSupport.pinnedIntervalConfig(java.time.Duration.ofDays(1),
                                8L << 20), new CountingBinStore(new MemoryBinStore()),
                        IngestTestSupport.PREFIX, "pod1", new Answering(), hub,
                        java.time.Clock.systemUTC(), index -> IngestTestSupport.LOGS,
                        durable::add, new io.github.huyz0.os.biningester.binstore
                                .IndexCostLedger())) {
            ingest.routeFast(index -> false, new Fast());
            CompletableFuture<AppendResult> one = IngestTestSupport.appendAsync(ingest,
                    "logs", 1, 3);
            CompletableFuture<AppendResult> two = IngestTestSupport.appendAsync(ingest,
                    "logs", 2, 2);
            CompletableFuture<AppendResult> three = IngestTestSupport.appendAsync(ingest,
                    "logs", 3, 1);
            IngestTestSupport.awaitPending(ingest, 3);
            ingest.flushNow();
            CompletableFuture.allOf(one, two, three).get(10, TimeUnit.SECONDS);

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (pushed.size() < 2) {
                assertThat(System.nanoTime()).as("both pushes").isLessThan(deadline);
                Thread.onSpinWait();
            }
            assertThat(pushed).extracting(SubscriptionHub.Push::key,
                    SubscriptionHub.Push::chainSequence).containsExactlyInAnyOrder(
                            org.assertj.core.groups.Tuple.tuple(ONE, 4L),
                            org.assertj.core.groups.Tuple.tuple(THREE, 5L));
            assertThat(durable).as("one segment, signalled once").hasSize(1);
        }
    }

    @Test
    void anERRORInOneRedirectedAppendFailsItAnswersTheRestAndKeepsTheFlusherAlive()
            throws Exception {
        // ⚠️ its review round 2, P4/T3: the re-send is a settlement on the flush
        // worker; an Error escaping it ended the worker and stranded everyone.
        Fast fast = new Fast();
        FastPath breaking = (stream, records) -> {
            if (records.get(0).id().equals("boom")) {
                throw new AssertionError("a fast path broke");
            }
            return fast.append(stream, records);
        };
        try (DefaultIngest ingest = IngestTestSupport.ingestWithSequencer(
                new CountingBinStore(new MemoryBinStore()), new RedirectingAll())) {
            ingest.routeFast(index -> false, breaking);
            CompletableFuture<AppendResult> broken = append(ingest, 2, List.of(doc("boom")));
            IngestTestSupport.awaitPending(ingest, 1);
            CompletableFuture<AppendResult> after = append(ingest, 2, List.of(doc("b")));
            IngestTestSupport.awaitPending(ingest, 2);
            ingest.flushNow();

            assertThatThrownBy(() -> broken.get(10, TimeUnit.SECONDS))
                    .hasRootCauseMessage("a fast path broke");
            assertThat(after.get(10, TimeUnit.SECONDS)).as("the rest answered")
                    .isEqualTo(new AppendResult(1, 10, 10));
            CompletableFuture<AppendResult> next = append(ingest, 2, List.of(doc("c")));
            IngestTestSupport.awaitPending(ingest, 1);
            ingest.flushNow();
            assertThat(next.get(10, TimeUnit.SECONDS)).as("the flusher still flushes")
                    .isEqualTo(new AppendResult(1, 11, 11));
        }
    }
}
