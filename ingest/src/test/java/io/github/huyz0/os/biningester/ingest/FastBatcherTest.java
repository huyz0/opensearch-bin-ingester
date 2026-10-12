// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A fast index's appends batched into one COMMIT of many runs (M13.29d;
 * ADR-0081 §2): the appends that arrive while a COMMIT is in flight go
 * together in the next, each stream's records as ONE run, and each producer
 * answered at its own slice of the offsets assigned -- so COMMITs scale with
 * the leader's answers, never appends.
 */
@Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FastBatcherTest {

    private static final RunKey S = new RunKey(new UUID(1, 1), 0);
    private static final RunKey T = new RunKey(new UUID(2, 2), 0);

    /** Assigns each stream's run from 100 on; a COMMIT whose gate is shut blocks. */
    private static final class Leader implements FastBatcher.Batch {
        final List<List<FastWriteFrame.CommitRun>> commits = new ArrayList<>();
        /** By COMMIT index: counted down when it arrives, and awaited before it answers. */
        final List<CountDownLatch> arrived = List.of(new CountDownLatch(1),
                new CountDownLatch(1), new CountDownLatch(1));
        final List<CountDownLatch> gates = new java.util.concurrent.CopyOnWriteArrayList<>(
                List.of(new CountDownLatch(1), new CountDownLatch(0), new CountDownLatch(0)));
        final Map<RunKey, Long> next = new HashMap<>();
        volatile boolean notFast;
        volatile Throwable failing;
        /** The one COMMIT that fails, by index; every one when negative. */
        volatile int failingAt = -1;
        volatile RunKey dropped;

        @Override
        public Optional<Map<RunKey, Long>> write(List<FastWriteFrame.CommitRun> runs)
                throws IOException {
            int index;
            synchronized (this) {
                index = commits.size();
                commits.add(List.copyOf(runs));
            }
            if (index < arrived.size()) {
                arrived.get(index).countDown();
                await(gates.get(index));
            }
            boolean fails = failingAt < 0 || failingAt == index;
            if (fails && failing instanceof IOException io) {
                throw io;
            }
            if (fails && failing instanceof Error e) {
                throw e;
            }
            if (notFast) {
                return Optional.empty();
            }
            Map<RunKey, Long> firsts = new HashMap<>();
            synchronized (this) {
                for (FastWriteFrame.CommitRun run : runs) {
                    long at = next.getOrDefault(run.stream(), 100L);
                    if (!run.stream().equals(dropped)) {
                        firsts.put(run.stream(), at);
                    }
                    next.put(run.stream(), at + run.records().size());
                }
            }
            return Optional.of(firsts);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    /** What an append threw, carried whole: a future unwraps a CompletionException. */
    private static final class Thrown extends RuntimeException {
        Thrown(Throwable thrown) {
            super(thrown);
        }
    }

    /** An append on a thread of its own, its outcome -- or exactly what it threw. */
    private static CompletableFuture<Optional<AppendResult>> append(FastBatcher batcher,
            RunKey stream, int n) {
        CompletableFuture<Optional<AppendResult>> outcome = new CompletableFuture<>();
        Thread.ofVirtual().start(() -> {
            try {
                outcome.complete(batcher.append(stream, IngestTestSupport.docs(n)));
            } catch (Throwable thrown) {
                outcome.completeExceptionally(new Thrown(thrown));
            }
        });
        return outcome;
    }

    /** Exactly what the append behind {@code outcome} threw. */
    private static Throwable thrownBy(CompletableFuture<Optional<AppendResult>> outcome)
            throws Exception {
        try {
            outcome.get(10, TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException failed) {
            return failed.getCause().getCause();
        }
        throw new AssertionError("the append did not fail");
    }

    /** Waits until {@code n} appends are queued behind the COMMIT in flight. */
    private static void awaitQueued(FastBatcher batcher, int n) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (batcher.queued() < n) {
            assertThat(System.nanoTime()).as("only " + batcher.queued() + " queued")
                    .isLessThan(deadline);
            Thread.onSpinWait();
        }
    }

    @Test
    void appendsARRIVINGDuringACommitGoTogetherInTheNextAsOneRunPerStream()
            throws Exception {
        Leader leader = new Leader();
        FastBatcher batcher = new FastBatcher(leader, 1_000);
        CompletableFuture<Optional<AppendResult>> first = append(batcher, S, 1);
        await(leader.arrived.get(0));
        CompletableFuture<Optional<AppendResult>> a = append(batcher, S, 2);
        awaitQueued(batcher, 1);
        CompletableFuture<Optional<AppendResult>> b = append(batcher, T, 3);
        awaitQueued(batcher, 2);
        CompletableFuture<Optional<AppendResult>> c = append(batcher, S, 4);
        awaitQueued(batcher, 3);

        leader.gates.get(0).countDown();

        assertThat(first.get(10, TimeUnit.SECONDS)).hasValue(new AppendResult(1, 100, 100));
        assertThat(a.get(10, TimeUnit.SECONDS)).hasValue(new AppendResult(2, 101, 102));
        assertThat(c.get(10, TimeUnit.SECONDS)).as("its slice after a's")
                .hasValue(new AppendResult(4, 103, 106));
        assertThat(b.get(10, TimeUnit.SECONDS)).hasValue(new AppendResult(3, 100, 102));
        assertThat(leader.commits).hasSize(2);
        assertThat(leader.commits.get(1)).extracting(FastWriteFrame.CommitRun::stream)
                .as("one run per stream").containsExactly(S, T);
    }

    @Test
    void aSENDERIsAnsweredWithoutWaitingForTheCommitsQueuedBehindIt() throws Exception {
        // ⚠️ its review round 1, P2: one COMMIT, then the turn is handed on.
        Leader leader = new Leader();
        CountDownLatch second = new CountDownLatch(1);
        leader.gates.set(1, second);
        FastBatcher batcher = new FastBatcher(leader, 1_000);
        CompletableFuture<Optional<AppendResult>> first = append(batcher, S, 1);
        await(leader.arrived.get(0));
        CompletableFuture<Optional<AppendResult>> a = append(batcher, S, 1);
        awaitQueued(batcher, 1);

        leader.gates.get(0).countDown();

        assertThat(first.get(10, TimeUnit.SECONDS)).as("while a's COMMIT is still in flight")
                .hasValue(new AppendResult(1, 100, 100));
        await(leader.arrived.get(1));
        assertThat(a).isNotDone();
        second.countDown();
        assertThat(a.get(10, TimeUnit.SECONDS)).hasValue(new AppendResult(1, 101, 101));
    }

    @Test
    void aBATCHERSendsAgainAfterItsQueueEmptied() throws Exception {
        Leader leader = new Leader();
        leader.gates.get(0).countDown();
        FastBatcher batcher = new FastBatcher(leader, 100);

        batcher.append(S, IngestTestSupport.docs(1));

        assertThat(append(batcher, S, 2).get(10, TimeUnit.SECONDS))
                .hasValue(new AppendResult(2, 101, 102));
    }

    @Test
    void aSTREAMsRunNeverPassesTheBound() throws Exception {
        Leader leader = new Leader();
        FastBatcher batcher = new FastBatcher(leader, 5);
        CompletableFuture<Optional<AppendResult>> first = append(batcher, S, 1);
        await(leader.arrived.get(0));
        CompletableFuture<Optional<AppendResult>> a = append(batcher, S, 3);
        awaitQueued(batcher, 1);
        CompletableFuture<Optional<AppendResult>> b = append(batcher, S, 3);
        awaitQueued(batcher, 2);

        leader.gates.get(0).countDown();

        first.get(10, TimeUnit.SECONDS);
        a.get(10, TimeUnit.SECONDS);
        b.get(10, TimeUnit.SECONDS);
        assertThat(leader.commits).as("3 + 3 > B = 5: two COMMITs after the first")
                .hasSize(3);
    }

    @Test
    void aRUNOfExactlyTheBoundIsOneCommit() throws Exception {
        Leader leader = new Leader();
        FastBatcher batcher = new FastBatcher(leader, 5);
        CompletableFuture<Optional<AppendResult>> first = append(batcher, T, 1);
        await(leader.arrived.get(0));
        CompletableFuture<Optional<AppendResult>> a = append(batcher, S, 2);
        awaitQueued(batcher, 1);
        CompletableFuture<Optional<AppendResult>> b = append(batcher, S, 3);
        awaitQueued(batcher, 2);

        leader.gates.get(0).countDown();
        first.get(10, TimeUnit.SECONDS);
        a.get(10, TimeUnit.SECONDS);
        b.get(10, TimeUnit.SECONDS);

        assertThat(leader.commits).as("2 + 3 = B: together").hasSize(2);
    }

    @Test
    void aSTREAMHeldBackTakesNothingMoreSoItsAppendsStayInOrder() throws Exception {
        Leader leader = new Leader();
        FastBatcher batcher = new FastBatcher(leader, 5);
        CompletableFuture<Optional<AppendResult>> first = append(batcher, T, 1);
        await(leader.arrived.get(0));
        CompletableFuture<Optional<AppendResult>> a = append(batcher, S, 3);
        awaitQueued(batcher, 1);
        CompletableFuture<Optional<AppendResult>> b = append(batcher, S, 3);
        awaitQueued(batcher, 2);
        CompletableFuture<Optional<AppendResult>> c = append(batcher, S, 1);
        awaitQueued(batcher, 3);

        leader.gates.get(0).countDown();
        first.get(10, TimeUnit.SECONDS);

        long bAt = b.get(10, TimeUnit.SECONDS).orElseThrow().firstOffset();
        long cAt = c.get(10, TimeUnit.SECONDS).orElseThrow().firstOffset();
        assertThat(a.get(10, TimeUnit.SECONDS).orElseThrow().firstOffset()).isEqualTo(100);
        assertThat(cAt).as("c arrived after b, so is assigned after it").isGreaterThan(bAt);
    }

    @Test
    void anAPPENDOverTheBoundGoesTheDefaultWayAloneAndOneOfItIsSent() throws Exception {
        Leader leader = new Leader();
        leader.gates.get(0).countDown();
        FastBatcher batcher = new FastBatcher(leader, 5);

        assertThat(batcher.append(S, IngestTestSupport.docs(6))).isEmpty();
        assertThat(leader.commits).isEmpty();
        assertThat(batcher.append(S, IngestTestSupport.docs(5))).as("exactly B is sent")
                .hasValue(new AppendResult(5, 100, 104));
    }

    @Test
    void notFASTSendsEveryAppendOfTheBatchTheDefaultWay() throws Exception {
        Leader leader = new Leader();
        leader.notFast = true;
        leader.gates.get(0).countDown();

        assertThat(new FastBatcher(leader, 100).append(S, IngestTestSupport.docs(2))).isEmpty();
    }

    @Test
    void aFAILEDCommitRefusesEveryAppendInItWithAnIOException() throws Exception {
        Leader leader = new Leader();
        leader.failing = new IOException("the leader did not answer");
        FastBatcher batcher = new FastBatcher(leader, 100);
        CompletableFuture<Optional<AppendResult>> first = append(batcher, S, 1);
        await(leader.arrived.get(0));
        CompletableFuture<Optional<AppendResult>> a = append(batcher, T, 1);
        awaitQueued(batcher, 1);

        leader.gates.get(0).countDown();

        assertThat(thrownBy(first)).isExactlyInstanceOf(IOException.class);
        assertThat(thrownBy(a)).isExactlyInstanceOf(IOException.class);
    }

    @Test
    void anAPPENDFailedByAnotherAppendersCommitSeesAnIOException() throws Exception {
        // ⚠️ its review round 2, P4 and T6: b rides a's COMMIT, which fails.
        Leader leader = new Leader();
        leader.failing = new IOException("the leader did not answer");
        leader.failingAt = 1;
        FastBatcher batcher = new FastBatcher(leader, 100);
        CompletableFuture<Optional<AppendResult>> first = append(batcher, S, 1);
        await(leader.arrived.get(0));
        CompletableFuture<Optional<AppendResult>> a = append(batcher, S, 1);
        awaitQueued(batcher, 1);
        CompletableFuture<Optional<AppendResult>> b = append(batcher, T, 1);
        awaitQueued(batcher, 2);

        leader.gates.get(0).countDown();

        assertThat(first.get(10, TimeUnit.SECONDS)).isPresent();
        assertThat(thrownBy(a)).isExactlyInstanceOf(IOException.class);
        assertThat(thrownBy(b)).as("failed by a's COMMIT, still an IOException")
                .isExactlyInstanceOf(IOException.class);
        assertThat(leader.commits).as("a and b in one COMMIT").hasSize(2);
    }

    @Test
    void anERRORInACommitLeavesTheBatcherSending() throws Exception {
        // ⚠️ its review round 1, P1: the turn passes whatever happens.
        Leader leader = new Leader();
        leader.failing = new StackOverflowError("a step recursed");
        leader.gates.get(0).countDown();
        FastBatcher batcher = new FastBatcher(leader, 100);
        assertThatThrownBy(() -> batcher.append(S, IngestTestSupport.docs(1)))
                .as("the Error itself, rethrown").isInstanceOf(StackOverflowError.class);
        leader.failing = null;

        assertThat(append(batcher, S, 1).get(10, TimeUnit.SECONDS)).isPresent();
    }

    @Test
    void anANSWERMissingAStreamsRunRefusesItsAppends() throws Exception {
        // ⚠️ its review round 1, T2: never the default way -- it may be exposed.
        Leader leader = new Leader();
        leader.dropped = S;
        leader.gates.get(0).countDown();

        assertThatThrownBy(() -> new FastBatcher(leader, 100).append(S,
                IngestTestSupport.docs(1))).isInstanceOf(IOException.class);
    }

    @Test
    void eachRUNCarriesItsAppendsRecordsInOrder() throws Exception {
        Leader leader = new Leader();
        FastBatcher batcher = new FastBatcher(leader, 1_000);
        CompletableFuture<Optional<AppendResult>> first = append(batcher, T, 1);
        await(leader.arrived.get(0));
        CompletableFuture<Optional<AppendResult>> a = append(batcher, S, 1);
        awaitQueued(batcher, 1);
        CompletableFuture<Optional<AppendResult>> b = append(batcher, S, 2);
        awaitQueued(batcher, 2);

        leader.gates.get(0).countDown();
        first.get(10, TimeUnit.SECONDS);
        a.get(10, TimeUnit.SECONDS);
        b.get(10, TimeUnit.SECONDS);

        List<SegmentRecord> run = leader.commits.get(1).get(0).records();
        assertThat(run).extracting(SegmentRecord::id).containsExactly("doc-0", "doc-0", "doc-1");
    }
}
