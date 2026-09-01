// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static binjava.ingest.IngestTestSupport.appendAsync;
import static binjava.ingest.IngestTestSupport.appendOnce;
import static binjava.ingest.IngestTestSupport.awaitPending;
import static binjava.ingest.IngestTestSupport.awaitPush;
import static binjava.ingest.IngestTestSupport.docs;
import static binjava.ingest.IngestTestSupport.ingest;
import static binjava.ingest.IngestTestSupport.read;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.BinStore;
import binjava.binstore.Capabilities;
import binjava.binstore.Body;
import binjava.binstore.ListPage;
import binjava.binstore.ObjectStat;
import binjava.binstore.Version;
import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.OpType;
import binjava.format.RunKey;
import binjava.format.SegmentReader;
import binjava.format.SegmentRecord;
import binjava.security.Principal;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * ⚠️ EVERY test here has a deadline. {@code append} blocks until a flush carries
 * its records, so the natural failure mode of a regression in this class is a
 * HANG rather than a failed assertion — and a hang is strictly worse: it wedges
 * the suite, and `tdd-red.sh` cannot record a red run for a test that never
 * returns. Observed: removing the closed-guard made
 * {@code anAppendAfterCloseIsRefused} hang instead of fail.
 *
 * <p>⚠️ SEPARATE_THREAD is load-bearing. The default thread mode only checks the
 * deadline once the test METHOD RETURNS, so a test parked in
 * {@code CompletableFuture.join} is never interrupted and the deadline never
 * fires — which is exactly the case here.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DefaultIngestTest {

    @Test
    void appendReturnsOnlyAfterTheSegmentAndItsCommitDeltaAreBothReadableFromTheStore()
            throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> seen = new CopyOnWriteArrayList<>();
        try (var sub = hub.subscribe(new RunKey(IngestTestSupport.LOGS, 3), seen::add);
                DefaultIngest ingest = ingest(store, hub, IngestTestSupport.NEVER)) {
            // ⚠️ Measured from AFTER construction: recover() spends one LIST at
            // startup, by design (R2 permits a LIST off the hot path). An
            // absolute count here would silently fold that in and then break the
            // moment recovery changed.
            long base = store.counts().total();
            AppendResult result = appendOnce(ingest, "logs", 3, 10);

            assertThat(result.recordCount()).isEqualTo(10);
            // ⚠️ M2.6: 2 (segment + commit delta, M1's own count) + 2 (the
            // filter's one-time registration of this genuinely new index --
            // one stat, one CAS put; ADR-0008, "~0/s, only on index creation").
            // A SECOND append of an already-known index costs 2 again, not 4.
            assertThat(store.counts().total() - base).isEqualTo(4);

            // ⚠️ READ BOTH OBJECTS BACK, do not merely count the PUTs. Counting
            // proves neither which key was committed nor that it exists: a
            // commit naming a key that was never written passes a count
            // assertion and produces a delta pointing at nothing, which is the
            // failure the production comment claims to prevent.
            awaitPush(seen);
            String committedKey = seen.get(0).segmentKey();
            assertThat(read(store, committedKey)).isNotEmpty();
            assertThat(SegmentReader.open(read(store, committedKey)).directory())
                    .anyMatch(e -> e.key().equals(new RunKey(IngestTestSupport.LOGS, 3)) && e.recordCount() == 10);
        }
    }

    @Test
    void noAppendIsAckedBeforeItsCommitDeltaIsDurable() throws Exception {
        // ⚠️ SPEC row T4, whose named mutation is "ack before the commit delta is
        // written". Counting PUTs cannot catch it: with a synchronous store the
        // commit has always returned by the time the test looks, so completing
        // the waiters from log.nextOffset() BEFORE log.commit() passes every
        // count-and-read-back assertion. The only way to see the ordering is to
        // hold the commit open and look at the future while it is held.
        StoreFakes.GatedCommit gate = new StoreFakes.GatedCommit(new MemoryBinStore());
        CountingBinStore store = new CountingBinStore(gate);
        try (DefaultIngest ingest = ingest(store)) {
            CompletableFuture<AppendResult> append = appendAsync(ingest, "logs", 0, 4);
            awaitPending(ingest, 1);
            CompletableFuture<Void> flush = CompletableFuture.runAsync(() -> {
                try {
                    ingest.flushNow();
                } catch (IOException e) {
                    throw new CompletionException(e);
                }
            });

            assertThat(gate.entered.await(10, TimeUnit.SECONDS))
                    .as("the flush reached the commit").isTrue();
            // ⚠️ THE assertion. The segment is already durable here; the delta is
            // not. A 202 now would mean "buffered", which is the 202 that loses
            // data on a crash.
            assertThat(append)
                    .as("the producer must not be acked while the commit is still open")
                    .isNotDone();

            gate.release.countDown();
            assertThat(append.get(10, TimeUnit.SECONDS).recordCount()).isEqualTo(4);
            flush.get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void theQueuedBYTESAreBudgetedAndReleasedAfterDelivery() throws Exception {
        // ⚠️ An earlier version of this test used a single flush against an
        // 8-byte budget. With one flush `queuedPushBytes` is 0 at the check, so
        // the condition degenerated to `bytes > max` -- a PER-PUSH size cap. Both
        // accounting statements could be deleted with the suite green:
        //   - drop the DECREMENT and the budget becomes lifetime-cumulative:
        //     after 64 MiB of delivered pushes every later push is dropped
        //     forever, which in M1 is permanent silent record loss (M1.6d)
        //   - drop the INCREMENT and the queue is unbounded in bytes again,
        //     restoring the OutOfMemoryError the bound exists to prevent
        // Holding the subscriber is what makes the QUEUE budget observable: the
        // bytes are released only after hub.publish returns.
        CountingBinStore sizer = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub sizerHub = new SubscriptionHub();
        List<SubscriptionHub.Push> measured = new CopyOnWriteArrayList<>();
        int segmentBytes;
        try (var sub = sizerHub.subscribe(new RunKey(IngestTestSupport.LOGS, 0), measured::add);
                DefaultIngest probe = new DefaultIngest(
                        new IngestConfig(IngestTestSupport.NEVER, 8L << 20, "cluster-a",
                                Long.MAX_VALUE / 4),
                        sizer, IngestTestSupport.PREFIX, "pod1", sizerHub, Clock.systemUTC(),
                        index -> IngestTestSupport.LOGS)) {
            appendOnce(probe, "logs", 0, 2);
            awaitPush(measured);
            segmentBytes = measured.get(0).segment().length;
        }
        // ⚠️ Measured, not guessed: a budget that admits one of these segments
        // and not two. Hardcoding a number would break the moment the format
        // changed, and would silently stop testing anything.
        long budget = segmentBytes + segmentBytes / 2;

        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        BlockingSink sink = new BlockingSink();
        try (var sub = hub.subscribe(new RunKey(IngestTestSupport.LOGS, 0), sink);
                DefaultIngest ingest = new DefaultIngest(
                        new IngestConfig(IngestTestSupport.NEVER, 8L << 20, "cluster-a", budget),
                        store, IngestTestSupport.PREFIX, "pod1", hub, Clock.systemUTC(),
                        index -> IngestTestSupport.LOGS)) {
            appendOnce(ingest, "logs", 0, 2);
            sink.awaitEntered();

            // ⚠️ Push #1's bytes are still charged, so #2 cannot fit. Without the
            // INCREMENT it would fit and nothing would be dropped.
            appendOnce(ingest, "logs", 0, 2);
            assertThat(ingest.droppedPushes())
                    .as("the second push does not fit while the first is undelivered")
                    .isEqualTo(1);

            sink.release();
            awaitDelivered(sink, 1);

            // ⚠️ And now the budget is FREE again. Without the DECREMENT the
            // charge would never be released and this third push would drop too.
            appendOnce(ingest, "logs", 0, 2);
            assertThat(ingest.droppedPushes())
                    .as("delivery releases the budget, so a later push is admitted")
                    .isEqualTo(1);
            awaitDelivered(sink, 2);
        }
    }

    private static void awaitDelivered(BlockingSink sink, int n) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (sink.delivered() < n) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("only " + sink.delivered() + " of " + n + " delivered");
            }
            Thread.onSpinWait();
        }
    }

    @Test
    void theInlinePushCarriesTheSegmentBytesAndNotAnEmptyArray() throws Exception {
        // ⚠️ ADR-0004 inline delivery is what makes an idle consumer cost zero
        // requests. An empty byte[] here would still satisfy every assertion
        // about the delta, and the consumer would decode nothing.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> seen = new CopyOnWriteArrayList<>();
        try (var sub = hub.subscribe(new RunKey(IngestTestSupport.LOGS, 0), seen::add);
                DefaultIngest ingest = ingest(store, hub, IngestTestSupport.NEVER)) {
            appendOnce(ingest, "logs", 0, 6);
            awaitPush(seen);

            assertThat(seen.get(0).segment()).isNotEmpty();
            assertThat(SegmentReader.open(seen.get(0).segment()).directory())
                    .anyMatch(e -> e.key().equals(new RunKey(IngestTestSupport.LOGS, 0)) && e.recordCount() == 6);
        }
    }

    @Test
    void manyAppendsShareOneSegmentAndStillCostExactlyTwoRequests() throws Exception {
        // ⚠️ THE architecture (non-negotiable 6). Five separate appends across
        // TWO streams land in one flush: one segment PUT plus one commit. A
        // flush-per-append costs 10; a PUT per directory run costs 3. Both are
        // refused here, and neither could be refused by a single-append test.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        try (DefaultIngest ingest = ingest(store)) {
            long base = store.counts().total();
            List<CompletableFuture<AppendResult>> all = List.of(
                    appendAsync(ingest, "logs", 0, 10),
                    appendAsync(ingest, "logs", 0, 10),
                    appendAsync(ingest, "logs", 1, 10),
                    appendAsync(ingest, "logs", 1, 10),
                    appendAsync(ingest, "logs", 0, 10));
            awaitPending(ingest, 5);
            ingest.flushNow();
            for (var f : all) {
                f.get(10, TimeUnit.SECONDS);
            }

            // ⚠️ M2.6: 2 (segment + commit) + 2 (one genuinely new index,
            // "logs" -- both streams are partitions of the SAME index).
            assertThat(store.counts().total() - base)
                    .as("50 records, 5 appends, 2 streams, ONE index -- one segment, one "
                            + "commit, one new-index registration")
                    .isEqualTo(4);

            // ⚠️ And the OFFSETS, which this test previously threw away. With
            // two streams in one flush, a single global counter instead of a
            // per-stream one gives the logs/1 appends offsets 20 and 30 on a
            // 20-record run -- past the end of their own run, which a consumer
            // silently dedups or gaps. Cost alone could not see it.
            // ⚠️ As a SET per stream, not per future. The five appends race for
            // the lock, so submission order is NOT registration order -- an
            // earlier version asserted `all.get(1)` had offset 10 and failed
            // with 20, which would have been a flaky test rather than a wrong
            // one. What must hold is that each stream's appends carve ITS run
            // into disjoint 10-record slices.
            assertThat(List.of(all.get(0).get().firstOffset(), all.get(1).get().firstOffset(),
                            all.get(4).get().firstOffset()))
                    .as("the three logs/0 appends tile their own run")
                    .containsExactlyInAnyOrder(0L, 10L, 20L);
            assertThat(List.of(all.get(2).get().firstOffset(), all.get(3).get().firstOffset()))
                    .as("logs/1 has its own run and starts at 0, not after logs/0")
                    .containsExactlyInAnyOrder(0L, 10L);
        }
    }

    @Test
    void appendsSharingAFlushEachGetTheirOwnSliceOfTheRun() throws Exception {
        // ⚠️ The half of batching that is easy to get wrong: three appends to ONE
        // stream in one flush must carve the run into disjoint contiguous
        // ranges. Handing each the run's own firstOffset and count would trip
        // AppendResult's contiguity invariant, or worse, give two appends the
        // same offsets and make the consumer dedup one of them away.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        try (DefaultIngest ingest = ingest(store)) {
            CompletableFuture<AppendResult> a = appendAsync(ingest, "logs", 0, 4);
            awaitPending(ingest, 1);
            CompletableFuture<AppendResult> b = appendAsync(ingest, "logs", 0, 3);
            awaitPending(ingest, 2);
            CompletableFuture<AppendResult> c = appendAsync(ingest, "logs", 0, 2);
            awaitPending(ingest, 3);
            ingest.flushNow();

            assertThat(a.get(10, TimeUnit.SECONDS).firstOffset()).isZero();
            assertThat(a.get().lastOffset()).isEqualTo(3);
            assertThat(b.get().firstOffset()).isEqualTo(4);
            assertThat(b.get().lastOffset()).isEqualTo(6);
            assertThat(c.get().firstOffset()).isEqualTo(7);
            assertThat(c.get().lastOffset()).isEqualTo(8);
        }
    }

    @Test
    void anAppendReturnsOnTheFlushINTERVALWithNobodyDrivingTheFlush() throws Exception {
        // ⚠️ BLOCKING gap until this existed: every other test drives the flush
        // through the package-private flushNow() or through close(), so gutting
        // BOTH production triggers -- the flusher thread and the isFlushDue()
        // check in append -- left the whole suite green. The only path by which
        // a real producer's append ever returns was unverified, and if it broke,
        // every producer would hang forever while CI stayed green.
        //
        // ⚠️ A real clock and a short interval, deliberately: the trigger IS a
        // clock, and flushNow() is never called here.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        try (DefaultIngest ingest = ingest(store, new SubscriptionHub(),
                Duration.ofMillis(30))) {
            long base = store.counts().total();
            AppendResult result = ingest.append(IngestTestSupport.PRINCIPAL, "logs", 0, docs(3)::forEach);

            assertThat(result.recordCount()).isEqualTo(3);
            // ⚠️ M2.6: 2 (segment + commit, M1's own count) + 2 (this flush's
            // one genuinely new index, registered once -- ADR-0008).
            assertThat(store.counts().total() - base)
                    .as("the interval trigger produced exactly one segment, one commit, "
                            + "and one new-index registration")
                    .isEqualTo(4);
        }
    }

    @Test
    void theSizeTriggerFlushesBeforeTheIntervalElapses() throws Exception {
        // ⚠️ The other production trigger. With a one-hour interval only
        // maxSegmentBytes can fire, so this pins that append does not wait for
        // the clock when the buffer is already full.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        try (DefaultIngest ingest = new DefaultIngest(
                new IngestConfig(IngestTestSupport.NEVER, 4096L, "cluster-a"), store, IngestTestSupport.PREFIX, "pod1",
                new SubscriptionHub(), Clock.systemUTC(), index -> IngestTestSupport.LOGS)) {
            long base = store.counts().total();
            AppendResult result = ingest.append(IngestTestSupport.PRINCIPAL, "logs", 0, docs(400)::forEach);

            assertThat(result.recordCount()).isEqualTo(400);
            // ⚠️ M2.6: 2 (segment + commit) + 2 (one genuinely new index).
            assertThat(store.counts().total() - base).isEqualTo(4);
        }
    }

    @Test
    void aStoreFailureFailsTheWaitingAppendRatherThanHangingIt() throws Exception {
        // ⚠️ The class's natural failure mode is a HANG: a producer parked in
        // join() waits for a flush that never comes. Deleting the
        // completeExceptionally loop from flushLocked's catch left the suite
        // green, so nothing constrained the one path that keeps a store error
        // from wedging every producer on the pod.
        CountingBinStore store = new CountingBinStore(new StoreFakes.FailingPuts(new MemoryBinStore()));
        try (DefaultIngest ingest = ingest(store)) {
            CompletableFuture<AppendResult> f = appendAsync(ingest, "logs", 0, 3);
            awaitPending(ingest, 1);
            assertThatThrownBy(ingest::flushNow).isInstanceOf(IOException.class);

            assertThatThrownBy(() -> f.get(10, TimeUnit.SECONDS))
                    .hasRootCauseInstanceOf(IOException.class);
        }
    }

    @Test
    void offsetsArePerStreamAndNeitherSharedNorResetAcrossPartitions() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        try (DefaultIngest ingest = ingest(store)) {
            AppendResult p0first = appendOnce(ingest, "logs", 0, 4);
            AppendResult p1 = appendOnce(ingest, "logs", 1, 4);
            AppendResult p0second = appendOnce(ingest, "logs", 0, 2);

            // ⚠️ Three appends, because an earlier version asserted only that
            // both partitions start at 0 -- which a hardcoded
            // `new AppendResult(1, 0, 0)` also satisfies. The red run caught it
            // as a test that could not fail.
            assertThat(p0first.firstOffset()).isZero();
            // ⚠️ Partition 1 has its OWN counter; one shared counter starts it at 4.
            assertThat(p1.firstOffset()).isZero();
            // ⚠️ And partition 0 continues from 4, neither reset nor constant.
            assertThat(p0second.firstOffset()).isEqualTo(4);
            assertThat(p0second.lastOffset()).isEqualTo(5);
        }
    }

    @Test
    void theFirstAppendAfterARestartCostsTheSameAsAnyOther() throws Exception {
        // ⚠️ This is a COST test, not an offset test, and the difference caught a
        // hole: without recover(), CommitLog.commit still lands on the right
        // offsets, because its retry loop reads each occupied slot and applies
        // it on the way past. So asserting "offsets continue" passes either way.
        // What recovery actually buys is that the walk does not happen: the
        // first append after a restart must cost the SAME two requests as any
        // other, never a number that grows with commit-log history.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        int priorFlushes = 6;
        try (DefaultIngest first = ingest(store)) {
            for (int i = 0; i < priorFlushes; i++) {
                appendOnce(first, "logs", 0, 2);
            }
        }

        try (DefaultIngest second = ingest(store)) {
            long afterRecovery = store.counts().total();
            AppendResult resumed = appendOnce(second, "logs", 0, 3);

            // ⚠️ M2.6: 2 (segment + commit) + 2 more -- NOT a new-index
            // registration ("logs" is already registered from the prior
            // instance's flushes), but a fresh LOOKUP: `second`'s own
            // IndexOrdinalRegistry starts with an empty cache, so its first
            // ordinalFor("logs") costs one stat + one get, same shape as the
            // recovery walk this test's own docstring is about -- a real cost,
            // paid once per pod restart, never growing with commit-log
            // history.
            assertThat(store.counts().total() - afterRecovery)
                    .as("two requests plus one fresh ordinal lookup, whatever the log "
                            + "already holds")
                    .isEqualTo(4);
            // ⚠️ And the offsets still continue -- 6 flushes x 2 records.
            assertThat(resumed.firstOffset()).isEqualTo(priorFlushes * 2L);
        }
    }

    @Test
    void aPrincipalFromAnotherTrustDomainIsRefused() throws Exception {
        // ⚠️ ADR-0021: the domain is the boundary, and this is the only place
        // both halves are in hand. `logs` is in the allow-list, so the index
        // check alone would let this through and bundle another tenant's
        // records into this domain's segment.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        Principal other = new Principal("cluster-b", "producer-9", Set.of("logs"));
        try (DefaultIngest ingest = ingest(store)) {
            long base = store.counts().total();
            assertThatThrownBy(() -> ingest.append(other, "logs", 0, docs(1)::forEach))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(store.counts().total() - base).isZero();
        }
    }

    @Test
    void everyRefusalHappensBeforeAnythingIsBuffered() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        try (DefaultIngest ingest = ingest(store)) {
            long base = store.counts().total();
            assertThatThrownBy(() -> ingest.append(IngestTestSupport.PRINCIPAL, "logs", 0, List.<SegmentRecord>of()::forEach))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> ingest.append(IngestTestSupport.PRINCIPAL, "nope", 0, docs(1)::forEach))
                    .isInstanceOf(IllegalArgumentException.class);
            // ⚠️ The negative partition is refused by RunKey's own constructor,
            // not by a guard here. A duplicate check in DefaultIngest survived
            // every mutation -- no test could tell the two apart -- so it was
            // removed rather than given a test that proves nothing. This
            // assertion still pins the BEHAVIOUR, wherever it is enforced.
            assertThatThrownBy(() -> ingest.append(IngestTestSupport.PRINCIPAL, "logs", -1, docs(1)::forEach))
                    .isInstanceOf(IllegalArgumentException.class);

            assertThat(store.counts().total() - base).isZero();
            assertThat(ingest.pendingAppends())
                    .as("a refused append leaves nothing waiting for the next flush")
                    .isZero();
        }
    }

    @Test
    void closeFlushesWhatIsStillBufferedRatherThanLosingIt() throws Exception {
        // ⚠️ The shutdown path is the one whose failure loses the LAST segment,
        // and while append flushed on every call it was unreachable. A no-op
        // close() must not pass this.
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> seen = new CopyOnWriteArrayList<>();
        var sub = hub.subscribe(new RunKey(IngestTestSupport.LOGS, 0), seen::add);
        DefaultIngest ingest = ingest(store, hub, IngestTestSupport.NEVER);
        long base = store.counts().total();
        CompletableFuture<AppendResult> inflight = appendAsync(ingest, "logs", 0, 7);
        awaitPending(ingest, 1);
        assertThat(store.counts().total() - base)
                .as("nothing written before the flush").isZero();

        ingest.close();

        assertThat(inflight.get(10, TimeUnit.SECONDS).recordCount()).isEqualTo(7);
        // ⚠️ M2.6: 2 (segment + commit) + 2 (one genuinely new index).
        assertThat(store.counts().total() - base).isEqualTo(4);
        assertThat(seen).hasSize(1);
        sub.close();
    }

    @Test
    void anAppendAfterCloseIsRefused() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        DefaultIngest ingest = ingest(store);
        ingest.close();
        assertThatThrownBy(() -> ingest.append(IngestTestSupport.PRINCIPAL, "logs", 0, docs(1)::forEach))
                .isInstanceOf(IOException.class);
    }

}
