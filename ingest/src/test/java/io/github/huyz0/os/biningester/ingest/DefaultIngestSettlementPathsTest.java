// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.LOGS;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.PREFIX;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.appendAsync;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.awaitPending;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.pinnedIntervalConfig;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.CommitDeferredException;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Every release path {@link DefaultIngest} routes through the flush's
 * settlements, and the order they run in (M11.10, H5; M10.13 review T4-T6,
 * M10.32 review T1). {@code DefaultIngestSettlementTest} pins the ack and the
 * failure; these pin the rest.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DefaultIngestSettlementPathsTest {

    /** Delegates commits; answers {@code epoch()} from a value the test moves. */
    private static final class Wrapped implements Sequencer {
        final Sequencer delegate;
        final AtomicLong epoch = new AtomicLong(7);
        volatile java.util.function.Function<List<CommitRequest>, CommitDelta> override;

        Wrapped(Sequencer delegate) {
            this.delegate = delegate;
        }

        @Override
        public CommitDelta commit(CommitRequest request) throws IOException {
            return commitAll(List.of(request));
        }

        @Override
        public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
            var o = override;
            return o != null ? o.apply(requests) : delegate.commitAll(requests);
        }

        @Override
        public long epoch() {
            return epoch.get();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static DefaultIngest ingest(CountingBinStore store, Sequencer sequencer,
            SubscriptionHub hub) throws IOException {
        return new DefaultIngest(pinnedIntervalConfig(Duration.ofDays(1), 8L << 20), store,
                PREFIX, "pod1", sequencer, hub, Clock.systemUTC(), index -> LOGS, ignored -> { }, new IndexCostLedger());
    }

    private static Boolean queuedAtRelease(DefaultIngest ingest) throws Exception {
        CompletableFuture<AppendResult> append = appendAsync(ingest, "logs", 0, 2);
        awaitPending(ingest, 1);
        AtomicReference<Boolean> queued = new AtomicReference<>();
        ingest.whenReleased(0, () -> queued.set(ingest.flushQueued()));
        try {
            ingest.flushNow();
        } catch (IOException | RuntimeException ignored) {
            // the append's own outcome is what the case reads
        }
        append.handle((result, failure) -> null).get(10, TimeUnit.SECONDS);
        assertThat(queued.get()).as("the release was observed").isNotNull();
        return queued.get();
    }

    @Test
    void aDeferredCommitsProducerIsReleasedOnlyOnceTheBatchIsNoLongerQueued()
            throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        try (DefaultIngest ingest = ingest(store, deferring(), new SubscriptionHub())) {
            assertThat(queuedAtRelease(ingest))
                    .as("⚠️ THE DEFERRAL RELEASE IS A SETTLEMENT TOO").isFalse();
        }
    }

    /** Every commit defers to the inbox. */
    private static Sequencer deferring() {
        return new Sequencer() {
            @Override
            public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
                throw new CommitDeferredException(PREFIX + "/ctl/inbox/0/pod1/i/0.intent",
                        new IOException("partitioned"));
            }

            @Override
            public void close() {
            }
        };
    }

    @Test
    void aProducerWhoseStreamTheDeltaOmitsIsReleasedOnlyOnceTheBatchIsNoLongerQueued()
            throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        Wrapped sequencer = new Wrapped(IngestTestSupport.sequencer(store, "pod1"));
        // ⚠️ A DELTA FOR ANOTHER SEGMENT ONLY: nothing in it is this batch's.
        sequencer.override = requests -> new CommitDelta(1, List.of(
                new io.github.huyz0.os.biningester.format.SegmentCommit("another/segment",
                        List.of(new io.github.huyz0.os.biningester.format.RunCommit(
                                new RunKey(LOGS, 0), 2, 0)),
                        new io.github.huyz0.os.biningester.format.SegmentCommit.Attribution(
                                "pod9", "inc-9", 0))));
        try (DefaultIngest ingest = ingest(store, sequencer, new SubscriptionHub())) {
            assertThat(queuedAtRelease(ingest))
                    .as("⚠️ THE MISSING-STREAM FAILURE IS A SETTLEMENT TOO").isFalse();
        }
    }

    @Test
    void aPushCarriesTheEpochReadAtFlushTimeNotAtItsOwnSettlement() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        Wrapped sequencer = new Wrapped(IngestTestSupport.sequencer(store, "pod1"));
        SubscriptionHub hub = new SubscriptionHub();
        List<Long> epochs = new CopyOnWriteArrayList<>();
        CountDownLatch pushed = new CountDownLatch(1);
        try (var subscription = hub.subscribe(new RunKey(LOGS, 0), pushes -> {
                    pushes.forEach(p -> epochs.add(p.sequencerEpoch()));
                    pushed.countDown();
                    return null;
                });
                DefaultIngest ingest = ingest(store, sequencer, hub)) {
            CompletableFuture<AppendResult> append = appendAsync(ingest, "logs", 0, 2);
            awaitPending(ingest, 1);
            // ⚠️ THE ACK's SETTLEMENT RUNS BEFORE THE PUSH's, on the worker: moving
            // the epoch there is a chain moving on between the flush and its push.
            ingest.whenReleased(0, () -> sequencer.epoch.set(99));
            ingest.flushNow();
            append.get(10, TimeUnit.SECONDS);
            assertThat(pushed.await(10, TimeUnit.SECONDS)).isTrue();

            assertThat(epochs).as("⚠️ THE FLUSH's EPOCH, 7, not the chain's later 99")
                    .containsOnly(7L);
        }
    }

    @Test
    void aPushIsHandedOnOnlyAfterTheProducersAreReleased() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        AtomicBoolean released = new AtomicBoolean();
        AtomicReference<Boolean> releasedAtPush = new AtomicReference<>();
        CountDownLatch pushed = new CountDownLatch(1);
        try (var subscription = hub.subscribe(new RunKey(LOGS, 0), pushes -> {
                    releasedAtPush.compareAndSet(null, released.get());
                    pushed.countDown();
                    return null;
                });
                DefaultIngest ingest = ingest(store, IngestTestSupport.sequencer(store, "pod1"),
                        hub)) {
            CompletableFuture<AppendResult> append = appendAsync(ingest, "logs", 0, 2);
            awaitPending(ingest, 1);
            // ⚠️ THE RELEASE WAITS FOR A PUSH, up to a second, and only then says
            // it happened: a push handed on before the acks arrives inside that
            // second and sees "not released"; one handed on after cannot arrive.
            ingest.whenReleased(0, () -> {
                try {
                    pushed.await(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                released.set(true);
            });
            ingest.flushNow();
            append.get(10, TimeUnit.SECONDS);
            assertThat(pushed.await(10, TimeUnit.SECONDS)).isTrue();

            assertThat(releasedAtPush.get())
                    .as("⚠️ A CONSUMER IS NEVER TOLD OF A SEGMENT BEFORE ITS PRODUCER IS")
                    .isTrue();
        }
    }

    @Test
    void aBufferDueBehindAFailedQueuedFlushIsFlushedWhenThatFlushFails() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        Wrapped sequencer = new Wrapped(IngestTestSupport.sequencer(store, "pod1"));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean failedOnce = new AtomicBoolean();
        sequencer.override = requests -> {
            if (failedOnce.compareAndSet(false, true)) {
                entered.countDown();
                try {
                    release.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                throw new java.io.UncheckedIOException(new IOException("injected: commit failed"));
            }
            try {
                return sequencer.delegate.commitAll(requests);
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        };
        // ⚠️ A ONE-BYTE SEGMENT BUDGET makes every append due at once, and a DAY's
        // interval makes the flush loop's own poll (floor / 4, six hours) far
        // longer than the case: only the failed flush's signal can wake it.
        try (DefaultIngest ingest = new DefaultIngest(
                pinnedIntervalConfig(Duration.ofDays(1), 1), store, PREFIX, "pod1",
                sequencer, new SubscriptionHub(), Clock.systemUTC(), index -> LOGS, ignored -> { }, new IndexCostLedger())) {
            CompletableFuture<AppendResult> first = appendAsync(ingest, "logs", 0, 2);
            assertThat(entered.await(10, TimeUnit.SECONDS)).as("the first flush is queued")
                    .isTrue();
            CompletableFuture<AppendResult> second = appendAsync(ingest, "logs", 0, 3);
            awaitPending(ingest, 1);
            release.countDown();

            assertThat(first.handle((r, f) -> f).get(10, TimeUnit.SECONDS))
                    .as("the premise: the queued flush failed").isNotNull();
            assertThat(second.get(10, TimeUnit.SECONDS).recordCount())
                    .as("⚠️ THE BUFFER DUE BEHIND IT IS FLUSHED NOW, not at the next poll "
                            + "six hours on")
                    .isEqualTo(3);
        }
    }
}
