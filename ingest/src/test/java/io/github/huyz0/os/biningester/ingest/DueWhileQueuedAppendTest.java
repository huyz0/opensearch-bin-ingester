// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.appendAsync;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.awaitPending;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * An append that finds the buffer due WHILE a flush is queued is carried by
 * the next flush, never dropped (M10.32).
 *
 * <p>⚠️ THE DEFECT: {@code append} enqueued a flush whenever the buffer was
 * due, without asking whether one was already queued. The detach cleared the
 * pending list and swapped the buffer, and the coordinator -- which admits one
 * batch at a time -- answered with the QUEUED batch's future and discarded the
 * new one. Those records were never written, and their producer blocked for
 * ever on an uninterruptible join. Reached whenever the size trigger or a
 * deadline fired during a slow PUT or commit.
 *
 * <p>⚠️ Deterministic, not a race: the first flush's commit is HELD on a latch,
 * and a one-byte segment budget makes every append due at once.
 */
class DueWhileQueuedAppendTest {

    /** Holds the FIRST commit until released; every later one passes straight through. */
    private static final class HeldSequencer implements Sequencer {
        private final Sequencer delegate;
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private boolean held;

        HeldSequencer(Sequencer delegate) {
            this.delegate = delegate;
        }

        private void holdFirst() throws IOException {
            synchronized (this) {
                if (held) {
                    return;
                }
                held = true;
            }
            entered.countDown();
            try {
                if (!release.await(30, TimeUnit.SECONDS)) {
                    throw new IOException("the test never released the held commit");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted", interrupted);
            }
        }

        @Override
        public CommitDelta commit(CommitRequest request) throws IOException {
            holdFirst();
            return delegate.commit(request);
        }

        @Override
        public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
            holdFirst();
            return delegate.commitAll(requests);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    @Test
    void anAppendDueWhileAFlushIsQUEUEDIsCarriedByTheNEXTFlushNotDROPPED() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        HeldSequencer sequencer =
                new HeldSequencer(IngestTestSupport.sequencer(store, "pod1"));
        try (DefaultIngest ingest = new DefaultIngest(
                IngestTestSupport.pinnedIntervalConfig(Duration.ofDays(1), 1),
                store, IngestTestSupport.PREFIX, "pod1", sequencer, new SubscriptionHub(),
                Clock.systemUTC(), index -> IngestTestSupport.LOGS, ignored -> { }, new IndexCostLedger())) {
            CompletableFuture<AppendResult> first = appendAsync(ingest, "logs", 0, 2);
            assertThat(sequencer.entered.await(10, TimeUnit.SECONDS))
                    .as("the first flush is queued, its commit held").isTrue();

            CompletableFuture<AppendResult> second = appendAsync(ingest, "logs", 0, 3);
            // ⚠️ THE ASSERTION THAT FAILS ON THE DEFECT: the second append is due
            // at once, and detaching it behind the queued flush emptied the
            // pending list, so it never shows as waiting.
            awaitPending(ingest, 1);
            sequencer.release.countDown();

            assertThat(first.get(10, TimeUnit.SECONDS).firstOffset()).isZero();
            AppendResult carried = second.get(10, TimeUnit.SECONDS);
            assertThat(carried.firstOffset())
                    .as("the second append is written after the first, not lost").isEqualTo(2);
            assertThat(carried.lastOffset()).isEqualTo(4);
        }
    }
}
