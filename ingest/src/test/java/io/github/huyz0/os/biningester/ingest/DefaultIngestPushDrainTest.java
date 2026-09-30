// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * M13.16 (M12 harvest R14): {@link DefaultIngest}'s shutdown gives its push
 * queue the production drain bound, and says through its public accessor how
 * many pushes it gave up on (M12.15 review P1, T4) -- where the constant was
 * pinned, not the bound the ingester is built with.
 *
 * <p>⚠️ FIVE SECONDS OF REAL TIME, on purpose: the public accessor has no
 * other way to see an abandoned push -- the ingester's bound is not injected --
 * so the test waits the bound out once. The bound's exact value is pinned in
 * zero time by {@code PushQueuePinsTest#defaultIngestGivesItsQueueTheProductionBound};
 * the window here is narrow enough to refuse a bound of 10 s too (review T1).
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DefaultIngestPushDrainTest {

    /** Holds the first delivery until released; counts nothing else. */
    private static final class HoldsTheFirst implements SubscriptionHub.Subscriber {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch gate = new CountDownLatch(1);

        @Override
        public SegmentSink open(List<SubscriptionHub.Push> pushes) {
            return (buffer, offset, length) -> { };
        }

        @Override
        public void complete(List<SubscriptionHub.Push> pushes, SegmentSink sink) {
            if (entered.getCount() > 0) {
                entered.countDown();
                try {
                    gate.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public String az() {
            return null;
        }
    }

    @Test
    void closingWaitsOutTheFiveSecondBoundAndCountsTheAbandonedPushesPublicly()
            throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        HoldsTheFirst held = new HoldsTheFirst();
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        try (var ignored = hub.subscribe(new RunKey(IngestTestSupport.LOGS, 0), held)) {
            DefaultIngest ingest = IngestTestSupport.ingest(store, hub, IngestTestSupport.NEVER);
            long elapsed;
            try {
                for (int push = 0; push < 3; push++) {
                    CompletableFuture<AppendResult> append = append(ingest);
                    IngestTestSupport.awaitPending(ingest, 1);
                    ingest.flushNow();
                    append.get(10, TimeUnit.SECONDS);
                    if (push == 0) {
                        assertThat(held.entered.await(10, TimeUnit.SECONDS))
                                .as("the premise: the first push is held").isTrue();
                    }
                }
                long started = System.nanoTime();
                ingest.close();
                elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            } finally {
                held.gate.countDown();
            }

            assertThat(ingest.abandonedPushes())
                    .as("⚠️ THE TWO QUEUED BEHIND THE HELD ONE, through the public accessor")
                    .isEqualTo(2);
            assertThat(elapsed).as("⚠️ THE PRODUCTION BOUND: five seconds, waited out")
                    .isBetween(4_500L, 7_500L);
        }
    }

    private static CompletableFuture<AppendResult> append(DefaultIngest ingest) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return ingest.append(IngestTestSupport.PRINCIPAL, "logs", 0, (byte) 0,
                        IngestTestSupport.docs(2)::forEach);
            } catch (IOException e) {
                throw new CompletionException(e);
            }
        });
    }
}
