// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.appendAsync;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.awaitPending;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.ingest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/**
 * A PRODUCER is released only once its flush is no longer queued (M10.13).
 *
 * <p>⚠️ {@code FlushCoordinatorTest} proves the settlement mechanism with a
 * synthetic handler; this pins that {@link DefaultIngest} actually routes every
 * producer release through it. Completing a producer's future directly inside
 * the flush -- the round-1 defect -- let a producer woken by a FAILED flush
 * find the batch still queued, so its {@code close()} re-threw the failure it
 * had just been given.
 *
 * <p>⚠️ The check runs on the releasing thread at the instant of release, so
 * it needs no race to fail.
 */
class DefaultIngestSettlementTest {

    @Test
    void aProducerOfAFAILEDFlushIsReleasedOnlyOnceTheBatchIsNoLongerQueued() throws Exception {
        CountingBinStore store =
                new CountingBinStore(new StoreFakes.FailingPuts(new MemoryBinStore()));
        try (DefaultIngest ingest = ingest(store)) {
            assertThat(queuedAtRelease(ingest, true)).isFalse();
        }
    }

    @Test
    void aProducerOfASUCCESSFULFlushIsReleasedOnlyOnceTheBatchIsNoLongerQueued()
            throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        try (DefaultIngest ingest = ingest(store)) {
            assertThat(queuedAtRelease(ingest, false)).isFalse();
        }
    }

    private static Boolean queuedAtRelease(DefaultIngest ingest, boolean flushFails)
            throws Exception {
        CompletableFuture<AppendResult> append = appendAsync(ingest, "logs", 0, 2);
        awaitPending(ingest, 1);
        AtomicReference<Boolean> queued = new AtomicReference<>();
        ingest.whenReleased(0, () -> queued.set(ingest.flushQueued()));

        if (flushFails) {
            assertThatThrownBy(ingest::flushNow).isInstanceOf(IOException.class);
        } else {
            ingest.flushNow();
        }
        append.handle((result, failure) -> null).get(10, TimeUnit.SECONDS);
        assertThat(queued.get()).as("the release was observed").isNotNull();
        return queued.get();
    }
}
