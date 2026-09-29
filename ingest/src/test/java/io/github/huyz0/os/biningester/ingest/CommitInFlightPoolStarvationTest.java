// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.appendAsync;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.awaitPending;
import static io.github.huyz0.os.biningester.ingest.IngestTestSupport.flushAsync;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * M11.25 (H17): the candidate cause of {@code DefaultIngestTest}'s one class
 * timeout -- that its appends and its flush starve on the JVM's common
 * ForkJoin pool -- tested rather than guessed at.
 *
 * <p>⚠️ THE SCENARIO NEEDS THREE BLOCKING TASKS AT ONCE: the first append
 * waits for its commit, the flush waits in the gated commit, and the second
 * append must start while both wait. {@code CompletableFuture.supplyAsync}
 * without an executor runs them on the common pool, whose parallelism is the
 * core count less one -- three on a four-core runner -- and which every other
 * async task in the JVM shares. Here the pool is filled first, which is the
 * worst case of that sharing, made deterministic.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class CommitInFlightPoolStarvationTest {

    @Test
    void aCommitInFlightStillAdmitsTheNextAppendWhenTheCommonPoolIsFull() throws Exception {
        int parallelism = ForkJoinPool.getCommonPoolParallelism();
        CountDownLatch busy = new CountDownLatch(parallelism);
        CountDownLatch hold = new CountDownLatch(1);
        for (int i = 0; i < parallelism; i++) {
            ForkJoinPool.commonPool().execute(() -> {
                busy.countDown();
                try {
                    hold.await(60, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        try {
            assertThat(busy.await(10, TimeUnit.SECONDS))
                    .as("the premise: every common-pool thread is occupied").isTrue();
            StoreFakes.GatedCommit gate = new StoreFakes.GatedCommit(new MemoryBinStore());
            CountingBinStore store = new CountingBinStore(gate);
            try (DefaultIngest ingest = IngestTestSupport.ingest(store)) {
                gate.arm(); // after setup: the lease and the log's open are not held
                CompletableFuture<AppendResult> first = appendAsync(ingest, "logs", 0, 4);
                awaitPending(ingest, 1);
                CompletableFuture<Void> flush = flushAsync(ingest);
                assertThat(gate.entered.await(10, TimeUnit.SECONDS))
                        .as("⚠️ THE FLUSH REACHED ITS COMMIT with the common pool full")
                        .isTrue();
                CompletableFuture<AppendResult> second = appendAsync(ingest, "logs", 0, 4);
                try {
                    awaitPending(ingest, 1);
                } finally {
                    gate.release.countDown();
                }
                flush.get(10, TimeUnit.SECONDS);
                ingest.flushNow();
                assertThat(first.get(10, TimeUnit.SECONDS).recordCount()).isEqualTo(4);
                assertThat(second.get(10, TimeUnit.SECONDS).recordCount()).isEqualTo(4);
            }
        } finally {
            hold.countDown();
        }
    }
}
