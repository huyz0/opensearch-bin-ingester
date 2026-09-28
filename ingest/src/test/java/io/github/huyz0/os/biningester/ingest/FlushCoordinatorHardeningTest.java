// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The flush coordinator refuses what would lose records and survives what
 * used to end it (M11.10, H5; M10.32 review R1, M10.13 review R2 and T7).
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FlushCoordinatorHardeningTest {

    private static CompletableFuture<Void> enqueue(ReentrantLock lock,
            FlushCoordinator coordinator) {
        lock.lock();
        try {
            return coordinator.enqueue(List.of(), null);
        } finally {
            lock.unlock();
        }
    }

    @Test
    void aSecondBatchWhileOneIsQueuedIsRefusedLoudlyNotAnsweredWithTheFirstsFuture()
            throws Exception {
        ReentrantLock lock = new ReentrantLock();
        CountDownLatch release = new CountDownLatch(1);
        FlushCoordinator coordinator = new FlushCoordinator(lock, lock.newCondition(),
                batch -> {
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }, batch -> { });
        try {
            CompletableFuture<Void> first = enqueue(lock, coordinator);
            assertThatThrownBy(() -> enqueue(lock, coordinator))
                    .as("⚠️ A SECOND BATCH IS REFUSED, not handed the first's future while "
                            + "its records go nowhere")
                    .isInstanceOf(IllegalStateException.class);
            release.countDown();
            first.get(10, TimeUnit.SECONDS);
        } finally {
            coordinator.close();
        }
    }

    @Test
    void anErrorFromTheHandlerFailsItsBatchAndTheWorkerFlushesTheNext() throws Exception {
        ReentrantLock lock = new ReentrantLock();
        AtomicInteger flushes = new AtomicInteger();
        Error injected = new Error("injected: the handler died");
        FlushCoordinator coordinator = new FlushCoordinator(lock, lock.newCondition(),
                batch -> {
                    if (flushes.incrementAndGet() == 1) {
                        throw injected;
                    }
                }, batch -> { });
        try {
            CompletableFuture<Void> first = enqueue(lock, coordinator);
            assertThatThrownBy(() -> first.get(10, TimeUnit.SECONDS))
                    .as("⚠️ ITS WAITERS ARE TOLD, rather than hanging on a future nobody "
                            + "completes")
                    .isInstanceOf(ExecutionException.class).hasCause(injected);

            CompletableFuture<Void> second = enqueue(lock, coordinator);
            second.get(10, TimeUnit.SECONDS);
            assertThat(flushes).as("⚠️ AND THE WORKER LIVED to flush the next batch")
                    .hasValue(2);
        } finally {
            coordinator.close();
        }
    }

    @Test
    void aSettlementThatThrowsDoesNotStrandTheOthers() throws Exception {
        ReentrantLock lock = new ReentrantLock();
        AtomicBoolean second = new AtomicBoolean();
        FlushCoordinator coordinator = new FlushCoordinator(lock, lock.newCondition(),
                batch -> {
                    batch.settle(() -> {
                        throw new IllegalStateException("injected: a settlement failed");
                    });
                    batch.settle(() -> second.set(true));
                }, batch -> { });
        try {
            enqueue(lock, coordinator).get(10, TimeUnit.SECONDS);
            assertThat(second)
                    .as("⚠️ THE PRODUCER BEHIND A THROWING SETTLEMENT IS STILL RELEASED")
                    .isTrue();
        } finally {
            coordinator.close();
        }
    }
}
