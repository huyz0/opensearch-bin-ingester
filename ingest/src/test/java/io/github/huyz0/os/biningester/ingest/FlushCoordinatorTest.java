// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.Test;

/**
 * No waiter of a flush -- a {@code flushNow()} caller on the batch's future, or
 * a producer released by a settlement the handler registered -- sees its batch
 * still queued (M10.13).
 *
 * <p>⚠️ THE ROOT CAUSE OF TWO LOAD-SENSITIVE FAILURES,
 * {@code DefaultIngestTest#aStoreFailureFailsTheWaitingAppendRatherThanHangingIt}
 * and {@code CommitRetryTripleTest#anABANDONEDFlushBURNSItsNumberRatherThanWedgingThePod}:
 * the worker completed the batch's future BEFORE clearing {@code queued}. A
 * caller woken by a FAILED flush that went straight on to
 * {@code DefaultIngest.close()} found the batch still queued, awaited the
 * same future, and re-threw the SAME exception -- which try-with-resources
 * then tried to suppress into itself ("Self-suppression not permitted"), or
 * which escaped a test that had already asserted it. Only a preempted worker
 * opened the window, hence load.
 *
 * <p>⚠️ Observed WITHOUT a race: a {@code whenComplete} callback registered
 * before the flush runs on the completing thread, at the instant of
 * completion, so it reads exactly what a woken waiter could.
 */
class FlushCoordinatorTest {

    @Test
    void aFAILEDFlushIsNoLongerQueuedWhenItsWaiterWakes() throws Exception {
        IOException injected = new IOException("injected: the store is unavailable");
        CompletableFuture<Void> done = assertQueuedClearedAtCompletion(batch -> {
            throw injected;
        });

        assertThat(done).as("the waiter is told the flush FAILED, with its own cause")
                .isCompletedExceptionally();
        assertThat(done.handle((ignored, failure) -> failure).join()).isSameAs(injected);
    }

    @Test
    void aProducerReleasedByTheHandlerIsReleasedOnlyOnceTheBatchIsNoLongerQueued()
            throws Exception {
        ReentrantLock lock = new ReentrantLock();
        AtomicReference<FlushCoordinator> self = new AtomicReference<>();
        AtomicReference<Boolean> queuedAtRelease = new AtomicReference<>();
        FlushCoordinator coordinator = new FlushCoordinator(lock, lock.newCondition(), batch -> {
            batch.settle(() -> queuedAtRelease.set(queuedUnder(lock, self.get())));
            throw new IOException("injected: the commit failed");
        }, batch -> { });
        self.set(coordinator);
        try {
            CompletableFuture<Void> done;
            lock.lock();
            try {
                done = coordinator.enqueue(List.of(), null);
            } finally {
                lock.unlock();
            }
            done.handle((ignored, failure) -> null).get(10, TimeUnit.SECONDS);

            assertThat(queuedAtRelease.get())
                    .as("⚠️ a producer woken inside the handler found its batch still queued, "
                            + "so its close() re-threw the failure it had just been given")
                    .isFalse();
        } finally {
            coordinator.close();
        }
    }

    @Test
    void aSUCCESSFULFlushIsNoLongerQueuedWhenItsWaiterWakes() throws Exception {
        assertQueuedClearedAtCompletion(batch -> { });
    }

    private static CompletableFuture<Void> assertQueuedClearedAtCompletion(
            FlushCoordinator.Handler handler) throws Exception {
        ReentrantLock lock = new ReentrantLock();
        CompletableFuture<Void> release = new CompletableFuture<>();
        FlushCoordinator coordinator = new FlushCoordinator(lock, lock.newCondition(), batch -> {
            release.join();
            handler.flush(batch);
        }, batch -> { });
        try {
            CompletableFuture<Void> done;
            lock.lock();
            try {
                done = coordinator.enqueue(List.of(), null);
            } finally {
                lock.unlock();
            }
            AtomicReference<Boolean> queuedAtCompletion = new AtomicReference<>();
            CompletableFuture<Void> observed = done.whenComplete((ignored, failure) ->
                    queuedAtCompletion.set(queuedUnder(lock, coordinator)));

            release.complete(null);
            observed.handle((ignored, failure) -> null).get(10, TimeUnit.SECONDS);

            assertThat(queuedAtCompletion.get())
                    .as("⚠️ a waiter woken by this flush must not find it still queued, or "
                            + "its next flushNow() awaits -- and re-throws -- the same outcome")
                    .isFalse();
            return done;
        } finally {
            coordinator.close();
        }
    }

    private static boolean queuedUnder(ReentrantLock lock, FlushCoordinator coordinator) {
        lock.lock();
        try {
            return coordinator.isQueued();
        } finally {
            lock.unlock();
        }
    }
}
