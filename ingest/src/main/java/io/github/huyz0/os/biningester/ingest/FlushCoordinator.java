// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** Detaches active ingest buffers and serializes their ordered store work. */
final class FlushCoordinator {
    private static final System.Logger LOG =
            System.getLogger(FlushCoordinator.class.getName());

    record Batch(List<DefaultIngest.Pending> pending, Accumulator accumulator,
            CompletableFuture<Void> done, List<Runnable> settlements) {
        /**
         * ⚠️ Defers a waiter's release until this batch is no longer queued
         * (M10.13): a producer woken inside the handler found it still queued,
         * so its next flushNow() -- close()'s -- re-threw the same failure.
         */
        void settle(Runnable settlement) {
            settlements.add(settlement);
        }
    }

    @FunctionalInterface
    interface Handler {
        void flush(Batch batch) throws IOException;
    }

    private static final Batch POISON = new Batch(null, null, null, null);
    private final ReentrantLock lock;
    private final Condition work;
    private final Handler handler;
    private final java.util.function.Consumer<Batch> afterFlush;
    private final LinkedBlockingQueue<Batch> queue = new LinkedBlockingQueue<>();
    private final Thread worker;
    private Batch current;
    private boolean queued;

    FlushCoordinator(ReentrantLock lock, Condition work, Handler handler,
            java.util.function.Consumer<Batch> afterFlush) {
        this.lock = lock;
        this.work = work;
        this.handler = handler;
        this.afterFlush = afterFlush;
        this.worker = Thread.ofVirtual().name("binstore-flush-worker").start(this::run);
    }

    boolean isQueued() {
        return queued;
    }

    CompletableFuture<Void> currentDone() {
        return current.done();
    }

    /** Caller holds the ingest lock. */
    CompletableFuture<Void> enqueue(List<DefaultIngest.Pending> pending,
            Accumulator accumulator) {
        if (queued) {
            return current.done();
        }
        Batch batch = new Batch(pending, accumulator, new CompletableFuture<>(),
                new java.util.ArrayList<>());
        current = batch;
        queued = true;
        queue.add(batch);
        return batch.done();
    }

    private void run() {
        while (true) {
            Batch batch;
            try {
                batch = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (batch == POISON) {
                return;
            }
            boolean flushed = false;
            Throwable failure = null;
            try {
                handler.flush(batch);
                flushed = true;
            } catch (IOException | RuntimeException e) {
                failure = e;
            } finally {
                // ⚠️ M10.13: SETTLED AND COMPLETED ONLY ONCE NO LONGER QUEUED.
                // Waking a waiter first -- a producer or a flushNow() caller --
                // let its next flushNow(), close()'s typically, find this batch
                // still queued and re-throw its outcome: the same exception
                // twice, which try-with-resources cannot suppress into itself.
                // A preempted worker opened that window, so only load did.
                lock.lock();
                try {
                    afterFlush.accept(batch);
                    queued = false;
                    current = null;
                    work.signalAll();
                } finally {
                    lock.unlock();
                    try {
                        settle(batch);
                    } finally {
                        if (flushed) {
                            batch.done().complete(null);
                        } else if (failure != null) {
                            batch.done().completeExceptionally(failure);
                        }
                    }
                }
            }
        }
    }

    /**
     * Runs every settlement, each on its own: one that throws must not strand
     * the producers behind it, nor the batch's own future.
     */
    private static void settle(Batch batch) {
        for (Runnable settlement : batch.settlements()) {
            try {
                settlement.run();
            } catch (RuntimeException failed) {
                LOG.log(System.Logger.Level.WARNING,
                        "a flush settlement failed; the others still run", failed);
            }
        }
    }

    static void await(CompletableFuture<Void> done) throws IOException {
        try {
            done.join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof IOException io) {
                throw io;
            }
            throw e;
        }
    }

    void close() {
        queue.add(POISON);
        try {
            if (!worker.join(Duration.ofSeconds(10))) {
                worker.interrupt();
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            worker.interrupt();
        }
    }
}
