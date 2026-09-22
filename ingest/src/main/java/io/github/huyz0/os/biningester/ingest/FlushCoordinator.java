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

    record Batch(List<DefaultIngest.Pending> pending, Accumulator accumulator,
            CompletableFuture<Void> done) {
    }

    @FunctionalInterface
    interface Handler {
        void flush(Batch batch) throws IOException;
    }

    private static final Batch POISON = new Batch(null, null, null);
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
        Batch batch = new Batch(pending, accumulator, new CompletableFuture<>());
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
            try {
                handler.flush(batch);
                batch.done().complete(null);
            } catch (IOException | RuntimeException e) {
                batch.done().completeExceptionally(e);
            } finally {
                lock.lock();
                try {
                    afterFlush.accept(batch);
                    queued = false;
                    current = null;
                    work.signalAll();
                } finally {
                    lock.unlock();
                }
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
