// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The leader's EXPOSED sends, run in the background one drain at a time
 * (M13.27w): each COMMIT or CONFIRM answer wakes it, since either can complete
 * a batch's quorum.
 *
 * <p>⚠️ WAKES DURING A DRAIN COALESCE INTO ONE MORE: a drain sends everything
 * owed when it reads, so one pass after the last wake covers every earlier
 * one -- never a thread per answer, never a wake lost.
 *
 * <p>⚠️ A DRAIN THAT THROWS ENDS ONLY ITSELF: the next wake starts another --
 * for an {@code Error} too, which leaves nothing owed behind it rather than a
 * count no thread will ever reset (M13.27w review round 1, P1).
 */
final class ExposedDrain {

    private final Runnable drain;
    private final AtomicInteger pending = new AtomicInteger();

    ExposedDrain(Runnable drain) {
        this.drain = Objects.requireNonNull(drain, "drain");
    }

    void wake() {
        if (pending.getAndIncrement() == 0) {
            Thread.ofVirtual().name("exposed-drain").start(this::run);
        }
    }

    /** Whether no drain runs or is owed: for tests, which must not sleep. */
    boolean idle() {
        return pending.get() == 0;
    }

    private void run() {
        try {
            while (true) {
                int seen = pending.get();
                try {
                    drain.run();
                } catch (RuntimeException failed) {
                    // ends this pass only; the loop below decides whether to go on
                }
                if (pending.compareAndSet(seen, 0)) {
                    return;
                }
            }
        } catch (Error fatal) {
            pending.set(0);
            throw fatal;
        }
    }
}
