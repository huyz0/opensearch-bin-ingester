// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * A {@link SegmentSink} that blocks and IGNORES interruption.
 *
 * <p>⚠️ THE ONLY FIXTURE THAT CAN SEE THE STALL. {@code BlockingSegmentSink}
 * returns when interrupted, so it cannot distinguish a serving loop that walks
 * away from a slow sink from one that waits for it -- and review MEASURED that
 * gap: the first version of the deadline closed its executor with
 * try-with-resources, whose {@code close()} is an unbounded
 * {@code awaitTermination}, so {@code streamTo} returned only when the slow
 * sink did. Every case passed.
 *
 * <p>⚠️ IT REPORTS WHETHER IT IS STILL INSIDE {@code write}, which is what
 * makes the assertion race-free without a clock: if {@code streamTo} has
 * returned while this sink has not left {@code write}, the serving path did
 * not wait for it. A timing assertion would be the flaky way to ask the same
 * question.
 */
final class UninterruptibleSegmentSink implements SegmentSink {

    private final CountDownLatch release = new CountDownLatch(1);
    private volatile boolean inside;
    private volatile boolean everEntered;

    @Override
    public void write(byte[] buffer, int offset, int length) {
        inside = true;
        everEntered = true;
        try {
            // ⚠️ THE INTERRUPT IS SWALLOWED AND THE WAIT RESUMED, which is the
            // whole point: `shutdownNow` interrupts, and a sink that honours it
            // would leave on its own. The bound is a safety net so a broken
            // serving path fails the suite by assertion rather than by hanging.
            long deadline = 10;
            while (deadline > 0) {
                try {
                    if (release.await(deadline, TimeUnit.SECONDS)) {
                        break;
                    }
                    break;
                } catch (InterruptedException swallowed) {
                    deadline--;
                }
            }
        } finally {
            inside = false;
        }
    }

    /** Whether this sink is still inside {@code write} right now. */
    boolean stillInsideWrite() {
        return inside;
    }

    boolean everEntered() {
        return everEntered;
    }

    void release() {
        release.countDown();
    }
}
