// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A sink that COMPARES EQUAL to every other sink of its type, optionally
 * blocking on its first chunk.
 *
 * <p>⚠️ IT EXISTS TO SEPARATE {@code ==} FROM {@code equals} IN ONE FAN-OUT
 * (M5.58a). {@code SegmentProxy.streamTo}'s drop list is built by identity
 * rather than by {@code List.removeAll}, and review MEASURED that no fixture in
 * the tree could tell the two apart: every other sink inherits
 * {@code Object.equals}, so {@code equals} IS {@code ==} for all of them and
 * {@code removeAll} survives every case. A {@code SegmentSink} is
 * caller-supplied and may implement {@code equals} however it likes, which is
 * the whole reason the production loop does not use it.
 *
 * <p>⚠️ AND {@code hashCode} IS CONSTANT WITH IT, because a type whose
 * {@code equals} is total and whose {@code hashCode} is not would be broken in
 * a way that has nothing to do with what this fixture is for.
 *
 * <p>⚠️ ASSERTIONS AGAINST IT USE {@code isSameAs}, NEVER {@code contains}:
 * AssertJ's {@code containsExactly} compares with {@code equals}, so against
 * this type it cannot tell the right sink from the wrong one either.
 */
final class EqualToEveryOtherSink implements SegmentSink {

    private final CountDownLatch release = new CountDownLatch(1);
    private final AtomicInteger writes = new AtomicInteger();
    private final boolean blocks;

    EqualToEveryOtherSink(boolean blocks) {
        this.blocks = blocks;
    }

    @Override
    public void write(byte[] buffer, int offset, int length) {
        // ⚠️ THE FIRST HAND-OFF ONLY, and bounded -- the same shape and the
        // same reasons as `BlockingSegmentSink`, which this fixture cannot
        // simply extend because it needs a non-blocking twin that still
        // compares equal to it.
        // ⚠️ COUNTED ONCE, OUTSIDE THE BRANCH. An earlier version incremented
        // inside the `blocks &&` test AND again in its else, so a blocking
        // instance counted twice from its second chunk on -- harmless to an
        // `isPositive` premise and wrong for the first case that asserts a
        // number.
        int seen = writes.getAndIncrement();
        if (blocks && seen == 0) {
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof EqualToEveryOtherSink;
    }

    @Override
    public int hashCode() {
        return EqualToEveryOtherSink.class.hashCode();
    }

    void release() {
        release.countDown();
    }

    int writes() {
        return writes.get();
    }
}
