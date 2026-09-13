// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link SegmentSink} that BLOCKS inside {@code write} instead of throwing.
 *
 * <p>⚠️ THE HALF `SegmentProxy` DID NOT HANDLE. Its contract said "a slow or
 * dead consumer must not stall" while only the dead half existed: the serving
 * loop catches a throw and drops that consumer, and a sink that simply never
 * returns held the serving thread, the shared chunk buffer and the open store
 * {@code InputStream} for every other consumer in the fan-out.
 *
 * <p>⚠️ IT BLOCKS ONCE, NOT FOREVER, and the bound is a safety net rather than
 * the thing under test: a fixture that hung indefinitely would fail the suite
 * by timeout instead of by assertion, and say nothing about which.
 */
final class BlockingSegmentSink implements SegmentSink {

    private final CountDownLatch release = new CountDownLatch(1);
    private final AtomicInteger writes = new AtomicInteger();

    @Override
    public void write(byte[] buffer, int offset, int length) {
        // ⚠️ THE FIRST HAND-OFF ONLY, and bounded. A sink that blocked on every
        // chunk would take 16 x the bound to observe under the UNFIXED serving
        // loop, and one that blocked forever would fail the suite by timeout
        // rather than by assertion -- saying nothing about which behaviour was
        // wrong. Blocking once is enough: the deadline is per chunk, so missing
        // one is what drops a consumer.
        if (writes.getAndIncrement() == 0) {
            try {
                release.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    void release() {
        release.countDown();
    }

    int writes() {
        return writes.get();
    }
}
