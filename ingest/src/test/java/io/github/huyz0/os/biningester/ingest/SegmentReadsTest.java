// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static io.github.huyz0.os.biningester.ingest.SegmentProxyFixtures.KEY;
import static io.github.huyz0.os.biningester.ingest.SegmentProxyFixtures.RecordingSink;
import static io.github.huyz0.os.biningester.ingest.SegmentProxyFixtures.SEGMENT_BYTES;
import static io.github.huyz0.os.biningester.ingest.SegmentProxyFixtures.segment;
import static io.github.huyz0.os.biningester.ingest.SegmentProxyFixtures.storeHolding;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The proxy route's reads (M10.1, FR-6, NFR-4, NFR-6): single-flight per
 * segment, and the gate is never held across a consumer's sink.
 */
class SegmentReadsTest {

    private static final int CHUNK = 64 * 1024;

    private static SegmentReads readsOver(BinStore store) {
        return new SegmentReads(new SegmentProxy(store, CHUNK,
                new SegmentCache(4L * SEGMENT_BYTES)));
    }

    @Test
    void concurrentColdReadsOfOneSegmentCostOneGetAndEachGetsItWhole() throws Exception {
        CountingBinStore counted = storeHolding(segment());
        CountDownLatch release = new CountDownLatch(1);
        BinStore gated = holdingGetsUntil(counted, release);
        SegmentReads reads = readsOver(gated);

        int n = 8;
        List<RecordingSink> sinks = new ArrayList<>();
        List<Thread> workers = new ArrayList<>();
        List<Throwable> failures = new java.util.concurrent.CopyOnWriteArrayList<>();
        for (int i = 0; i < n; i++) {
            RecordingSink sink = new RecordingSink();
            sinks.add(sink);
            workers.add(Thread.ofPlatform().start(() -> {
                try {
                    reads.serve(KEY, sink);
                } catch (Throwable t) {
                    failures.add(t);
                }
            }));
        }
        // Every worker is parked -- either on the one GET, or behind the gate
        // (single-flight), or in a GET of its own (no single-flight). Only
        // then is the store released, so the count below tells the two apart.
        awaitAllParked(workers);
        release.countDown();
        for (Thread worker : workers) {
            worker.join(TimeUnit.SECONDS.toMillis(30));
        }

        assertThat(failures).isEmpty();
        assertThat(counted.counts().gets())
                .as("%d concurrent cold reads of one segment are ONE store GET", n)
                .isEqualTo(1);
        for (RecordingSink sink : sinks) {
            assertThat(sink.received.toByteArray()).isEqualTo(segment());
        }
    }

    @Test
    void aCachedSegmentIsServedWithoutAGetAndInChunks() throws Exception {
        CountingBinStore store = storeHolding(segment());
        SegmentReads reads = readsOver(store);
        reads.serve(KEY, new RecordingSink());
        long before = store.counts().gets();

        RecordingSink sink = new RecordingSink();
        assertThat(reads.serve(KEY, sink)).isTrue();

        assertThat(store.counts().gets() - before).as("a hit is not a GET").isZero();
        assertThat(sink.received.toByteArray()).isEqualTo(segment());
        assertThat(sink.largestHandOff)
                .as("no write to a consumer exceeds one chunk, hit or miss")
                .isLessThanOrEqualTo(CHUNK);
        assertThat(sink.handOffs).isGreaterThan(1);
    }

    @Test
    void theGateIsNotHeldWhileASinkIsWriting() throws Exception {
        CountingBinStore store = storeHolding(segment());
        SegmentReads reads = readsOver(store);

        // A consumer whose socket stops accepting bytes: its sink blocks. It
        // arrives on a COLD cache, so it is the read that takes the gate --
        // a design streaming to the first consumer under the gate would hold
        // it here, and the second consumer would wait behind the stall.
        CountDownLatch stalled = new CountDownLatch(1);
        CountDownLatch unstall = new CountDownLatch(1);
        Thread slow = Thread.ofPlatform().start(() -> {
            try {
                reads.serve(KEY, (buffer, offset, length) -> {
                    stalled.countDown();
                    try {
                        unstall.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            } catch (IOException ignored) {
                // not under test
            }
        });
        assertThat(stalled.await(30, TimeUnit.SECONDS)).isTrue();

        RecordingSink other = new RecordingSink();
        Thread fast = Thread.ofPlatform().start(() -> {
            try {
                reads.serve(KEY, other);
            } catch (IOException ignored) {
                // asserted below by the bytes
            }
        });
        fast.join(TimeUnit.SECONDS.toMillis(30));
        try {
            assertThat(fast.isAlive())
                    .as("a stalled consumer must not stall another consumer of the same segment")
                    .isFalse();
            assertThat(other.received.toByteArray()).isEqualTo(segment());
        } finally {
            unstall.countDown();
            slow.join(TimeUnit.SECONDS.toMillis(30));
        }
    }

    @Test
    void aStoreFailureOnTheFillPropagatesAndNothingIsCached() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SegmentReads reads = readsOver(store);
        RecordingSink sink = new RecordingSink();

        assertThatThrownBy(() -> reads.serve(KEY, sink)).isInstanceOf(IOException.class);

        assertThat(sink.handOffs).as("nothing reached the consumer").isZero();
        store.put(KEY, io.github.huyz0.os.biningester.binstore.Body.ofBytes(segment()));
        RecordingSink retry = new RecordingSink();
        assertThat(reads.serve(KEY, retry)).isTrue();
        assertThat(retry.received.toByteArray())
                .as("the failure was not cached as an empty segment")
                .isEqualTo(segment());
        long before = store.counts().gets();
        assertThat(reads.serve(KEY, new RecordingSink())).isTrue();
        assertThat(store.counts().gets() - before)
                .as("a transient failure did not mark the segment uncacheable: the retry "
                        + "cached it, so the next read is not a GET")
                .isZero();
    }

    @Test
    void concurrentReadsOfAFailingSegmentShareOneFailedGet() throws Exception {
        java.util.concurrent.atomic.AtomicInteger gets =
                new java.util.concurrent.atomic.AtomicInteger();
        CountDownLatch release = new CountDownLatch(1);
        BinStore failing = (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("get")) {
                        gets.incrementAndGet();
                        release.await();
                        throw new IOException("the store timed out");
                    }
                    if (method.getName().equals("capabilities")) {
                        return new MemoryBinStore().capabilities();
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        SegmentReads reads = readsOver(failing);

        int n = 6;
        List<Thread> workers = new ArrayList<>();
        List<Throwable> outcomes = new java.util.concurrent.CopyOnWriteArrayList<>();
        for (int i = 0; i < n; i++) {
            workers.add(Thread.ofPlatform().start(() -> {
                try {
                    reads.serve(KEY, new RecordingSink());
                    outcomes.add(new AssertionError("served from a failing store"));
                } catch (Throwable t) {
                    outcomes.add(t);
                }
            }));
        }
        awaitAllParked(workers);
        release.countDown();
        for (Thread worker : workers) {
            worker.join(TimeUnit.SECONDS.toMillis(30));
        }

        assertThat(outcomes).hasSize(n).allMatch(t -> t instanceof IOException);
        assertThat(gets.get())
                .as("the waiters share the one failed read rather than each re-trying it")
                .isEqualTo(1);
        assertThat(reads.gatesHeld()).isZero();
    }

    @Test
    void concurrentReadsOfAnUncacheableSegmentFillItOnce() throws Exception {
        CountingBinStore counted = storeHolding(segment());
        CountDownLatch release = new CountDownLatch(1);
        SegmentReads reads = new SegmentReads(new SegmentProxy(
                holdingGetsUntil(counted, release), CHUNK, new SegmentCache(SEGMENT_BYTES / 2)));

        int n = 5;
        List<RecordingSink> sinks = new ArrayList<>();
        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            RecordingSink sink = new RecordingSink();
            sinks.add(sink);
            workers.add(Thread.ofPlatform().start(() -> {
                try {
                    reads.serve(KEY, sink);
                } catch (IOException ignored) {
                    // asserted below by the bytes
                }
            }));
        }
        awaitAllParked(workers);
        release.countDown();
        for (Thread worker : workers) {
            worker.join(TimeUnit.SECONDS.toMillis(30));
        }

        for (RecordingSink sink : sinks) {
            assertThat(sink.received.toByteArray()).isEqualTo(segment());
        }
        assertThat(counted.counts().gets())
                .as("one fill discovers it cannot be cached; the waiters do not re-fill it")
                .isLessThanOrEqualTo(n + 1L);
    }

    @Test
    void aCacheThatCannotHoldTheSegmentStillServesItWithOneGetPerRead() throws Exception {
        CountingBinStore store = storeHolding(segment());
        SegmentReads reads = new SegmentReads(new SegmentProxy(store, CHUNK,
                new SegmentCache(0)));

        RecordingSink first = new RecordingSink();
        RecordingSink second = new RecordingSink();
        assertThat(reads.serve(KEY, first)).isTrue();
        assertThat(reads.serve(KEY, second)).isTrue();

        assertThat(first.received.toByteArray()).isEqualTo(segment());
        assertThat(second.received.toByteArray()).isEqualTo(segment());
        assertThat(store.counts().gets())
                .as("no cache: one GET per read, never a discarded fill plus a second read")
                .isEqualTo(2);
    }

    @Test
    void aSegmentTooLargeToCacheCostsAtMostOneGetPerReadPlusTheFill() throws Exception {
        CountingBinStore store = storeHolding(segment());
        // A cache that can hold something, but not this segment.
        SegmentReads reads = new SegmentReads(new SegmentProxy(store, CHUNK,
                new SegmentCache(SEGMENT_BYTES / 2)));

        int k = 3;
        for (int i = 0; i < k; i++) {
            RecordingSink sink = new RecordingSink();
            assertThat(reads.serve(KEY, sink)).isTrue();
            assertThat(sink.received.toByteArray()).isEqualTo(segment());
        }

        assertThat(store.counts().gets())
                .as("K reads of an uncacheable segment: one GET each, plus at most one "
                        + "wasted fill -- never a fill per read")
                .isLessThanOrEqualTo(k + 1L);
    }

    @Test
    void noGateOutlivesTheReadsThatTookIt() throws Exception {
        // One gate per segment ever read would be a map growing with every
        // segment this node serves: memory in segments, never released.
        CountingBinStore store = storeHolding(segment());
        SegmentReads reads = readsOver(store);

        assertThat(reads.serve(KEY, new RecordingSink())).isTrue();
        assertThat(reads.gatesHeld()).as("after a cold read").isZero();

        CountingBinStore empty = new CountingBinStore(new MemoryBinStore());
        SegmentReads failing = readsOver(empty);
        assertThatThrownBy(() -> failing.serve(KEY, new RecordingSink()))
                .isInstanceOf(IOException.class);
        assertThat(failing.gatesHeld()).as("after a failed fill").isZero();
    }

    @Test
    void theMemoryOfRefusedSegmentsIsBounded() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        int keys = SegmentReads.REFUSED_KEYS_REMEMBERED + 3;
        for (int i = 0; i < keys; i++) {
            store.put("seg/huge-" + i, io.github.huyz0.os.biningester.binstore.Body.ofBytes(
                    new byte[2 * CHUNK]));
        }
        SegmentReads reads = new SegmentReads(new SegmentProxy(store, CHUNK,
                new SegmentCache(CHUNK)));

        for (int i = 0; i < keys; i++) {
            assertThat(reads.serve("seg/huge-" + i, new RecordingSink())).isTrue();
        }

        assertThat(reads.refusedKeysRemembered())
                .as("a node serving endless uncacheable segments remembers a bounded few")
                .isEqualTo(SegmentReads.REFUSED_KEYS_REMEMBERED);
    }

    @Test
    void aConsumerThatDiesMidStreamIsReportedAsNotServedWhole() throws Exception {
        SegmentReads reads = readsOver(storeHolding(segment()));
        int[] writes = {0};

        boolean whole = reads.serve(KEY, (buffer, offset, length) -> {
            if (++writes[0] == 2) {
                throw new IOException("peer reset");
            }
        });

        assertThat(whole).isFalse();
    }

    /** Parks every {@code get} until {@code release}, then reads through. */
    private static BinStore holdingGetsUntil(BinStore delegate, CountDownLatch release) {
        return (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (proxy, method, args) -> {
                    if (method.getName().equals("get")) {
                        release.await();
                    }
                    try {
                        Object result = method.invoke(delegate, args);
                        return result instanceof InputStream in ? new FilterInputStream(in) { }
                                : result;
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static void awaitAllParked(List<Thread> workers) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            boolean parked = workers.stream().allMatch(t -> {
                Thread.State s = t.getState();
                return s == Thread.State.WAITING || s == Thread.State.BLOCKED
                        || s == Thread.State.TIMED_WAITING;
            });
            if (parked) {
                return;
            }
            Thread.onSpinWait();
        }
        throw new AssertionError("workers never parked");
    }
}
