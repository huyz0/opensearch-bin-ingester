// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Concurrent cold reads of one segment cost one store GET (M5.63, NFR-4, M10
 * criterion 5).
 *
 * <p>⚠️ THE GET IS HELD OPEN UNTIL EVERY CALLER HAS JOINED, observed through
 * the proxy's own count rather than a sleep, so "concurrent" is a state the
 * test builds rather than a timing it hopes for.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class SegmentProxyInFlightTest {

    private static final String KEY = "bins/c/data/in-flight.bseg";
    private static final int CALLERS = 8;
    private static final String STORE_FAILURE = "the store failed mid-fan-in";

    private static byte[] segment() {
        byte[] b = new byte[10_001];
        new Random(63).nextBytes(b);
        return b;
    }

    /** A store whose GET waits on {@code release}, counting GETs, failing while {@code fail}. */
    private static BinStore gated(MemoryBinStore memory, CountDownLatch release,
            AtomicInteger gets, AtomicBoolean fail) {
        return (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (self, method, args) -> {
                    if (method.getName().equals("get")) {
                        gets.incrementAndGet();
                        release.await();
                        if (fail.get()) {
                            throw new IOException(STORE_FAILURE);
                        }
                    }
                    try {
                        return method.invoke(memory, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static final class Collecting implements SegmentSink {
        final ByteArrayOutputStream got = new ByteArrayOutputStream();

        @Override
        public synchronized void write(byte[] buffer, int offset, int length) {
            got.write(buffer, offset, length);
        }
    }

    private static CompletableFuture<Collecting> call(SegmentProxy proxy, SegmentSink extra) {
        return CompletableFuture.supplyAsync(() -> {
            Collecting sink = new Collecting();
            try {
                proxy.streamTo(KEY, extra == null ? List.of(sink) : List.of(extra, sink));
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
            return sink;
        }, Executors.newVirtualThreadPerTaskExecutor());
    }

    private static List<CompletableFuture<Collecting>> race(SegmentProxy proxy, int callers) {
        List<CompletableFuture<Collecting>> calls = new ArrayList<>();
        for (int i = 0; i < callers; i++) {
            calls.add(call(proxy, null));
        }
        return calls;
    }

    private static void awaitJoiners(SegmentProxy proxy, int joiners) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (proxy.joinersOf(KEY) < joiners && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(proxy.joinersOf(KEY)).as("every other caller joined the one read")
                .isEqualTo(joiners);
    }

    private static void awaitGets(AtomicInteger gets, int n) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (gets.get() < n && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertThat(gets.get()).isEqualTo(n);
    }

    private static boolean causedByTheStore(Throwable t) {
        for (Throwable c = t; c != null; c = c.getCause()) {
            if (c.getMessage() != null && c.getMessage().contains(STORE_FAILURE)) {
                return true;
            }
        }
        return false;
    }

    @Test
    void concurrentColdCallersShareOneGetAndIdenticalBytes() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        byte[] segment = segment();
        memory.put(KEY, Body.ofBytes(segment));
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger gets = new AtomicInteger();
        SegmentProxy proxy = new SegmentProxy(gated(memory, release, gets, new AtomicBoolean()),
                1024, SegmentCache.forSegmentsOf(1 << 20));

        List<CompletableFuture<Collecting>> calls = race(proxy, CALLERS);
        awaitJoiners(proxy, CALLERS - 1);
        release.countDown();

        for (CompletableFuture<Collecting> call : calls) {
            assertThat(call.get(30, TimeUnit.SECONDS).got.toByteArray()).isEqualTo(segment);
        }
        assertThat(gets.get()).as("⚠️ ONE GET for %d concurrent callers", CALLERS)
                .isEqualTo(1);
        assertThat(proxy.inFlight(KEY)).as("the in-flight entry is gone").isFalse();
    }

    @Test
    void aFailedReadFailsEveryJoinerWithTheStoresFailureAndLeavesTheKeyReadable()
            throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        byte[] segment = segment();
        memory.put(KEY, Body.ofBytes(segment));
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger gets = new AtomicInteger();
        AtomicBoolean fail = new AtomicBoolean(true);
        SegmentProxy proxy = new SegmentProxy(gated(memory, release, gets, fail), 1024,
                SegmentCache.forSegmentsOf(1 << 20));

        List<CompletableFuture<Collecting>> calls = race(proxy, CALLERS);
        awaitJoiners(proxy, CALLERS - 1);
        release.countDown();

        for (CompletableFuture<Collecting> call : calls) {
            try {
                call.get(30, TimeUnit.SECONDS);
                throw new AssertionError("a caller of a failed read succeeded");
            } catch (ExecutionException e) {
                assertThat(causedByTheStore(e))
                        .as("the winner's failure is every joiner's, not some other one")
                        .isTrue();
            }
        }
        assertThat(gets.get()).isEqualTo(1);
        assertThat(proxy.cache().get(KEY)).as("nothing cached from a failed read").isNull();
        assertThat(proxy.inFlight(KEY)).isFalse();

        fail.set(false);
        assertThat(call(proxy, null).get(30, TimeUnit.SECONDS).got.toByteArray())
                .as("⚠️ ONE BLIP IS NOT PERMANENT: the next caller reads again")
                .isEqualTo(segment);
        assertThat(gets.get()).isEqualTo(2);
    }

    @Test
    void aCallerWhoseClaimFollowsAFinishedReadIsServedFromTheCache() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        byte[] segment = segment();
        memory.put(KEY, Body.ofBytes(segment));
        AtomicInteger gets = new AtomicInteger();
        SegmentProxy proxy = new SegmentProxy(gated(memory, new CountDownLatch(0), gets,
                new AtomicBoolean()), 1024, SegmentCache.forSegmentsOf(1 << 20));
        AtomicBoolean once = new AtomicBoolean();
        // ⚠️ THE WINDOW, BUILT: this caller has missed the cache; before it
        // claims the key, another read runs to completion, fills the cache
        // and leaves.
        proxy.betweenMissAndClaim = () -> {
            if (once.compareAndSet(false, true)) {
                try {
                    proxy.streamTo(KEY, List.of(new Collecting()));
                } catch (IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            }
        };
        Collecting sink = new Collecting();

        proxy.streamTo(KEY, List.of(sink));

        assertThat(once.get()).as("PREMISE: the window between miss and claim was exercised")
                .isTrue();
        assertThat(sink.got.toByteArray()).isEqualTo(segment);
        assertThat(gets.get()).as("the late claimant re-checks the cache: one GET, not two")
                .isEqualTo(1);
    }

    @Test
    void aSegmentLargerThanTheCacheIsStillSharedByCallersAlreadyAttached() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        byte[] segment = segment();
        memory.put(KEY, Body.ofBytes(segment));
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger gets = new AtomicInteger();
        SegmentProxy proxy = new SegmentProxy(gated(memory, release, gets, new AtomicBoolean()),
                1024, new SegmentCache(4_096));

        List<CompletableFuture<Collecting>> calls = race(proxy, CALLERS);
        awaitJoiners(proxy, CALLERS - 1);
        release.countDown();

        for (CompletableFuture<Collecting> call : calls) {
            assertThat(call.get(30, TimeUnit.SECONDS).got.toByteArray())
                    .as("every caller gets the WHOLE segment, not the admitted prefix")
                    .isEqualTo(segment);
        }
        assertThat(gets.get()).as("attached before the cache refused it: still one GET")
                .isEqualTo(1);
        assertThat(proxy.cache().get(KEY)).as("and too large to keep").isNull();
    }

    @Test
    void anErrorInTheWinnerFailsItsJoinersRatherThanStrandingThem() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        memory.put(KEY, Body.ofBytes(segment()));
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger gets = new AtomicInteger();
        SegmentProxy proxy = new SegmentProxy(gated(memory, release, gets, new AtomicBoolean()),
                1024, SegmentCache.forSegmentsOf(1 << 20));
        SegmentSink erring = (buffer, offset, length) -> {
            throw new AssertionError("an Error, which no sink loop catches");
        };

        CompletableFuture<Collecting> winner = call(proxy, erring);
        awaitGets(gets, 1);
        List<CompletableFuture<Collecting>> joiners = race(proxy, CALLERS - 1);
        awaitJoiners(proxy, CALLERS - 1);
        release.countDown();

        for (CompletableFuture<Collecting> joiner : joiners) {
            try {
                joiner.get(30, TimeUnit.SECONDS);
                throw new AssertionError("a joiner of a read that died succeeded");
            } catch (ExecutionException expected) {
                assertThat(expected.getCause()).isInstanceOf(java.io.UncheckedIOException.class);
            }
        }
        try {
            winner.get(30, TimeUnit.SECONDS);
            throw new IllegalStateException("the caller whose sink threw an Error succeeded");
        } catch (ExecutionException expected) {
            assertThat(expected.getCause()).isInstanceOf(AssertionError.class);
        }
        assertThat(proxy.inFlight(KEY)).isFalse();
    }

    /**
     * A store whose FIRST stream pauses once, after {@code pauseAfter} bytes,
     * until {@code resume} opens -- so a caller can arrive with a real prefix
     * already read. Every later stream is served straight through.
     */
    private static BinStore pausing(MemoryBinStore memory, int pauseAfter, CountDownLatch paused,
            CountDownLatch resume, AtomicInteger gets) {
        return (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (self, method, args) -> {
                    Object result;
                    try {
                        result = method.invoke(memory, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                    if (!method.getName().equals("get")) {
                        return result;
                    }
                    boolean first = gets.incrementAndGet() == 1;
                    java.io.InputStream in = (java.io.InputStream) result;
                    if (!first) {
                        return in;
                    }
                    return new java.io.InputStream() {
                        private int served;
                        private boolean waited;

                        @Override
                        public int read() throws IOException {
                            byte[] one = new byte[1];
                            return read(one, 0, 1) < 0 ? -1 : one[0] & 0xff;
                        }

                        @Override
                        public int read(byte[] b, int off, int len) throws IOException {
                            if (served >= pauseAfter && !waited) {
                                waited = true;
                                paused.countDown();
                                try {
                                    resume.await();
                                } catch (InterruptedException e) {
                                    Thread.currentThread().interrupt();
                                    throw new IOException(e);
                                }
                            }
                            int cap = served < pauseAfter ? Math.min(len, pauseAfter - served) : len;
                            int n = in.read(b, off, cap);
                            if (n > 0) {
                                served += n;
                            }
                            return n;
                        }
                    };
                });
    }

    /** A caller on its own thread, answering what its sinks got and how many were whole. */
    private record Outcome(Collecting sink, int whole) {
    }

    private static CompletableFuture<Outcome> callOn(SegmentProxy proxy, SegmentSink extra,
            java.util.concurrent.atomic.AtomicReference<Thread> thread) {
        return callOn(proxy, new Collecting(), extra, thread, null);
    }

    private static CompletableFuture<Outcome> callOn(SegmentProxy proxy, Collecting sink,
            SegmentSink extra, java.util.concurrent.atomic.AtomicReference<Thread> thread,
            AtomicBoolean interruptedAfter) {
        CompletableFuture<Outcome> outcome = new CompletableFuture<>();
        Thread t = Thread.ofVirtual().unstarted(() -> {
            try {
                int whole = proxy.streamTo(KEY, extra == null ? List.of(sink) : List.of(sink, extra));
                outcome.complete(new Outcome(sink, whole));
            } catch (Throwable e) {
                if (interruptedAfter != null) {
                    interruptedAfter.set(Thread.currentThread().isInterrupted());
                }
                outcome.completeExceptionally(e);
            }
        });
        if (thread != null) {
            thread.set(t);
        }
        t.start();
        return outcome;
    }

    private static void await(CountDownLatch latch) throws InterruptedException {
        assertThat(latch.await(30, TimeUnit.SECONDS)).as("reached in time").isTrue();
    }

    @Test
    void aJoinerArrivingMidReadCatchesUpFromThePrefixAndCountsItsWholeSinks() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        byte[] segment = segment();
        memory.put(KEY, Body.ofBytes(segment));
        CountDownLatch paused = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        AtomicInteger gets = new AtomicInteger();
        SegmentProxy proxy = new SegmentProxy(pausing(memory, 3_072, paused, resume, gets), 1024,
                SegmentCache.forSegmentsOf(1 << 20));
        int[] writes = {0};
        SegmentSink diesOnItsThirdChunk = (buffer, offset, length) -> {
            // ⚠️ THE FIFTH WRITE: the prefix is three chunks, so this sink is
            // ATTACHED when it dies and is dropped by the relay, which is the
            // count the identity comparison exists for.
            if (++writes[0] == 5) {
                throw new IOException("this consumer went away");
            }
        };

        CompletableFuture<Outcome> first = callOn(proxy, null, null);
        await(paused);
        CompletableFuture<Outcome> joiner = callOn(proxy, diesOnItsThirdChunk, null);
        awaitJoiners(proxy, 1);
        resume.countDown();

        assertThat(first.get(30, TimeUnit.SECONDS).sink().got.toByteArray()).isEqualTo(segment);
        Outcome joined = joiner.get(30, TimeUnit.SECONDS);
        assertThat(joined.sink().got.toByteArray())
                .as("⚠️ THE 3,072-BYTE PREFIX, THEN THE STREAM: whole, not truncated")
                .isEqualTo(segment);
        assertThat(joined.whole()).as("its sink that died is not counted whole").isEqualTo(1);
        assertThat(gets.get()).isEqualTo(1);
    }

    @Test
    void aJoinerArrivingAfterTheReadCompletedTakesTheWholeSegment() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        byte[] segment = segment();
        memory.put(KEY, Body.ofBytes(segment));
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger gets = new AtomicInteger();
        SegmentProxy proxy = new SegmentProxy(gated(memory, release, gets, new AtomicBoolean()),
                1024, SegmentCache.forSegmentsOf(1 << 20));
        var firstThread = new java.util.concurrent.atomic.AtomicReference<Thread>();
        var lateThread = new java.util.concurrent.atomic.AtomicReference<Thread>();
        CountDownLatch completed = new CountDownLatch(1);
        CountDownLatch lateReturned = new CountDownLatch(1);
        proxy.betweenCompleteAndRemove = () -> {
            if (Thread.currentThread() == firstThread.get()) {
                completed.countDown();
                try {
                    lateReturned.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
        proxy.betweenMissAndClaim = () -> {
            if (Thread.currentThread() == lateThread.get()) {
                try {
                    completed.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };

        CompletableFuture<Outcome> first = callOn(proxy, null, firstThread);
        awaitGets(gets, 1);
        CompletableFuture<Outcome> late = callOn(proxy, null, lateThread);
        release.countDown();
        Outcome latecomer = late.get(30, TimeUnit.SECONDS);
        lateReturned.countDown();

        assertThat(latecomer.sink().got.toByteArray())
                .as("it missed the cache before the fill and found the entry done: the WHOLE "
                        + "prefix, not nothing")
                .isEqualTo(segment);
        assertThat(latecomer.whole()).isEqualTo(1);
        assertThat(first.get(30, TimeUnit.SECONDS).sink().got.toByteArray()).isEqualTo(segment);
        assertThat(gets.get()).as("the entry was still there: no second read").isEqualTo(1);
    }

    @Test
    void aJoinerOfALateClaimIsServedTheWholeCachedSegment() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        byte[] segment = segment();
        memory.put(KEY, Body.ofBytes(segment));
        AtomicInteger gets = new AtomicInteger();
        SegmentProxy proxy = new SegmentProxy(gated(memory, new CountDownLatch(0), gets,
                new AtomicBoolean()), 1024, SegmentCache.forSegmentsOf(1 << 20));
        var claimant = new java.util.concurrent.atomic.AtomicReference<Thread>();
        var joiner = new java.util.concurrent.atomic.AtomicReference<Thread>();
        CountDownLatch joinerMissed = new CountDownLatch(1);
        CountDownLatch claimed = new CountDownLatch(1);
        AtomicBoolean innerDone = new AtomicBoolean();
        AtomicBoolean insideInner = new AtomicBoolean();
        proxy.betweenMissAndClaim = () -> {
            try {
                if (Thread.currentThread() == joiner.get()) {
                    joinerMissed.countDown();
                    claimed.await(30, TimeUnit.SECONDS);
                } else if (Thread.currentThread() == claimant.get()
                        && innerDone.compareAndSet(false, true)) {
                    // another read runs to completion, fills the cache and leaves
                    insideInner.set(true);
                    try {
                        proxy.streamTo(KEY, List.of(new Collecting()));
                    } finally {
                        insideInner.set(false);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        };
        proxy.betweenClaimAndRecheck = () -> {
            if (Thread.currentThread() == claimant.get() && !insideInner.get()) {
                claimed.countDown();
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                while (proxy.joinersOf(KEY) < 1 && System.nanoTime() < deadline) {
                    Thread.onSpinWait();
                }
            }
        };

        CompletableFuture<Outcome> joined = callOn(proxy, null, joiner);
        await(joinerMissed);
        CompletableFuture<Outcome> claim = callOn(proxy, null, claimant);

        assertThat(claim.get(30, TimeUnit.SECONDS).sink().got.toByteArray()).isEqualTo(segment);
        assertThat(joined.get(30, TimeUnit.SECONDS).sink().got.toByteArray())
                .as("⚠️ ATTACHED TO A CLAIM THAT FOUND THE CACHE FULL: relayed the whole "
                        + "cached segment, not left with nothing")
                .isEqualTo(segment);
        assertThat(gets.get()).as("one read: the one that filled the cache").isEqualTo(1);
    }

    @Test
    void aCallerArrivingAfterTheCacheRefusedTheSegmentReadsForItself() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        byte[] segment = segment();
        memory.put(KEY, Body.ofBytes(segment));
        CountDownLatch paused = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        AtomicInteger gets = new AtomicInteger();
        SegmentProxy proxy = new SegmentProxy(pausing(memory, 5_120, paused, resume, gets), 1024,
                new SegmentCache(4_096));

        CompletableFuture<Outcome> first = callOn(proxy, null, null);
        await(paused);
        Outcome latecomer = callOn(proxy, null, null).get(30, TimeUnit.SECONDS);
        resume.countDown();

        assertThat(latecomer.sink().got.toByteArray())
                .as("no prefix to take once the cache refused it: its own read, whole")
                .isEqualTo(segment);
        assertThat(first.get(30, TimeUnit.SECONDS).sink().got.toByteArray()).isEqualTo(segment);
        assertThat(gets.get()).isEqualTo(2);
    }

    @Test
    void anInterruptedJoinerIsDetachedAndWrittenNothingMore() throws Exception {
        MemoryBinStore memory = new MemoryBinStore();
        byte[] segment = segment();
        memory.put(KEY, Body.ofBytes(segment));
        CountDownLatch paused = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        AtomicInteger gets = new AtomicInteger();
        SegmentProxy proxy = new SegmentProxy(pausing(memory, 2_048, paused, resume, gets), 1024,
                SegmentCache.forSegmentsOf(1 << 20));
        var joinerThread = new java.util.concurrent.atomic.AtomicReference<Thread>();

        CompletableFuture<Outcome> first = callOn(proxy, null, null);
        await(paused);
        Collecting joinersSink = new Collecting();
        AtomicBoolean stillInterrupted = new AtomicBoolean();
        CompletableFuture<Outcome> joiner = callOn(proxy, joinersSink, null, joinerThread,
                stillInterrupted);
        awaitJoiners(proxy, 1);
        joinerThread.get().interrupt();
        try {
            joiner.get(30, TimeUnit.SECONDS);
            throw new IllegalStateException("an interrupted joiner succeeded");
        } catch (ExecutionException expected) {
            assertThat(expected.getCause()).isInstanceOf(java.io.IOException.class);
        }
        int beforeResume = joinersSink.got.size();
        assertThat(stillInterrupted.get()).as("the interrupt is restored, not swallowed")
                .isTrue();
        resume.countDown();

        assertThat(first.get(30, TimeUnit.SECONDS).sink().got.toByteArray()).isEqualTo(segment);
        assertThat(joinersSink.got.size())
                .as("⚠️ DETACHED: a caller that already threw is written nothing more")
                .isEqualTo(beforeResume);
        assertThat(proxy.joinersOf(KEY)).isZero();
    }
}
