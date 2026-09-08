// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Blocks the FIRST {@code putIfAbsent} until released, and passes every later
 * one straight through.
 *
 * <p>⚠️ THIS IS HOW A DETERMINISTIC INTERLEAVE IS BUILT AT T1, and it is worth
 * saying because an earlier draft of M4.3c claimed the opposite — that a T1
 * test with a {@code MemoryBinStore} could not make a JVM interleave
 * deterministically, and parked the property on M4.12 where the mutation could
 * not have died. Determinism does not come from controlling the scheduler; it
 * comes from parking a thread at a chosen point INSIDE the critical section,
 * using a store call as the blocking primitive. {@code StoreFakes.GatedCommit}
 * does the same thing in {@code ingest}, but it is in that module's plain test
 * sources — {@code ingest} depends on {@code sequencer}, so the direction
 * cannot be reversed — which is why this one lives here.
 *
 * <p>⚠️ Only the FIRST call gates. If every call blocked, the second thread
 * would park on the gate rather than on whatever the test is trying to observe,
 * and the interleave would be the one the fake imposed rather than the one the
 * code allows.
 *
 * <p>⚠️ WHICH call is chosen by {@link Target}, because the cold-start write
 * and the renew/release write are different methods and a test usually needs
 * to park inside one while a belief established by the other already exists.
 */
public final class GateFirstPutStore implements BinStore {

    /** Which conditional write to park inside. */
    public enum Target {
        /** The cold-start acquisition. */
        PUT_IF_ABSENT,
        /** Every later transition — takeover, renew, release. */
        PUT_IF_MATCH
    }

    private final BinStore delegate;
    private final Target target;
    private final CountDownLatch entered = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private volatile boolean gated;

    public GateFirstPutStore(BinStore delegate) {
        this(delegate, Target.PUT_IF_ABSENT);
    }

    public GateFirstPutStore(BinStore delegate, Target target) {
        this.delegate = delegate;
        this.target = target;
    }

    /** @return true if this call is the one that gates, and marks it taken. */
    private boolean claim(Target of) {
        if (of != target) {
            return false;
        }
        synchronized (this) {
            if (gated) {
                return false;
            }
            gated = true;
            return true;
        }
    }

    private void parkUntilReleased() throws IOException {
        entered.countDown();
        try {
            // ⚠️ Bounded so a test that fails before calling `release`
            // cannot leave this non-daemon thread parked forever, keeping
            // the worker JVM alive long after the failure was recorded.
            if (!release.await(10, TimeUnit.SECONDS)) {
                throw new IOException("gate was never released");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while gated", e);
        }
    }

    /**
     * Blocks until a thread is parked inside the first {@code putIfAbsent}.
     *
     * <p>⚠️ The message names {@link Target}, because a fixture that reports
     * the wrong method gives the wrong diagnosis in exactly the failure this
     * bounded wait exists to report clearly.
     *
     * <p>⚠️ BOUNDED, like every wait in this class. A mutation that stops
     * the code under test calling the gated method at all never counts the
     * latch down, and an unbounded wait would spend the whole ten-minute
     * Gradle task timeout and then report a timed-out TASK rather than a named
     * failing test.
     */
    public void awaitEntered() throws InterruptedException {
        if (!entered.await(10, TimeUnit.SECONDS)) {
            throw new AssertionError("no thread ever entered " + target);
        }
    }

    /** Lets the parked thread finish its conditional write. */
    public void release() {
        release.countDown();
    }

    @Override
    public Optional<binjava.binstore.Version> putIfAbsent(String key, Body body)
            throws IOException {
        if (claim(Target.PUT_IF_ABSENT)) {
            parkUntilReleased();
        }
        return delegate.putIfAbsent(key, body);
    }

    @Override
    public java.io.InputStream get(String k) throws IOException {
        return delegate.get(k);
    }

    @Override
    public java.io.InputStream getRange(String k, long a, long b) throws IOException {
        return delegate.getRange(k, a, b);
    }

    @Override
    public Optional<binjava.binstore.ObjectStat> stat(String k) throws IOException {
        return delegate.stat(k);
    }

    @Override
    public binjava.binstore.Version put(String k, Body b) throws IOException {
        return delegate.put(k, b);
    }

    @Override
    public Optional<binjava.binstore.Version> putIfMatch(String k, Body b,
            binjava.binstore.Version v) throws IOException {
        if (claim(Target.PUT_IF_MATCH)) {
            parkUntilReleased();
        }
        return delegate.putIfMatch(k, b, v);
    }

    @Override
    public binjava.binstore.MultipartWriter multipart(String k) throws IOException {
        return delegate.multipart(k);
    }

    @Override
    public binjava.binstore.ListPage list(String p, String a, int m) throws IOException {
        return delegate.list(p, a, m);
    }

    @Override
    public void delete(java.util.List<String> keys) throws IOException {
        delegate.delete(keys);
    }

    @Override
    public binjava.binstore.Capabilities capabilities() {
        return delegate.capabilities();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
