// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ⚠️ Fails {@code putIfMatch} for ONE key while a toggle is set, so a renew can
 * be made to throw and then recover.
 *
 * <p>{@code putIfMatch} is the write every lease operation uses (ADR-0008), and
 * {@link RefuseKeyStore} does not cover it — that one refuses
 * {@code putIfAbsent}, which is the commit chain's write and the lease's
 * cold-start acquire, never a renew.
 *
 * <p>⚠️ THE TOGGLE IS THE POINT, not the failure. An IOException out of
 * {@code renew} is TRANSIENT: {@link LeaseManager#renew()} distinguishes it from
 * an empty result, which alone means fenced. So a test has to show the leader
 * surviving the failure AND renewing again afterwards, and a store that fails
 * forever can only show the first half.
 */
public final class FailPutIfMatchStore implements BinStore {

    private final BinStore delegate;
    private final String failedKey;
    private final AtomicBoolean failing = new AtomicBoolean();

    public FailPutIfMatchStore(BinStore delegate, String failedKey) {
        this.delegate = delegate;
        this.failedKey = failedKey;
    }

    /** Makes the next {@code putIfMatch} for the key throw, until cleared. */
    public void startFailing() {
        failing.set(true);
    }

    public void stopFailing() {
        failing.set(false);
    }

    @Override public Optional<binjava.binstore.Version> putIfMatch(String k, Body b,
            binjava.binstore.Version v) throws IOException {
        if (failing.get() && k.equals(failedKey)) {
            throw new IOException("injected: the store refused putIfMatch for " + k);
        }
        return delegate.putIfMatch(k, b, v);
    }

    @Override public Optional<binjava.binstore.Version> putIfAbsent(String k, Body b)
            throws IOException {
        return delegate.putIfAbsent(k, b);
    }

    @Override public Optional<binjava.binstore.ObjectStat> stat(String k) throws IOException {
        return delegate.stat(k);
    }

    @Override public java.io.InputStream get(String k) throws IOException {
        return delegate.get(k);
    }

    @Override public java.io.InputStream getRange(String k, long a, long b) throws IOException {
        return delegate.getRange(k, a, b);
    }

    @Override public binjava.binstore.Version put(String k, Body b) throws IOException {
        return delegate.put(k, b);
    }

    @Override public binjava.binstore.MultipartWriter multipart(String k) throws IOException {
        return delegate.multipart(k);
    }

    @Override public binjava.binstore.ListPage list(String p, String a, int m)
            throws IOException {
        return delegate.list(p, a, m);
    }

    @Override public void delete(java.util.List<String> keys) throws IOException {
        delegate.delete(keys);
    }

    @Override public binjava.binstore.Capabilities capabilities() {
        return delegate.capabilities();
    }

    @Override public void close() throws IOException {
        delegate.close();
    }
}
