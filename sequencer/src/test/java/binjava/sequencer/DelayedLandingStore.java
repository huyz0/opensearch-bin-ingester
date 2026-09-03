// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.Capabilities;
import binjava.binstore.ListPage;
import binjava.binstore.MultipartWriter;
import binjava.binstore.ObjectStat;
import binjava.binstore.Version;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

/**
 * The write that was never lost, only LATE — the one case a single refresh
 * attempt cannot settle (M4.3h).
 *
 * <p>⚠️ {@link AmbiguousPutStore} models the two outcomes a caller can reason
 * about: the write landed before the response was lost, or it never landed at
 * all. This models the third, which is neither: the client's timeout fired
 * while the write was still queued inside the object store, and it takes effect
 * LATER — specifically, after a recovery read has already concluded the version
 * had not moved.
 *
 * <p>⚠️ The first {@code putIfMatch} throws while stashing its arguments, and
 * the SECOND applies the stash before delegating. That is exactly the
 * interleaving that defeats one refresh attempt: the refresh's {@code stat}
 * sees the old version, adopts it, clears the ambiguity flag, and the
 * conditional write that follows loses to this instance's own earlier bytes
 * with nothing left to say it might.
 */
public final class DelayedLandingStore implements BinStore {

    private final BinStore delegate;
    private int matchCalls;
    private String stashedKey;
    private byte[] stashedBody;
    private Version stashedExpected;

    public DelayedLandingStore(BinStore delegate) {
        this.delegate = delegate;
    }

    @Override
    public Optional<Version> putIfMatch(String key, Body body, Version expected)
            throws IOException {
        matchCalls++;
        if (matchCalls == 1) {
            stashedKey = key;
            stashedBody = body.readFully();
            stashedExpected = expected;
            throw new IOException("response lost; the write is still queued in the store");
        }
        if (stashedBody != null) {
            // ⚠️ Lands FIRST, so the caller's own conditional write is the one
            // that loses -- to bytes it wrote itself, one call earlier.
            byte[] late = stashedBody;
            stashedBody = null;
            if (delegate.putIfMatch(stashedKey, Body.ofBytes(late), stashedExpected).isEmpty()) {
                throw new IllegalStateException("the delayed write was meant to land, and lost");
            }
        }
        return delegate.putIfMatch(key, body, expected);
    }

    @Override
    public InputStream get(String k) throws IOException {
        return delegate.get(k);
    }

    @Override
    public InputStream getRange(String k, long a, long b) throws IOException {
        return delegate.getRange(k, a, b);
    }

    @Override
    public Optional<ObjectStat> stat(String k) throws IOException {
        return delegate.stat(k);
    }

    @Override
    public Version put(String k, Body b) throws IOException {
        return delegate.put(k, b);
    }

    @Override
    public Optional<Version> putIfAbsent(String k, Body b) throws IOException {
        return delegate.putIfAbsent(k, b);
    }

    @Override
    public MultipartWriter multipart(String k) throws IOException {
        return delegate.multipart(k);
    }

    @Override
    public ListPage list(String p, String a, int m) throws IOException {
        return delegate.list(p, a, m);
    }

    @Override
    public void delete(List<String> keys) throws IOException {
        delegate.delete(keys);
    }

    @Override
    public Capabilities capabilities() {
        return delegate.capabilities();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
