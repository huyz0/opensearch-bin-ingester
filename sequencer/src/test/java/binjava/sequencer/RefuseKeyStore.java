// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import java.io.IOException;
import java.util.Optional;

/**
 * ⚠️ Refuses {@code putIfAbsent} for ONE key, so a failure can be placed at an
 * exact point in a multi-step operation.
 *
 * <p>Becoming the leader is three writes — acquire, seal, open — and the
 * dangerous failures are the ones BETWEEN them, where the lease is held and the
 * chain is half-built. A store that fails randomly reaches those only by luck;
 * this one puts the failure exactly where the test means it.
 */
public record RefuseKeyStore(BinStore delegate, String refusedKey) implements BinStore {

    @Override public Optional<binjava.binstore.Version> putIfAbsent(String k, Body b)
            throws IOException {
        if (k.equals(refusedKey)) {
            throw new IOException("injected: the store refused " + k);
        }
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

    @Override public Optional<binjava.binstore.Version> putIfMatch(String k, Body b,
            binjava.binstore.Version v) throws IOException {
        return delegate.putIfMatch(k, b, v);
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
