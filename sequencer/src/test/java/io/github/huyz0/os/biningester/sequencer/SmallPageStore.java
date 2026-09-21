// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import java.io.IOException;
import java.util.Optional;

/**
 * ⚠️ Caps every {@code list} page, so PAGINATION happens with a handful of
 * objects instead of a thousand.
 *
 * <p>Without it, {@code recover()}'s early {@code return} at a SEAL and a mere
 * {@code break} are INDISTINGUISHABLE: every fixture in the suite holds fewer
 * objects than the 1000 {@code CommitLog} asks for, so {@code nextStartAfter}
 * is always empty and the outer loop never runs twice. Measured — the mutation
 * survived the whole sequencer suite. On a chain long enough to page, it
 * applies the
 * discarded suffix beyond the seal, which is I3, and spends a second LIST doing
 * it.
 *
 * <p>The alternative was a fixture of 1100 objects, which buys the same
 * discrimination at a much worse cost per run.
 */
public record SmallPageStore(BinStore delegate, int pageSize) implements BinStore {

    @Override public io.github.huyz0.os.biningester.binstore.ListPage list(String p, String a, int m)
            throws IOException {
        return delegate.list(p, a, Math.min(m, pageSize));
    }

    @Override public Optional<io.github.huyz0.os.biningester.binstore.ObjectStat> stat(String k) throws IOException {
        return delegate.stat(k);
    }

    @Override public java.io.InputStream get(String k) throws IOException {
        return delegate.get(k);
    }

    @Override public java.io.InputStream getRange(String k, long a, long b)
            throws IOException {
        return delegate.getRange(k, a, b);
    }

    @Override public io.github.huyz0.os.biningester.binstore.Version put(String k, Body b) throws IOException {
        return delegate.put(k, b);
    }

    @Override public Optional<io.github.huyz0.os.biningester.binstore.Version> putIfAbsent(String k, Body b)
            throws IOException {
        return delegate.putIfAbsent(k, b);
    }

    @Override public Optional<io.github.huyz0.os.biningester.binstore.Version> putIfMatch(String k, Body b,
            io.github.huyz0.os.biningester.binstore.Version v) throws IOException {
        return delegate.putIfMatch(k, b, v);
    }

    @Override public io.github.huyz0.os.biningester.binstore.MultipartWriter multipart(String k) throws IOException {
        return delegate.multipart(k);
    }

    @Override public void delete(java.util.List<String> keys) throws IOException {
        delegate.delete(keys);
    }

    @Override public io.github.huyz0.os.biningester.binstore.Capabilities capabilities() {
        return delegate.capabilities();
    }

    @Override public io.github.huyz0.os.biningester.binstore.SignedUrl presign(String key, java.time.Duration ttl)
            throws java.io.IOException {
        // ⚠️ FORWARDED because a `default` method on the SPI cannot force a
        // decorator to do it, and review MEASURED that forgetting it makes a
        // capable backend pass the startup check and throw at first fetch.
        return delegate.presign(key, ttl);
    }

    @Override public void close() throws IOException {
        delegate.close();
    }
}
