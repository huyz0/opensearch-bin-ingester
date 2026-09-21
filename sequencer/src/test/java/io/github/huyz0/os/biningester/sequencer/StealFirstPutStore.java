// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ⚠️ Makes a takeover LOSE its first {@code putIfAbsent}, exactly once.
 *
 * <p>Without it no test in this module ever seals a CONTENDED predecessor: every
 * takeover here follows a voluntary {@code close()} against a frozen clock, so
 * the old leader is already gone and the seal always wins its first attempt.
 * Measured — with no contention anywhere, {@code seal}'s entire redrive loop
 * could be deleted and every {@code LocalSequencerTest} stayed green, which is
 * the scenario M4.6b and M4.6c exist for going untested by both of them.
 *
 * <p>The steal writes {@code thief} to the contested key first, so the real
 * caller loses to bytes a fenced leader could plausibly have written — a delta
 * still in flight when its lease went away.
 */
public record StealFirstPutStore(BinStore delegate, String contestedKey, byte[] thief,
        AtomicInteger steals) implements BinStore {

    public StealFirstPutStore(BinStore delegate, String contestedKey, byte[] thief) {
        this(delegate, contestedKey, thief, new AtomicInteger());
    }

    @Override public Optional<io.github.huyz0.os.biningester.binstore.Version> putIfAbsent(String k, Body b)
            throws IOException {
        if (k.equals(contestedKey) && steals.compareAndSet(0, 1)) {
            delegate.putIfAbsent(k, new Body(thief.length, () -> new ByteArrayInputStream(thief)));
        }
        return delegate.putIfAbsent(k, b);
    }

    @Override public Optional<io.github.huyz0.os.biningester.binstore.ObjectStat> stat(String k) throws IOException {
        return delegate.stat(k);
    }

    @Override public java.io.InputStream get(String k) throws IOException {
        return delegate.get(k);
    }

    @Override public java.io.InputStream getRange(String k, long a, long b) throws IOException {
        return delegate.getRange(k, a, b);
    }

    @Override public io.github.huyz0.os.biningester.binstore.Version put(String k, Body b) throws IOException {
        return delegate.put(k, b);
    }

    @Override public Optional<io.github.huyz0.os.biningester.binstore.Version> putIfMatch(String k, Body b,
            io.github.huyz0.os.biningester.binstore.Version v) throws IOException {
        return delegate.putIfMatch(k, b, v);
    }

    @Override public io.github.huyz0.os.biningester.binstore.MultipartWriter multipart(String k) throws IOException {
        return delegate.multipart(k);
    }

    @Override public io.github.huyz0.os.biningester.binstore.ListPage list(String p, String a, int m)
            throws IOException {
        return delegate.list(p, a, m);
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
