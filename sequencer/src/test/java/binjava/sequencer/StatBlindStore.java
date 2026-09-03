// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import java.io.IOException;
import java.util.Optional;

/**
 * ⚠️ Makes every {@code stat} report the key as ABSENT, so two managers can
 * be driven down the FIRST-ACQUISITION path deterministically. Without it
 * no test ever reaches a LOSING acquisition CAS -- the second contender is
 * refused by the expiry check long before it gets there -- which left
 * `putIfAbsent` replaceable by an unconditional `put` with the whole suite
 * green. Two ingesters starting against a fresh prefix would then both be
 * told they hold epoch 0.
 */
public record StatBlindStore(BinStore delegate) implements BinStore {
    @Override public Optional<binjava.binstore.ObjectStat> stat(String key) {
        return Optional.empty();
    }

    @Override public java.io.InputStream get(String k) throws IOException {
        return delegate.get(k);
    }

    @Override public java.io.InputStream getRange(String k, long a, long b)
            throws IOException {
        return delegate.getRange(k, a, b);
    }

    @Override public binjava.binstore.Version put(String k, Body b) throws IOException {
        return delegate.put(k, b);
    }

    @Override public Optional<binjava.binstore.Version> putIfAbsent(String k, Body b)
            throws IOException {
        return delegate.putIfAbsent(k, b);
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
