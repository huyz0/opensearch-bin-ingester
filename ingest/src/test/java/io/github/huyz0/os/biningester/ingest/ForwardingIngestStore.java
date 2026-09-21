// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import java.util.List;

/**
 * Everything the delegate does, so a case can override ONE verb.
 *
 * <p>⚠️ A TEST FIXTURE, NOT A MOCK. Each case that needs one verb to fail
 * overrides exactly that verb, and every other request still reaches a real
 * {@code MemoryBinStore}.
 */
abstract class ForwardingIngestStore implements BinStore {
    private final BinStore delegate;

    ForwardingIngestStore(BinStore delegate) {
        this.delegate = delegate;
    }

    @Override
    public java.io.InputStream get(String key) throws java.io.IOException {
        return delegate.get(key);
    }

    @Override
    public java.io.InputStream getRange(String key, long start, long endIncl)
            throws java.io.IOException {
        return delegate.getRange(key, start, endIncl);
    }

    @Override
    public java.util.Optional<io.github.huyz0.os.biningester.binstore.ObjectStat> stat(String key)
            throws java.io.IOException {
        return delegate.stat(key);
    }

    @Override
    public io.github.huyz0.os.biningester.binstore.Version put(String key, Body body) throws java.io.IOException {
        return delegate.put(key, body);
    }

    @Override
    public java.util.Optional<io.github.huyz0.os.biningester.binstore.Version> putIfAbsent(String key, Body body)
            throws java.io.IOException {
        return delegate.putIfAbsent(key, body);
    }

    @Override
    public java.util.Optional<io.github.huyz0.os.biningester.binstore.Version> putIfMatch(String key, Body body,
            io.github.huyz0.os.biningester.binstore.Version expected) throws java.io.IOException {
        return delegate.putIfMatch(key, body, expected);
    }

    @Override
    public io.github.huyz0.os.biningester.binstore.MultipartWriter multipart(String key) throws java.io.IOException {
        return delegate.multipart(key);
    }

    @Override
    public io.github.huyz0.os.biningester.binstore.ListPage list(String prefix, String startAfter, int maxKeys)
            throws java.io.IOException {
        return delegate.list(prefix, startAfter, maxKeys);
    }

    @Override
    public void delete(List<String> keys) throws java.io.IOException {
        delegate.delete(keys);
    }

    @Override
    public io.github.huyz0.os.biningester.binstore.Capabilities capabilities() {
        return delegate.capabilities();
    }

    @Override
    public void close() throws java.io.IOException {
        delegate.close();
    }
    }

