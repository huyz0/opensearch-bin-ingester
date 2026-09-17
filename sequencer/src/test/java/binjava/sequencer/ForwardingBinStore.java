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
 * Everything the delegate does, so a case can override ONE verb.
 *
 * <p>⚠️ A TEST FIXTURE, NOT A MOCK. Each case that needs one verb to fail
 * overrides exactly that verb, and every other request still reaches a real
 * {@code MemoryBinStore} — so a case about a failing DELETE is still a case
 * about a store that otherwise behaves.
 */
abstract class ForwardingBinStore implements BinStore {

    private final BinStore delegate;

    ForwardingBinStore(BinStore delegate) {
        this.delegate = delegate;
    }

    @Override
    public InputStream get(String key) throws IOException {
        return delegate.get(key);
    }

    @Override
    public InputStream getRange(String key, long start, long endIncl) throws IOException {
        return delegate.getRange(key, start, endIncl);
    }

    @Override
    public Optional<ObjectStat> stat(String key) throws IOException {
        return delegate.stat(key);
    }

    @Override
    public Version put(String key, Body body) throws IOException {
        return delegate.put(key, body);
    }

    @Override
    public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
        return delegate.putIfAbsent(key, body);
    }

    @Override
    public Optional<Version> putIfMatch(String key, Body body, Version expected)
            throws IOException {
        return delegate.putIfMatch(key, body, expected);
    }

    @Override
    public MultipartWriter multipart(String key) throws IOException {
        return delegate.multipart(key);
    }

    @Override
    public ListPage list(String prefix, String startAfter, int maxKeys) throws IOException {
        return delegate.list(prefix, startAfter, maxKeys);
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
