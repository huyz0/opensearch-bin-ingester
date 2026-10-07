// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;

import io.github.huyz0.os.biningester.binstore.BinStore;
import java.io.IOException;

/** A store whose inbox reads fail while {@code failing} is set, counting them. */
final class FailingInboxStore implements BinStore {
    private final BinStore inner;
    volatile boolean failing;
    final java.util.concurrent.atomic.AtomicInteger inboxGets =
            new java.util.concurrent.atomic.AtomicInteger();

    FailingInboxStore(BinStore inner) {
        this.inner = inner;
    }

    @Override
    public java.io.InputStream get(String key) throws IOException {
        if (key.startsWith(Inbox.prefixFor(PREFIX))) {
            inboxGets.incrementAndGet();
        }
        if (failing && key.startsWith(Inbox.prefixFor(PREFIX))) {
            throw new IOException("the inbox read failed");
        }
        return inner.get(key);
    }

    @Override
    public java.io.InputStream getRange(String key, long start, long endIncl)
            throws IOException {
        return inner.getRange(key, start, endIncl);
    }

    @Override
    public java.util.Optional<io.github.huyz0.os.biningester.binstore.ObjectStat> stat(
            String key) throws IOException {
        return inner.stat(key);
    }

    @Override
    public io.github.huyz0.os.biningester.binstore.Version put(String key,
            io.github.huyz0.os.biningester.binstore.Body body) throws IOException {
        return inner.put(key, body);
    }

    @Override
    public java.util.Optional<io.github.huyz0.os.biningester.binstore.Version> putIfAbsent(
            String key, io.github.huyz0.os.biningester.binstore.Body body) throws IOException {
        return inner.putIfAbsent(key, body);
    }

    @Override
    public java.util.Optional<io.github.huyz0.os.biningester.binstore.Version> putIfMatch(
            String key, io.github.huyz0.os.biningester.binstore.Body body,
            io.github.huyz0.os.biningester.binstore.Version expected) throws IOException {
        return inner.putIfMatch(key, body, expected);
    }

    @Override
    public io.github.huyz0.os.biningester.binstore.MultipartWriter multipart(String key)
            throws IOException {
        return inner.multipart(key);
    }

    @Override
    public io.github.huyz0.os.biningester.binstore.ListPage list(String prefix,
            String startAfter, int maxKeys) throws IOException {
        return inner.list(prefix, startAfter, maxKeys);
    }

    @Override
    public void delete(java.util.List<String> keys) throws IOException {
        inner.delete(keys);
    }

    @Override
    public io.github.huyz0.os.biningester.binstore.Capabilities capabilities() {
        return inner.capabilities();
    }

    @Override
    public void close() throws IOException {
        inner.close();
    }
}
