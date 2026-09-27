// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.Capabilities;
import io.github.huyz0.os.biningester.binstore.ListPage;
import io.github.huyz0.os.biningester.binstore.MultipartWriter;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.binstore.Version;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A memory store that reports what REACHED it, by key and by prefix (M10.11).
 *
 * <p>⚠️ **PER KEY AND PER PREFIX, NOT IN TOTAL**, because an assembled node has
 * background callers -- the lease renewer, the takeover inbox drain -- whose
 * requests land whenever they land. A total taken around a case's action
 * counts them too, and a case built on one fails when they happen to fall
 * inside its window. Counting the one key or prefix the case is about cannot.
 *
 * <p>⚠️ **AND IT SAYS WHEN THE STARTUP DRAIN's LIST HAS RETURNED**, so a case
 * that must measure the node's own counter can take its baseline after it.
 */
final class ObservedStore implements BinStore {

    private final MemoryBinStore delegate = new MemoryBinStore();
    private final String inboxPrefix;
    private final CountDownLatch inboxListed = new CountDownLatch(1);
    private final Map<String, AtomicLong> getsByKey = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> listsByPrefix = new ConcurrentHashMap<>();

    ObservedStore(String inboxPrefix) {
        this.inboxPrefix = inboxPrefix;
    }

    long getsOf(String key) {
        return getsByKey.getOrDefault(key, new AtomicLong()).get();
    }

    /** LISTs whose prefix starts with {@code prefix}. */
    long listsUnder(String prefix) {
        return listsByPrefix.entrySet().stream().filter(e -> e.getKey().startsWith(prefix))
                .mapToLong(e -> e.getValue().get()).sum();
    }

    /** Waits for the first LIST of the inbox to have RETURNED. */
    void awaitInboxListed() throws InterruptedException {
        if (!inboxListed.await(30, TimeUnit.SECONDS)) {
            throw new AssertionError("no LIST of " + inboxPrefix + " reached the store");
        }
    }

    @Override
    public InputStream get(String key) throws IOException {
        getsByKey.computeIfAbsent(key, k -> new AtomicLong()).incrementAndGet();
        return delegate.get(key);
    }

    @Override
    public InputStream getRange(String key, long start, long endIncl) throws IOException {
        getsByKey.computeIfAbsent(key, k -> new AtomicLong()).incrementAndGet();
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
        listsByPrefix.computeIfAbsent(prefix, k -> new AtomicLong()).incrementAndGet();
        try {
            return delegate.list(prefix, startAfter, maxKeys);
        } finally {
            if (prefix.startsWith(inboxPrefix)) {
                inboxListed.countDown();
            }
        }
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
