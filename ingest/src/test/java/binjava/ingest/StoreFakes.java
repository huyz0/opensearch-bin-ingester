// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.Capabilities;
import binjava.binstore.ListPage;
import binjava.binstore.MultipartWriter;
import binjava.binstore.ObjectStat;
import binjava.binstore.Version;
import binjava.binstore.backend.MemoryBinStore;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Store decorators that misbehave on purpose, so a test can observe ordering and
 * failure paths a well-behaved fake cannot show.
 *
 * <p>⚠️ Split out of {@code DefaultIngestTest} when that file passed the 500-line
 * limit. code-structure.md rule 1: split it, never raise the limit.
 */
final class StoreFakes {

    private StoreFakes() {
    }

    /** Holds the commit's putIfAbsent open so the ack ordering is observable. */
    static final class GatedCommit implements BinStore {
        private final BinStore delegate;
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        GatedCommit(BinStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
            entered.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
            return delegate.putIfAbsent(key, body);
        }

        @Override public Version put(String k, Body b) throws IOException {
            return delegate.put(k, b);
        }

        @Override public InputStream get(String k) throws IOException { return delegate.get(k); }

        @Override public InputStream getRange(String k, long s, long e) throws IOException {
            return delegate.getRange(k, s, e);
        }

        @Override public ListPage list(String p, String a, int m) throws IOException {
            return delegate.list(p, a, m);
        }

        @Override public void delete(List<String> k) throws IOException { delegate.delete(k); }

        @Override public Capabilities capabilities() { return delegate.capabilities(); }

        @Override public Optional<ObjectStat> stat(String k) throws IOException {
            return delegate.stat(k);
        }

        @Override public Optional<Version> putIfMatch(String k, Body b, Version v) throws IOException {
            return delegate.putIfMatch(k, b, v);
        }

        @Override public MultipartWriter multipart(String k) throws IOException {
            return delegate.multipart(k);
        }

        @Override public void close() throws IOException { delegate.close(); }
    }

    /**
     * A store whose PUTs fail, so a flush fails with waiters already attached.
     *
     * <p>⚠️ Delegates rather than extends — {@code MemoryBinStore} is final, and
     * a decorator is the shape the SPI is built for anyway.
     */
    record FailingPuts(BinStore delegate) implements BinStore {
        FailingPuts() {
            this(new MemoryBinStore());
        }

        @Override
        public Version put(String key, Body body) throws IOException {
            throw new IOException("the store is unavailable");
        }

        @Override
        public Capabilities capabilities() {
            return delegate.capabilities();
        }

        @Override
        public Optional<ObjectStat> stat(String key) throws IOException {
            return delegate.stat(key);
        }

        @Override
        public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
            return delegate.putIfAbsent(key, body);
        }

        @Override
        public Optional<Version> putIfMatch(String key, Body body, Version expected) throws IOException {
            return delegate.putIfMatch(key, body, expected);
        }

        @Override
        public MultipartWriter multipart(String key) throws IOException {
            return delegate.multipart(key);
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
        public ListPage list(String prefix, String startAfter, int maxKeys) throws IOException {
            return delegate.list(prefix, startAfter, maxKeys);
        }

        @Override
        public void delete(List<String> keys) throws IOException {
            delegate.delete(keys);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
