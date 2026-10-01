// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.Capabilities;
import io.github.huyz0.os.biningester.binstore.ListPage;
import io.github.huyz0.os.biningester.binstore.MultipartWriter;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.binstore.Version;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Pod;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;

/**
 * A leader releasing its lease has stopped exposing BEFORE the release write
 * reaches the store (ADR-0081 §3, §9; M13.26 review round 3, T1).
 *
 * <p>⚠️ LOAD-BEARING SINCE M13.26b: a successor whose walk finds the old
 * leader departed is exempt from the lease's waits, so nothing but this order
 * keeps the old leader from exposing once the expired lease is readable.
 */
class FastLeaseFenceReleaseTest {

    /** Records {@code probe} at every conditional write of an existing object. */
    static final class ProbingStore implements BinStore {
        private final BinStore delegate;
        BooleanSupplier probe = () -> false;
        final List<Boolean> seen = new ArrayList<>();

        ProbingStore(BinStore delegate) {
            this.delegate = delegate;
        }

        @Override
        public Optional<Version> putIfMatch(String key, Body body, Version expected)
                throws IOException {
            seen.add(probe.getAsBoolean());
            return delegate.putIfMatch(key, body, expected);
        }

        @Override
        public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
            return delegate.putIfAbsent(key, body);
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

    @Test
    void theFENCEIsDownWhenTheReleaseWriteIsSent() throws Exception {
        ProbingStore store = new ProbingStore(new MemoryBinStore());
        Pod a = FastLeaseFenceHolderTest.pod(store, "a");
        a.leases().tryAcquire();
        store.probe = a.fence()::mayExpose;

        a.leases().release();

        assertThat(store.seen).as("exposing was refused when the expired lease was written")
                .containsExactly(false);
    }
}
