// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.Capabilities;
import io.github.huyz0.os.biningester.binstore.ListPage;
import io.github.huyz0.os.biningester.binstore.MultipartWriter;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.binstore.Version;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.Lease;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * FENCEs out of order, a store failing at start, and no write for an epoch
 * already durable (M13.26c review round 2, T5-T7).
 */
class EpochFenceOrderTest {

    private static final String LEASE = "p/ctl/lease/0.json";

    /** Fails its stat or its get, as a store erroring during a restart does. */
    private static final class ErroringStore implements BinStore {
        private final BinStore delegate;
        private final boolean statFails;

        ErroringStore(BinStore delegate, boolean statFails) {
            this.delegate = delegate;
            this.statFails = statFails;
        }

        @Override
        public Optional<ObjectStat> stat(String key) throws IOException {
            if (statFails) {
                throw new IOException("store unavailable");
            }
            return delegate.stat(key);
        }

        @Override
        public InputStream get(String key) throws IOException {
            throw new IOException("store unavailable");
        }

        @Override
        public InputStream getRange(String key, long start, long endIncl) throws IOException {
            throw new IOException("store unavailable");
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

    private static MemoryBinStore leaseAt(long epoch) throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        store.put(LEASE, Body.ofBytes(new Lease(epoch, "pod", "", 1_000).encode()));
        return store;
    }

    @Test
    void aLATERFenceOfAnOlderSuccessorNeverLowersIt() throws Exception {
        EpochFence fence = EpochFence.start(leaseAt(7), LEASE, Optional.of(new MemoryJournalFile()));
        fence.raise(9);

        fence.raise(8);

        assertThat(fence.highest()).isEqualTo(9);
        assertThat(fence.admit(8)).isFalse();
        assertThat(fence.deposes(8)).isTrue();
    }

    @Test
    void aSTOREFailingAtStartKeepsThePodUnready() throws Exception {
        MemoryBinStore backing = leaseAt(7);

        assertThatThrownBy(() -> EpochFence.start(new ErroringStore(backing, true), LEASE,
                Optional.empty())).as("its stat fails").isInstanceOf(IOException.class);
        assertThatThrownBy(() -> EpochFence.start(new ErroringStore(backing, false), LEASE,
                Optional.empty())).as("its get fails").isInstanceOf(IOException.class);
    }

    @Test
    void aFRAMEAtAnEpochAlreadyDurableWritesNothing() throws Exception {
        int[] writes = {0};
        MemoryJournalFile inner = new MemoryJournalFile();
        io.github.huyz0.os.biningester.binstore.JournalFile counting =
                new io.github.huyz0.os.biningester.binstore.JournalFile() {
                    @Override
                    public byte[] readAll() {
                        return inner.readAll();
                    }

                    @Override
                    public void append(byte[] bytes) {
                        inner.append(bytes);
                    }

                    @Override
                    public void force() {
                        inner.force();
                    }

                    @Override
                    public void truncate(long length) {
                        inner.truncate(length);
                    }

                    @Override
                    public void replace(byte[] contents) {
                        writes[0]++;
                        inner.replace(contents);
                    }

                    @Override
                    public long size() {
                        return inner.size();
                    }

                    @Override
                    public void close() {
                    }
                };
        EpochFence fence = EpochFence.start(leaseAt(7), LEASE, Optional.of(counting));
        int afterStart = writes[0];

        for (int i = 0; i < 5; i++) {
            fence.admit(7);
        }

        assertThat(writes[0]).as("no fsync per frame on the hot path").isEqualTo(afterStart);
    }
}
