// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.INDEX;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.latest;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.put;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.read;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.roster;
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
import io.github.huyz0.os.biningester.format.Roster;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A fast index recorded after a term's start keeps it open, and a close that
 * cannot land closes nothing after it (M13.27g review round 1, T1, T2).
 */
class EmptyTermCloserGuardsTest {

    /** Refuses every conditional write to {@code key}, as a store under a racing writer would. */
    static final class RefusingStore implements BinStore {
        private final MemoryBinStore delegate;
        private final String key;

        RefusingStore(MemoryBinStore delegate, String key) {
            this.delegate = delegate;
            this.key = key;
        }

        @Override
        public Optional<Version> putIfMatch(String k, Body body, Version expected)
                throws IOException {
            return k.equals(key) ? Optional.empty() : delegate.putIfMatch(k, body, expected);
        }

        @Override
        public Optional<Version> putIfAbsent(String k, Body body) throws IOException {
            return delegate.putIfAbsent(k, body);
        }

        @Override
        public InputStream get(String k) throws IOException {
            return delegate.get(k);
        }

        @Override
        public InputStream getRange(String k, long start, long endIncl) throws IOException {
            return delegate.getRange(k, start, endIncl);
        }

        @Override
        public Optional<ObjectStat> stat(String k) throws IOException {
            return delegate.stat(k);
        }

        @Override
        public Version put(String k, Body body) throws IOException {
            return delegate.put(k, body);
        }

        @Override
        public MultipartWriter multipart(String k) throws IOException {
            return delegate.multipart(k);
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

    private static FastTermStart.Started start(BinStore store, long epoch) throws Exception {
        return (FastTermStart.Started) FastTermStartTest.start((MemoryBinStore) store, epoch,
                Long.MIN_VALUE, Optional.empty());
    }

    private static void twoEmptyTerms(MemoryBinStore store) throws Exception {
        put(store, roster(1, -1, "old1", false, 0, 0, false));
        put(store, roster(2, 1, "old2", false, 0, 0, false));
        latest(store, 2);
    }

    @Test
    void aFASTIndexRecordedAfterTheTermsStartKeepsItOpen() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Roster empty = roster(1, -1, "old1", false, 0, 0, false);
        TreeMap<UUID, Integer> later = new TreeMap<>();
        later.put(INDEX, 2);
        put(store, new Roster(1, -1, empty.leader(), empty.members(),
                List.of(new Roster.TermRecord(0, new TreeMap<>()),
                        new Roster.TermRecord(1, later)), List.of(), 0, 0, false));
        put(store, roster(2, 1, "old2", false, 0, 0, false));
        latest(store, 2);
        FastTermStart.Started third = start(store, 3);

        assertThat(new EmptyTermCloser(store, "p").close(3, third.unclosed())).isZero();

        assertThat(read(store, 1).closed()).as("its first element is empty; its second is not")
                .isFalse();
        assertThat(read(store, 2).closed()).isFalse();
    }

    @Test
    void aCLOSEAlwaysRefusedThrowsAndClosesNothingAfterIt() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        twoEmptyTerms(backing);
        FastTermStart.Started third = start(backing, 3);
        RefusingStore store = new RefusingStore(backing, Roster.key("p", 1));

        assertThatThrownBy(() -> new EmptyTermCloser(store, "p").close(3, third.unclosed()))
                .isInstanceOf(IOException.class);

        assertThat(read(backing, 1).closed()).isFalse();
        assertThat(read(backing, 2).closed()).as("never past an open term").isFalse();
    }

    @Test
    void aROSTERGoneAfterItsFenceThrowsAndClosesNothingAfterIt() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        twoEmptyTerms(store);
        FastTermStart.Started third = start(store, 3);
        store.delete(List.of(Roster.key("p", 1)));

        assertThatThrownBy(() -> new EmptyTermCloser(store, "p").close(3, third.unclosed()))
                .isInstanceOf(IOException.class);

        assertThat(read(store, 2).closed()).isFalse();
    }
}
