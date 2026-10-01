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
import io.github.huyz0.os.biningester.format.Roster;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The walk against a deposed leader writing at the same time (ADR-0081 §5
 * step 3; M13.26b): a roster written after the walk read it, a late
 * {@code LATEST} below this term, and a newer term's {@code LATEST}.
 */
class FastTermStartRaceTest {

    /** Runs {@code before} once, just before the first conditional write to {@code key}. */
    static final class HookedStore implements BinStore {
        interface Action {
            void run(MemoryBinStore backing) throws Exception;
        }

        private final MemoryBinStore delegate;
        private final String key;
        private final Action before;
        private boolean fired;

        HookedStore(MemoryBinStore delegate, String key, Action before) {
            this.delegate = delegate;
            this.key = key;
            this.before = before;
        }

        private void maybeFire(String k) throws IOException {
            if (!fired && k.equals(key)) {
                fired = true;
                try {
                    before.run(delegate);
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }
        }

        @Override
        public Optional<Version> putIfMatch(String k, Body body, Version expected)
                throws IOException {
            maybeFire(k);
            return delegate.putIfMatch(k, body, expected);
        }

        @Override
        public Optional<Version> putIfAbsent(String k, Body body) throws IOException {
            maybeFire(k);
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

    private static FastTermStart.Outcome start(BinStore store, long epoch) throws Exception {
        return new FastTermStart(store, "p").start(epoch, FastTermStartTest.SELF,
                Map.of(FastTermStartTest.INDEX, 2), 0, Optional.empty());
    }

    @Test
    void aROSTERWrittenAfterTheWalkIsReReadAndFencedWithItsWrite() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        FastTermStartTest.put(backing, FastTermStartTest.roster(5, -1, "c", false, 0, 0, false));
        FastTermStartTest.latest(backing, 5);
        Roster joined = new Roster(5, -1, FastTermStartTest.incarnation("c"),
                List.of(new Roster.Member(FastTermStartTest.incarnation("c"),
                                Roster.State.ROSTERED),
                        new Roster.Member(FastTermStartTest.incarnation("d"),
                                Roster.State.ROSTERED)),
                FastTermStartTest.roster(5, -1, "c", false, 0, 0, false).termRecord(), List.of(),
                0, 0, false);
        HookedStore store = new HookedStore(backing, Roster.key("p", 5),
                b -> FastTermStartTest.put(b, joined));

        FastTermStart.Started started = (FastTermStart.Started) start(store, 7);

        Roster fenced = FastTermStartTest.read(backing, 5);
        assertThat(fenced.fencedBy()).isEqualTo(7);
        assertThat(fenced.members()).as("the deposed leader's join is kept, not overwritten")
                .hasSize(2);
        assertThat(started.unclosed().get(0)).isEqualTo(fenced);
    }

    @Test
    void aLATELatestBelowThisTermIsWalkedFencedAndBecomesThePredecessor() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        FastTermStartTest.put(backing, FastTermStartTest.roster(5, -1, "c", false, 100, 0, false));
        FastTermStartTest.latest(backing, 5);
        HookedStore store = new HookedStore(backing, Roster.latestKey("p"), b -> {
            FastTermStartTest.put(b, FastTermStartTest.roster(6, 5, "e", false, 900, 0, false));
            FastTermStartTest.latest(b, 6);
        });

        FastTermStart.Started started = (FastTermStart.Started) start(store, 7);

        assertThat(FastTermStartTest.read(backing, 6).fencedBy()).isEqualTo(7);
        assertThat(FastTermStartTest.read(backing, 7).predecessor()).isEqualTo(6);
        assertThat(FastTermStartTest.read(backing, 7).notBefore())
                .as("the late term's notBefore is inherited").isEqualTo(900);
        assertThat(started.unclosed()).extracting(Roster::epoch).containsExactly(6L, 5L);
        assertThat(FastTermStartTest.readLatest(backing)).isEqualTo(7);
    }

    @Test
    void aNEWERLatestDuringTheStartDeposesIt() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        FastTermStartTest.put(backing, FastTermStartTest.roster(5, -1, "c", false, 0, 0, false));
        FastTermStartTest.latest(backing, 5);
        HookedStore store = new HookedStore(backing, Roster.latestKey("p"),
                b -> FastTermStartTest.latest(b, 8));

        FastTermStart.Outcome outcome = start(store, 7);

        assertThat(outcome).isEqualTo(new FastTermStart.Deposed(8));
        assertThat(FastTermStartTest.readLatest(backing)).as("never written lower").isEqualTo(8);
    }

    @Test
    void aLATELatestNamingAnUndepartedLeaderVoidsTheExemption() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        FastTermStartTest.put(backing, FastTermStartTest.roster(5, -1, "c", true, 0, 0, false));
        FastTermStartTest.latest(backing, 5);
        HookedStore store = new HookedStore(backing, Roster.latestKey("p"), b -> {
            FastTermStartTest.put(b, FastTermStartTest.roster(6, 5, "e", false, 0, 0, false));
            FastTermStartTest.latest(b, 6);
        });

        FastTermStart.Started started = (FastTermStart.Started) new FastTermStart(store, "p")
                .start(7, FastTermStartTest.SELF, Map.of(), 400, Optional.of("uid-c"));

        assertThat(started.handedOver())
                .as("term 6's leader did not depart; the first walk alone said exempt").isFalse();
        assertThat(FastTermStartTest.read(backing, 7).notBefore()).isEqualTo(400);
    }
}
