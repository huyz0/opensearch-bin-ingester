// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.INDEX;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.latest;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.put;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.roster;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Roster;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * {@code closedThrough} counts what a failing closer closed, and the steady
 * state -- an earlier term holding a fast index open -- is closed through the
 * term before it (M13.27j review round 1, P2, T3).
 */
class FastTermOpeningPartlyClosedTest {

    private static FastLeaseFence fence() {
        return new FastLeaseFence(Duration.ofSeconds(10), new SimulatedClock(1_000_000L),
                new FastLeaseFenceHolderTest.Mono());
    }

    private static Roster recording(long epoch, long predecessor, boolean closed) {
        Roster empty = roster(epoch, predecessor, "old" + epoch, false, 0, 0, closed);
        TreeMap<java.util.UUID, Integer> q = new TreeMap<>();
        q.put(INDEX, 2);
        return new Roster(epoch, predecessor, empty.leader(), empty.members(),
                List.of(new Roster.TermRecord(0, q)), List.of(), 0, 0, closed);
    }

    private static void threeEmptyTerms(MemoryBinStore store) throws Exception {
        put(store, roster(1, -1, "a", false, 0, 0, false));
        put(store, roster(2, 1, "b", false, 0, 0, false));
        put(store, roster(3, 2, "c", false, 0, 0, false));
        latest(store, 3);
    }

    @Test
    void aCLOSERThatFailedAfterTwoCountsTheTwo() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        threeEmptyTerms(store);
        FastTermOpening opening = new FastTermOpening(new FastTermStart(store, "p"),
                (epoch, unclosed) -> {
                    throw new EmptyTermCloser.Stopped(2, new IOException("store down"));
                }, fence(), FastTermStartTest.SELF, Map::of);

        FastTermOpening.Opened opened = opening.open(4, () -> { }).orElseThrow();

        assertThat(opened.closedThrough()).as("1 and 2 were closed before the failure")
                .isEqualTo(2);
        assertThat(opened.stillOpen()).extracting(Roster::epoch).containsExactly(3L);
    }

    @Test
    void theCLOSERSaysHowManyItClosedBeforeTheStoreFailed() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        threeEmptyTerms(backing);
        FastTermStart.Started started = (FastTermStart.Started) new FastTermStart(backing, "p")
                .start(4, FastTermStartTest.SELF, Map.of(), Long.MIN_VALUE, Optional.empty());
        EmptyTermCloserGuardsTest.RefusingStore store =
                new EmptyTermCloserGuardsTest.RefusingStore(backing, Roster.key("p", 2));

        assertThatThrownBy(() -> new EmptyTermCloser(store, "p").close(4, started.unclosed()))
                .isInstanceOfSatisfying(EmptyTermCloser.Stopped.class,
                        stopped -> assertThat(stopped.closed()).isEqualTo(1));
    }

    @Test
    void aTERMHeldOpenByAFastIndexBoundsClosedThroughBelowIt() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        put(store, roster(1, -1, "a", false, 0, 0, true));
        put(store, recording(2, 1, false));
        latest(store, 2);
        FastTermOpening opening = new FastTermOpening(new FastTermStart(store, "p"),
                new EmptyTermCloser(store, "p")::close, fence(), FastTermStartTest.SELF,
                Map::of);

        FastTermOpening.Opened opened = opening.open(3, () -> { }).orElseThrow();

        assertThat(opened.closedThrough()).as("1, closed; 2 stays open").isEqualTo(1);
        assertThat(opened.stillOpen()).extracting(Roster::epoch).containsExactly(2L);
    }
}
