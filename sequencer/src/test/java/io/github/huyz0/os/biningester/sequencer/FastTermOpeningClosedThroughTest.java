// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.INDEX;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.latest;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.put;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.roster;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Roster;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * A started term knows the highest closed epoch -- {@code closedThrough},
 * which every JOINED carries (ADR-0081 §5 step 7) -- and the earlier terms
 * still open after its start closed what it could (M13.27j).
 */
class FastTermOpeningClosedThroughTest {

    private static FastTermOpening.Opened open(MemoryBinStore store, long epoch) throws Exception {
        return new FastTermOpening(new FastTermStart(store, "p"),
                new EmptyTermCloser(store, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), new SimulatedClock(1_000_000L),
                        new FastLeaseFenceHolderTest.Mono()),
                FastTermStartTest.SELF, Map::of).open(epoch, () -> { }).orElseThrow();
    }

    private static Roster recording(long epoch, long predecessor) {
        Roster empty = roster(epoch, predecessor, "old" + epoch, false, 0, 0, false);
        TreeMap<java.util.UUID, Integer> q = new TreeMap<>();
        q.put(INDEX, 2);
        return new Roster(epoch, predecessor, empty.leader(), empty.members(),
                List.of(new Roster.TermRecord(0, q)), List.of(), 0, 0, false);
    }

    @Test
    void theFIRSTTermHasClosedNothing() throws Exception {
        FastTermOpening.Opened opened = open(new MemoryBinStore(), 1);

        assertThat(opened.closedThrough()).isZero();
        assertThat(opened.stillOpen()).isEmpty();
    }

    @Test
    void EMPTYEarlierTermsClosedAtTheStartAreClosedThrough() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        put(store, roster(1, -1, "a", false, 0, 0, false));
        put(store, roster(2, 1, "b", false, 0, 0, false));
        latest(store, 2);

        FastTermOpening.Opened opened = open(store, 3);

        assertThat(opened.closedThrough()).isEqualTo(2);
        assertThat(opened.stillOpen()).isEmpty();
    }

    @Test
    void aWALKThatStoppedAtAClosedTermIsClosedThroughIt() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        put(store, roster(1, -1, "a", false, 0, 0, true));
        latest(store, 1);

        assertThat(open(store, 2).closedThrough()).isEqualTo(1);
    }

    @Test
    void aTERMThatRecordedAFastIndexStaysOpenAndBoundsClosedThrough() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        put(store, roster(1, -1, "a", false, 0, 0, false));
        put(store, recording(2, 1));
        put(store, roster(3, 2, "c", false, 0, 0, false));
        latest(store, 3);

        FastTermOpening.Opened opened = open(store, 4);

        assertThat(opened.closedThrough()).as("term 1 closed; 2 recorded a fast index")
                .isEqualTo(1);
        assertThat(opened.stillOpen()).extracting(Roster::epoch).containsExactly(3L, 2L);
    }
}
