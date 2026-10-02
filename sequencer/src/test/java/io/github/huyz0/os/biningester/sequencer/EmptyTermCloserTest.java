// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.INDEX;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.latest;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.put;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.read;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.roster;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.List;
import java.util.Optional;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * A term start closes, oldest first, every earlier term it fenced that
 * recorded no fast index, so the next walk stops there (M13.27g).
 */
class EmptyTermCloserTest {

    private static FastTermStart.Started start(MemoryBinStore store, long epoch) throws Exception {
        return (FastTermStart.Started) FastTermStartTest.start(store, epoch, Long.MIN_VALUE,
                Optional.empty());
    }

    private static Roster recording(long epoch, long predecessor) {
        Roster empty = roster(epoch, predecessor, "old" + epoch, false, 0, 0, false);
        TreeMap<java.util.UUID, Integer> q = new TreeMap<>();
        q.put(INDEX, 2);
        return new Roster(epoch, predecessor, empty.leader(), empty.members(),
                List.of(new Roster.TermRecord(0, q)), List.of(), 0, 0, false);
    }

    private static void twoEmptyTerms(MemoryBinStore store) throws Exception {
        put(store, roster(1, -1, "old1", false, 0, 0, false));
        put(store, roster(2, 1, "old2", false, 0, 0, false));
        latest(store, 2);
    }

    @Test
    void EMPTYEarlierTermsAreClosedAndTheNextWalkStopsAtThem() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        twoEmptyTerms(store);
        FastTermStart.Started third = start(store, 3);

        int closed = new EmptyTermCloser(store, "p").close(3, third.unclosed());

        assertThat(closed).isEqualTo(2);
        assertThat(read(store, 1).closed()).isTrue();
        assertThat(read(store, 2).closed()).as("still fenced by 3").isTrue();
        assertThat(read(store, 2).fencedBy()).isEqualTo(3);
        assertThat(start(store, 4).unclosed()).extracting(Roster::epoch)
                .as("the walk stops at the closed term 2").containsExactly(3L);
    }

    @Test
    void aTermThatRECORDEDAFastIndexStaysOpenAndSoDoesEveryLaterOne() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        put(store, recording(1, -1));
        put(store, roster(2, 1, "old2", false, 0, 0, false));
        latest(store, 2);
        FastTermStart.Started third = start(store, 3);

        assertThat(new EmptyTermCloser(store, "p").close(3, third.unclosed())).isZero();

        assertThat(read(store, 1).closed()).isFalse();
        assertThat(read(store, 2).closed()).as("an empty term after an open one").isFalse();
    }

    @Test
    void theOLDERTermsBeforeTheFirstRecordingOneAreClosed() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        put(store, roster(1, -1, "old1", false, 0, 0, false));
        put(store, recording(2, 1));
        latest(store, 2);
        FastTermStart.Started third = start(store, 3);

        assertThat(new EmptyTermCloser(store, "p").close(3, third.unclosed())).isEqualTo(1);

        assertThat(read(store, 1).closed()).isTrue();
        assertThat(read(store, 2).closed()).isFalse();
    }

    @Test
    void aTermThatDECIDEDAStreamStaysOpen() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Roster empty = roster(1, -1, "old1", false, 0, 0, false);
        put(store, new Roster(1, -1, empty.leader(), empty.members(), empty.termRecord(),
                List.of(new Roster.Decision(0, new RunKey(INDEX, 0), 5)), 0, 0, false));
        latest(store, 1);
        FastTermStart.Started second = start(store, 2);

        assertThat(new EmptyTermCloser(store, "p").close(2, second.unclosed())).isZero();

        assertThat(read(store, 1).closed()).isFalse();
    }

    @Test
    void aTermANEWERTermFencedIsLeftToIt() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        twoEmptyTerms(store);
        FastTermStart.Started third = start(store, 3);
        put(store, read(store, 1).fencedBy(5));

        assertThat(new EmptyTermCloser(store, "p").close(3, third.unclosed())).isZero();

        assertThat(read(store, 1).closed()).isFalse();
        assertThat(read(store, 2).closed()).isFalse();
    }

    @Test
    void aTermAlreadyCLOSEDCountsAndTheNextIsClosed() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        twoEmptyTerms(store);
        FastTermStart.Started third = start(store, 3);
        put(store, read(store, 1).asClosed());

        assertThat(new EmptyTermCloser(store, "p").close(3, third.unclosed())).isEqualTo(2);

        assertThat(read(store, 2).closed()).isTrue();
    }

    @Test
    void aREFUSEDCloseIsReadAgainAndRetried() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        twoEmptyTerms(backing);
        FastTermStart.Started third = start(backing, 3);
        // the same roster written again: a new version, nothing else changed
        FastTermStartRaceTest.HookedStore store = new FastTermStartRaceTest.HookedStore(backing,
                Roster.key("p", 1), b -> put(b, read(b, 1)));

        assertThat(new EmptyTermCloser(store, "p").close(3, third.unclosed())).isEqualTo(2);

        assertThat(read(backing, 1).closed()).isTrue();
    }

    @Test
    void aCLOSERacedByANewerFenceStops() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        twoEmptyTerms(backing);
        FastTermStart.Started third = start(backing, 3);
        FastTermStartRaceTest.HookedStore store = new FastTermStartRaceTest.HookedStore(backing,
                Roster.key("p", 1), b -> put(b, read(b, 1).fencedBy(5)));

        assertThat(new EmptyTermCloser(store, "p").close(3, third.unclosed())).isZero();

        assertThat(read(backing, 1).closed()).isFalse();
        assertThat(read(backing, 2).closed()).isFalse();
    }
}
