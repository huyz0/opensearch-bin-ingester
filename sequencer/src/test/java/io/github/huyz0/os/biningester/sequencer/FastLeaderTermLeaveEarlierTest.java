// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.INDEX;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.SELF;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.latest;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.put;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.read;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.roster;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A leader shutting down marks itself departed in each earlier unclosed
 * roster listing it where it holds no entry of that term (ADR-0081 §9;
 * M13.35d), as a non-leader does; one it holds an entry of keeps it, its
 * deletion counted there as a crash would be.
 */
class FastLeaderTermLeaveEarlierTest {

    /** Term 3 over an open term 2 (it recorded a fast index) listing this leader. */
    private static FastLeaderTerm term3(MemoryBinStore store) throws Exception {
        put(store, roster(1, -1, "x", false, 0, 0, true));
        Roster two = roster(2, 1, "y", false, 0, 0, false);
        TreeMap<UUID, Integer> q = new TreeMap<>();
        q.put(INDEX, 2);
        put(store, new Roster(2, 1, two.leader(),
                List.of(two.members().get(0), new Roster.Member(SELF, Roster.State.ROSTERED)),
                List.of(new Roster.TermRecord(0, q)), List.of(), 0, 0, false));
        latest(store, 2);
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(store, "p"),
                new EmptyTermCloser(store, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), new SimulatedClock(1_000_000L),
                        new Mono()), SELF, Map::of)
                .open(3, () -> { }).orElseThrow();
        assertThat(opened.stillOpen()).extracting(Roster::epoch).containsExactly(2L);
        return new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(store, "p", 3, new Mono(), Duration.ofMillis(250)),
                        nanos -> { }),
                key -> 0L, store, "p", new Mono(), Duration.ofMillis(250));
    }

    private static FastJournalRecord.Entry entryOf(long epoch) {
        return new FastJournalRecord.Entry(epoch, new RunKey(INDEX, 0), 0, 2, 0,
                new FastJournalRecord.IdempotencyKey("pod-y", new UUID(9, 9), 0),
                List.of(new SegmentRecord("doc", OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1})));
    }

    @Test
    void aLEADERHoldingNothingOfAnEarlierOpenTermDepartsItThere() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = term3(store);
        FastJournal journal = FastJournal.recover(new MemoryJournalFile(), 1 << 20);

        assertThat(term.leave(() -> { }, Optional.of(journal)))
                .isEqualTo(FastLeaderTerm.Left.DEPARTED);

        assertThat(read(store, 2).member(SELF.podUid()).orElseThrow().state())
                .isEqualTo(Roster.State.DEPARTED);
        assertThat(read(store, 3).leaderDeparted()).isTrue();
        assertThat(read(store, 3).closed()).as("term 2 is open").isFalse();
    }

    @Test
    void aLEADERHoldingAnEntryOfAnEarlierTermStaysUndepartedThere() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = term3(store);
        FastJournal journal = FastJournal.recover(new MemoryJournalFile(), 1 << 20);
        assertThat(journal.append(entryOf(2))).isTrue();

        assertThat(term.leave(() -> { }, Optional.of(journal)))
                .isEqualTo(FastLeaderTerm.Left.DEPARTED);

        assertThat(read(store, 2).member(SELF.podUid()).orElseThrow().state())
                .as("its entry of term 2 is not known committed")
                .isEqualTo(Roster.State.ROSTERED);
        assertThat(read(store, 3).leaderDeparted()).isTrue();
    }
}
