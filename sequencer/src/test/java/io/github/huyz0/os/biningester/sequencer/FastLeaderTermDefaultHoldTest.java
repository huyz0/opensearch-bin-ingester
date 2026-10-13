// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.SELF;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.latest;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.put;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.roster;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A default-path run of an index with a term-record value in a walked term
 * waits until its stream is decided (ADR-0081 §5 step 3's tail; M13.33n):
 * held before any takeover ran, while every member is not yet heard, and
 * while its own stream is undecided -- an index no walked term recorded never.
 */
class FastLeaderTermDefaultHoldTest {

    /** Term 3 over an open term 2 whose record holds {@code index}. */
    static FastLeaderTerm walking(MemoryBinStore store, BinStore over, UUID index)
            throws Exception {
        put(store, roster(1, -1, "x", false, 0, 0, true));
        Roster two = roster(2, 1, "y", false, 0, 0, false);
        TreeMap<UUID, Integer> q = new TreeMap<>();
        q.put(index, 2);
        put(store, new Roster(2, 1, two.leader(), two.members(),
                List.of(new Roster.TermRecord(0, q)), List.of(), 0, 0, false));
        latest(store, 2);
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(over, "p"),
                new EmptyTermCloser(over, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), new SimulatedClock(1_000_000L),
                        new Mono()), SELF, Map::of)
                .open(3, () -> { }).orElseThrow();
        return new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(over, "p", 3, new Mono(), Duration.ofMillis(250)),
                        nanos -> { }),
                key -> 0L, over, "p", new Mono(), Duration.ofMillis(250));
    }

    private static final UUID WALKED = new UUID(4, 4);
    private static final RunKey STREAM = new RunKey(WALKED, 0);

    private static CommitDesk desk(FastLeaderTerm term) throws Exception {
        return term.commitDesk(FastJournal.recover(new MemoryJournalFile(), 1 << 20), SELF);
    }

    @Test
    void aWALKEDIndexsRunIsHeldBeforeAnyTakeoverRan() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = walking(store, store, WALKED);

        assertThat(term.holdsDefault(STREAM)).isTrue();
    }

    @Test
    void anINDEXNoWalkedTermRecordedIsNeverHeld() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = walking(store, store, WALKED);

        assertThat(term.holdsDefault(new RunKey(new UUID(5, 5), 0))).isFalse();
    }

    @Test
    void aWALKEDStreamIsHeldUntilItIsDecided() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = walking(store, store, WALKED);
        CommitDesk desk = desk(term);
        desk.leader().undecided(List.of(STREAM));
        desk.leader().everyMemberHeard();
        assertThat(term.holdsDefault(STREAM)).as("undecided").isTrue();

        desk.leader().decided(STREAM, 0);

        assertThat(term.holdsDefault(STREAM)).as("decided and committed").isFalse();
    }

    @Test
    void aWALKEDStreamWithNothingCollectedIsHeldUntilEveryMemberIsHeard() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = walking(store, store, WALKED);
        CommitDesk desk = desk(term);
        assertThat(term.holdsDefault(STREAM)).as("taking over").isTrue();

        desk.leader().everyMemberHeard();

        assertThat(term.holdsDefault(STREAM)).isFalse();
    }
}
