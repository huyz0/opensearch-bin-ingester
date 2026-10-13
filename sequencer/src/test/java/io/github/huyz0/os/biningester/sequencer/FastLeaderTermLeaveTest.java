// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.SELF;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.put;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.read;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.Q1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.Version;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * A leader shutting down marks itself departed in its roster, and closes it
 * only if every earlier term is closed (ADR-0081 §9; M13.35a) -- so its
 * successor is handed over without a fence -- unless an entry it exposed is
 * not yet committed: it then stays undeparted, counted as a crash would be.
 */
class FastLeaderTermLeaveTest {

    /** A store counting its writes of every kind. */
    static final class Counted extends ForwardingBinStore {
        final AtomicInteger writes = new AtomicInteger();

        Counted(MemoryBinStore delegate) {
            super(delegate);
        }

        @Override
        public Version put(String key, Body body) throws IOException {
            writes.incrementAndGet();
            return super.put(key, body);
        }

        @Override
        public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
            writes.incrementAndGet();
            return super.putIfAbsent(key, body);
        }

        @Override
        public Optional<Version> putIfMatch(String key, Body body, Version expected)
                throws IOException {
            writes.incrementAndGet();
            return super.putIfMatch(key, body, expected);
        }
    }

    /** {@link FastLeaderTermCommitTest#term3}'s term, over {@code counted}. */
    private static FastLeaderTerm term3(MemoryBinStore store, Counted counted) throws Exception {
        put(store, FastTermStartTest.roster(1, -1, "x", false, 0, 0, false));
        put(store, FastTermStartTest.roster(2, 1, "y", false, 0, 0, false));
        FastTermStartTest.latest(store, 2);
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(counted, "p"),
                new EmptyTermCloser(counted, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), new SimulatedClock(1_000_000L),
                        new Mono()), SELF, Map::of)
                .open(3, () -> { }).orElseThrow();
        return new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(counted, "p", 3, new Mono(), Duration.ofMillis(250)),
                        nanos -> { }),
                key -> 0L, counted, "p", new Mono(), Duration.ofMillis(250));
    }

    @Test
    void aLEADERWithNothingExposedAndEveryEarlierTermClosedDepartsAndClosesInOneWrite()
            throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Counted counted = new Counted(store);
        FastLeaderTerm term = term3(store, counted);
        assertThat(read(store, 2).closed()).as("the empty predecessor closed at the start")
                .isTrue();
        int before = counted.writes.get();

        assertThat(term.leave()).isEqualTo(FastLeaderTerm.Left.DEPARTED_AND_CLOSED);

        Roster own = read(store, 3);
        assertThat(own.leaderDeparted()).isTrue();
        assertThat(own.closed()).isTrue();
        assertThat(counted.writes.get() - before).as("one roster write").isEqualTo(1);
    }

    @Test
    void aLEADERWithAnEarlierTermOpenDepartsAndLeavesItsRosterOpen() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = FastLeaderTermDepartTest.term3(store, Map.of());

        assertThat(term.leave()).isEqualTo(FastLeaderTerm.Left.DEPARTED);

        Roster own = read(store, 3);
        assertThat(own.leaderDeparted()).isTrue();
        assertThat(own.closed()).as("term 2 is open: its takeover is the successor's")
                .isFalse();
    }

    @Test
    void aLEADERHoldingAnExposedEntryStaysUndeparted() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = FastLeaderTermCommitTest.term3(store);
        FastLeaderTermCommitTest.join(term);
        term.catalogSays(Map.of(Q1, 1));
        term.flushTermRecord();
        CommitDesk desk = term.commitDesk(
                FastJournal.recover(new MemoryJournalFile(), 1 << 20), SELF);
        term.answerCommit(FastLeaderTermCommitTest.header(3, FastLeaderTermCommitTest.POD),
                commit(FastLeaderTermCommitTest.POD.podId(), 1, S1), desk);
        assertThat(desk.exposures()).as("q = 1: the leader's own copy").hasSize(1);

        assertThat(term.leave()).isEqualTo(FastLeaderTerm.Left.STAYED);

        Roster own = read(store, 3);
        assertThat(own.leaderDeparted()).isFalse();
        assertThat(own.closed()).isFalse();
    }

    @Test
    void aDEPOSEDLeaderWritesNothing() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = FastLeaderTermCommitTest.term3(store);
        put(store, read(store, 3).fencedBy(5));

        assertThat(term.leave()).isEqualTo(FastLeaderTerm.Left.DEPOSED);

        assertThat(read(store, 3).leaderDeparted()).isFalse();
    }

    @Test
    void aLEFTTermsDeskAssignsNothing() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = FastLeaderTermCommitTest.term3(store);
        FastLeaderTermCommitTest.join(term);
        term.catalogSays(Map.of(Q1, 1));
        term.flushTermRecord();
        term.leave();

        CommitDesk desk = term.commitDesk(
                FastJournal.recover(new MemoryJournalFile(), 1 << 20), SELF);

        // ⚠️ THE COMMIT aLEADERHoldingAnExposedEntryStaysUndeparted has assigned.
        assertThat(term.answerCommit(
                FastLeaderTermCommitTest.header(3, FastLeaderTermCommitTest.POD),
                commit(FastLeaderTermCommitTest.POD.podId(), 1, S1), desk))
                .as("a desk built after the leave is stopped from its birth")
                .isInstanceOf(io.github.huyz0.os.biningester.format.FastFrame.Refused.class);
    }

    @Test
    void theSUCCESSORAfterALeaveIsHandedOverWithNothingToFence() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Counted counted = new Counted(store);
        term3(store, counted).leave();
        int before = counted.writes.get();

        FastTermStart.Started next = (FastTermStart.Started) new FastTermStart(counted, "p")
                .start(4, FastTermStartTest.incarnation("next"), Map.of(), Long.MAX_VALUE,
                        Optional.of(SELF.podUid()));

        assertThat(next.handedOver()).as("its holder departed: no lease wait").isTrue();
        assertThat(next.unclosed()).as("term 3 closed: nothing fenced").isEmpty();
        assertThat(next.notBefore()).isZero();
        assertThat(counted.writes.get() - before).as("its roster and LATEST, no fence")
                .isEqualTo(2);
    }
}
