// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.B;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.C;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.LEADER;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A {@code wal_quorum = 3} index on two live zones switched to the default path
 * (M13.34aa; ADR-0081 §8; SPEC criterion 15): its entries assigned while az-c
 * was live, az-c then silent, no entry can reach its quorum, so the whole tail
 * is unexposed -- discarded by the switch's decision, each of its writers owed
 * DISCARDED and never acked -- and, with a fast writer that never stops, the
 * default-path run admitted within one roster write on the injected clock.
 *
 * <p>⚠️ THE CHAIN IS A MAP here: the unexposed tail uploads nothing, so the
 * chain is already at the frontier, and the default run's commit is stood for
 * by moving it. The real upload and commit are {@code FastSwitchToDefaultTest}'s.
 */
class FastSwitchTwoZonesTest {

    private static final UUID Q3 = new UUID(3, 3);
    private static final RunKey S = new RunKey(Q3, 0);
    private static final RunKey OTHER = new RunKey(new UUID(4, 4), 0);
    private static final Set<String> LIVE = Set.of("uid-l", "uid-b", "uid-c");
    private static final long INTERVAL = Duration.ofMillis(250).toNanos();

    private final long[] nanos = {0};
    private final int[] rosterWrites = {0};
    private final Map<RunKey, Long> chain = new HashMap<>(Map.of(S, 100L));
    private final List<FastJournalRecord.IdempotencyKey> written = new ArrayList<>();
    private final Roster roster = threeZones();
    private FastJournal journal;
    private FastWriteLeader leader;
    private MemoryBinStore rosters;

    /**
     * The leader in az-a, a writer in az-b and a holder in az-c, a quorum of
     * three. ⚠️ A batch is admitted only with three zones available, so the
     * tail is assigned while az-c is live; az-c is then silent, two zones live.
     */
    private static Roster threeZones() {
        List<Roster.Member> members = new ArrayList<>();
        members.add(new Roster.Member(LEADER, Roster.State.ROSTERED));
        for (QuorumFrontier.Holder h : List.of(B, C)) {
            members.add(new Roster.Member(new Roster.Incarnation(h.podUid(), h.podUid(),
                    h.az(), ""), Roster.State.ROSTERED));
        }
        return new Roster(7, -1, LEADER, members,
                List.of(new Roster.TermRecord(0, new TreeMap<>(Map.of(Q3, 3)))),
                List.of(), 0, 0, false);
    }

    /** Three batches of S assigned, az-c silent: the tail, none of it exposed. */
    private long unexposedTailPaused() throws Exception {
        journal = FastJournal.recover(new MemoryJournalFile(), 1 << 20);
        leader = new FastWriteLeader(7, LEADER, new FastCursor(1_000),
                new TermRecordWriter(new MemoryBinStore(), "p", roster, new Mono(),
                        Duration.ofMillis(250)),
                new QuorumFrontier("uid-l", "az-a"), journal, () -> 3);
        leader.open(S, 100);
        for (long seq = 1; seq <= 3; seq++) {
            leader.commit(B, commit(seq, S), roster, LIVE);
            leader.flushGroup();
            written.add(commit(seq, S).key());
        }
        // ⚠️ az-c SILENT: asked, never acks; only az-b's copies count.
        for (FastWriteLeader.ReplicaSend send : leader.replicas(List.of(B, C))) {
            if (!send.holder().equals(B)) {
                continue;
            }
            leader.replicaAck(B, new FastWriteFrame.ReplicaAck(3, send.replica().entries()
                    .stream().map(e -> new FastWriteFrame.RunAt(e.key(), e.firstOffset()))
                    .toList()));
        }
        assertThat(journal.held()).as("the premise: the tail assigned").hasSize(3);
        assertThat(leader.expose()).as("the premise: two zones never make a quorum of 3")
                .isEmpty();
        rosters = new MemoryBinStore();
        FastTermStartTest.put(rosters,
                FastTermStartTest.roster(3, 2, "self", false, 0, 0, false));
        return leader.pauseForSwitch(S).orElseThrow();
    }

    /** One drain pass; OTHER, a stream with no tail here, discards nothing. */
    private void tick(SwitchPause pause) throws IOException {
        pause.tick(new SwitchPause.Leader() {
            @Override
            public Set<FastJournalRecord.IdempotencyKey> discardFrom(RunKey stream, long from)
                    throws IOException {
                return stream.equals(S) ? leader.discardFrom(stream, from) : Set.of();
            }

            @Override
            public void unpause(RunKey stream, long chainNext) {
                if (stream.equals(S)) {
                    leader.unpause(stream, chainNext);
                }
            }
        }, new RosterDecisions(rosters, "p", 3), () -> rosterWrites[0]++,
                s -> chain.getOrDefault(s, 0L), s -> false);
    }

    @Test
    void aQ3IndexOnTwoZonesIsDiscardedWhollyAndItsDefaultRunAdmittedWithinOneWrite()
            throws Exception {
        long frontier = unexposedTailPaused();
        assertThat(frontier).as("nothing exposed: the frontier at the first offset")
                .isEqualTo(100);
        assertThat(leader.commit(B, commit(4, S), roster, LIVE))
                .as("the writer that never stops assigns nothing while paused")
                .isEqualTo(new FastWriteLeader.Wait("stream " + S
                        + " is paused for a switch to the default path"));
        SwitchPause pause = new SwitchPause(() -> nanos[0], INTERVAL);
        pause.paused(S, frontier, 0);

        tick(pause);

        assertThat(pause.holds(S)).as("the default run admitted in the same pass")
                .isFalse();
        assertThat(rosterWrites[0]).as("within one roster write").isEqualTo(1);
        assertThat(FastTermStartTest.read(rosters, 3).decisions())
                .containsExactly(new Roster.Decision(0, S, 100));
        assertThat(leader.discards()).as("every writer owed DISCARDED, none acked")
                .extracting(FastWriteLeader.Discard::key)
                .containsExactlyInAnyOrderElementsOf(written);
        assertThat(journal.held()).as("the tail dropped from the journal").isEmpty();

        chain.put(S, 106L);
        tick(pause);
        leader.commit(B, commit(5, S), roster, LIVE);
        assertThat(leader.flushGroup()).singleElement().satisfies(a -> assertThat(
                a.assigned().runs().get(0).firstOffset())
                .as("after the default run, never over it").isEqualTo(106));
    }

    @Test
    void aSWITCHBehindAnotherDecisionIsAdmittedOnceItsIntervalPassesNeverLater()
            throws Exception {
        // ⚠️ THE BOUND ON THE INJECTED CLOCK: another switch's decision was just
        // written, so this one's waits for the next roster write -- one
        // min_upload_interval, and not a nanosecond more.
        long frontier = unexposedTailPaused();
        SwitchPause pause = new SwitchPause(() -> nanos[0], INTERVAL);
        pause.paused(OTHER, 0, 0);
        tick(pause);
        pause.paused(S, frontier, 1);

        nanos[0] = INTERVAL - 1;
        tick(pause);
        assertThat(pause.holds(S)).as("inside the interval: no second write").isTrue();
        assertThat(journal.held()).as("nothing discarded before its decision").hasSize(3);

        nanos[0] = INTERVAL;
        tick(pause);

        assertThat(pause.holds(S)).as("admitted once the interval passes").isFalse();
        assertThat(rosterWrites[0]).as("one write each").isEqualTo(2);
        assertThat(journal.held()).isEmpty();
    }
}
