// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S2;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * What a COMMIT is admitted against (M13.27s review round 1, T1-T3): only a
 * ROSTERED writer, at its own zone, against the ROSTERED pods' zones; never
 * by a deposed term; under the next decision its roster numbers and the
 * ADR's bound.
 */
class FastLeaderTermCommitAdmissionTest {

    private static final Roster.Incarnation A = FastTermStartTest.incarnation("a");
    private static final Roster.Incarnation B = FastTermStartTest.incarnation("b");
    /** In the leader's own zone, so a second zone must come from another pod. */
    private static final Roster.Incarnation W =
            new Roster.Incarnation("w", "uid-w", "az-self", "");

    private static FastLeaderTerm term3(MemoryBinStore store) throws Exception {
        FastTermStartTest.put(store, FastTermStartTest.roster(1, -1, "x", false, 0, 0, false));
        FastTermStartTest.put(store, FastTermStartTest.roster(2, 1, "y", false, 0, 0, false));
        FastTermStartTest.latest(store, 2);
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(store, "p"),
                new EmptyTermCloser(store, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), new SimulatedClock(1_000_000L),
                        new Mono()), FastTermStartTest.SELF, Map::of)
                .open(3, () -> { }).orElseThrow();
        // ⚠️ ONE CLOCK, ADVANCED BY THE SLEEPER: a second roster write waits out
        // min_upload_interval, and a frozen clock would wait for ever.
        Mono mono = new Mono();
        return new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(store, "p", 3, mono, Duration.ofMillis(250)),
                        nanos -> mono.advance(Duration.ofNanos(nanos))),
                key -> 0L, store, "p", mono, Duration.ofMillis(250));
    }

    private static FastFrame.Header header(int kind, Roster.Incarnation from) {
        return new FastFrame.Header(kind, 3, from.podUid(), "uid-self");
    }

    private static void join(FastLeaderTerm term, Roster.Incarnation pod) throws Exception {
        assertThat(term.answerJoin(header(FastFrame.KIND_JOIN, pod),
                new FastFrame.Join(pod, FastFrame.Held.NONE)))
                .isInstanceOf(FastFrame.Joined.class);
    }

    private static void depart(FastLeaderTerm term, Roster.Incarnation pod) throws Exception {
        term.answerDepart(header(FastFrame.KIND_DEPART, pod),
                new FastFrame.Depart(pod, 2, FastFrame.Held.NONE));
    }

    private static CommitDesk recorded(FastWriteLeaderTest.Leader l) {
        return new CommitDesk(l.leader(), stream -> 0L, Duration.ofSeconds(10));
    }

    private static FastFrame.Header commitFrom(Roster.Incarnation pod) {
        return header(FastWriteFrame.KIND_COMMIT, pod);
    }

    @Test
    void aDEPARTEDWriterIsRefusedNotRostered() throws Exception {
        FastLeaderTerm term = term3(new MemoryBinStore());
        join(term, A);
        depart(term, A);
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);

        FastFrame.Body answer = term.answerCommit(commitFrom(A), commit(1, S2), recorded(l));

        assertThat(((FastFrame.Refused) answer).reason())
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        assertThat(l.journal().held()).isEmpty();
    }

    @Test
    void aDEPARTEDPodLendsNoZoneToAQuorumOfTwo() throws Exception {
        FastLeaderTerm term = term3(new MemoryBinStore());
        join(term, A);
        depart(term, A);
        join(term, W);
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);

        // S2's index at wal_quorum 2: the leader and W share az-self, and only
        // the departed A was elsewhere.
        FastFrame.Body answer = term.answerCommit(commitFrom(W), commit(1, S2), recorded(l));

        assertThat(answer).isInstanceOfSatisfying(FastFrame.Refused.class, refused -> {
            assertThat(refused.reason()).isEqualTo(FastFrame.Reason.BACKPRESSURE);
            assertThat(refused.text()).contains("zones");
        });
    }

    @Test
    void theWRITERsZoneDecidesItsRequiredCopy() throws Exception {
        FastLeaderTerm term = term3(new MemoryBinStore());
        join(term, B);
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);

        FastFrame.Body answer = term.answerCommit(commitFrom(B), commit(1, S2), recorded(l));

        // the fixture's leader is in az-a: a writer in az-b holds a required copy
        assertThat(answer).isEqualTo(new FastWriteFrame.Assigned(3,
                List.of(new FastWriteFrame.AssignedRun(S2, 0, 2, true))));
    }

    @Test
    void aDEPOSEDTermRefusesACOMMITLowerEpoch() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = term3(store);
        join(term, B);
        FastTermStartTest.put(store, FastTermStartTest.read(store, 3).fencedBy(5));
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);

        FastFrame.Body answer = term.answerCommit(commitFrom(B), commit(1, S2), recorded(l));

        assertThat(((FastFrame.Refused) answer).reason())
                .isEqualTo(FastFrame.Reason.LOWER_EPOCH);
        assertThat(l.journal().held()).isEmpty();
    }

    @Test
    void theNEXTDecisionIsOnePastTheHighestItsRosterHolds() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = term3(store);
        assertThat(term.nextDecision()).as("none decided yet").isZero();
        RunKey s = new RunKey(new UUID(3, 3), 0);
        Roster r = FastTermStartTest.read(store, 3);
        // ⚠️ PRUNED: two decisions numbered 4 and 9 -- the list's size is 2.
        FastTermStartTest.put(store, new Roster(r.epoch(), r.predecessor(), r.leader(),
                r.members(), r.termRecord(), List.of(new Roster.Decision(4, s, 0),
                        new Roster.Decision(9, s, 0)), r.notBefore(), r.fencedBy(),
                r.closed()));
        term.answerHeld(header(FastFrame.KIND_HELD, B), new FastFrame.HeldReport(
                FastFrame.Held.NONE));

        assertThat(term.nextDecision()).isEqualTo(10);
        assertThat(FastLeaderTerm.STREAM_BOUND).as("ADR-0081's B").isEqualTo(65_536);
    }
}
