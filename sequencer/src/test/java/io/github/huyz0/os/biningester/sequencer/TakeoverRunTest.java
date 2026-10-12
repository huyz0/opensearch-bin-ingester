// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

/**
 * The takeover's loop (ADR-0081 §5 steps 4-7; M13.33k): collected until
 * every member answered or is gone, each stream decided once the wait has
 * passed, the decided streams written as one recovery entry, then the
 * roster's decisions, then the term's roster re-read, then each stream
 * resumed -- and only then RELEASE and the terms closed.
 */
class TakeoverRunTest {

    private static final UUID I = new UUID(1, 1);
    private static final UUID OTHER = new UUID(2, 2);
    private static final RunKey S1 = new RunKey(I, 0);
    private static final RunKey S2 = new RunKey(I, 1);
    private static final RunKey SO = new RunKey(OTHER, 0);
    private static final Roster.Incarnation L = pod("l", "az-a");
    private static final Roster.Incarnation M = pod("m", "az-b");
    private static final Roster.Incarnation N = pod("n", "az-c");
    private static final long BOUND = 10;

    private static Roster.Incarnation pod(String name, String az) {
        return new Roster.Incarnation(name, "uid-" + name, az, "");
    }

    /** Term 7, led by L, I recorded at {@code q} when {@code q > 0}. */
    private static Roster term7(int q) {
        List<Roster.TermRecord> record = q > 0
                ? List.of(new Roster.TermRecord(0, new TreeMap<>(Map.of(I, q))))
                : List.of();
        return new Roster(7, 6, L, List.of(
                new Roster.Member(L, Roster.State.ROSTERED),
                new Roster.Member(M, Roster.State.ROSTERED),
                new Roster.Member(N, Roster.State.ROSTERED)), record, List.of(), 0, 0, false);
    }

    private static FastJournalRecord.Entry at(long epoch, RunKey stream, long first, int count) {
        List<SegmentRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(new SegmentRecord("d" + first + "-" + i, OpType.INDEX,
                    OptionalLong.empty(), new byte[] {1}));
        }
        return new FastJournalRecord.Entry(epoch, stream, first, 2, 0,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), first), records);
    }

    private static TakeoverCollection.Collected collected(
            Map<String, List<FastJournalRecord.Entry>> byMember, Set<String> waiting) {
        return collected(byMember, Set.of(), waiting);
    }

    private static TakeoverCollection.Collected collected(
            Map<String, List<FastJournalRecord.Entry>> byMember, Set<String> gone,
            Set<String> waiting) {
        return new TakeoverCollection.Collected(byMember, gone, waiting);
    }

    /** Every step and hold call, in order, as one line each. */
    private static final class Fake implements TakeoverRun.Steps, TakeoverRun.Hold {
        final List<String> calls = new ArrayList<>();
        final Deque<TakeoverCollection.Collected> collections = new ArrayDeque<>();
        final List<List<RunKey>> recovered = new ArrayList<>();
        final Map<RunKey, Long> chainNext = new TreeMap<>();
        RosterDecisions.Outcome decisions = new RosterDecisions.Recorded(List.of());
        Map<RunKey, Long> released;
        int mayDecideAfter;
        long batchBytes = Long.MAX_VALUE;
        boolean recoverFails;
        private int collects;

        @Override
        public TakeoverCollection.Collected collect(List<Roster> stillOpen, long closedThrough) {
            if (++collects > 10) {
                throw new IllegalStateException("collected forever");
            }
            calls.add("collect");
            return collections.size() > 1 ? collections.poll() : collections.peek();
        }

        @Override
        public long chainNext(RunKey stream) {
            return chainNext.getOrDefault(stream, 0L);
        }

        @Override
        public Recovery recover(List<StreamDecision.Decided> batch) throws IOException {
            if (recoverFails) {
                calls.add("recover failed");
                throw new IOException("the chain append failed");
            }
            List<RunKey> streams = new ArrayList<>();
            for (StreamDecision.Decided d : batch) {
                streams.add(d.commits().isEmpty() ? d.voids().get(0).key()
                        : d.commits().get(0).key());
            }
            recovered.add(streams);
            calls.add("recover " + streams);
            return null;
        }

        @Override
        public RosterDecisions.Outcome recordDecisions(Map<RunKey, Long> resumeAt) {
            calls.add("decisions " + new TreeMap<>(resumeAt));
            return decisions;
        }

        @Override
        public void rosterWritten() {
            calls.add("rosterWritten");
        }

        @Override
        public int release(List<Roster> unclosed, Map<RunKey, Long> below) {
            calls.add("release");
            released = below;
            return 0;
        }

        @Override
        public int close(List<Roster> unclosed, Predicate<Roster> decided) {
            calls.add("close " + unclosed.stream().filter(decided).count());
            return 1;
        }

        @Override
        public void undecided(Collection<RunKey> streams) {
            calls.add("undecided " + new java.util.TreeSet<>(streams));
        }

        @Override
        public void everyMemberHeard() {
            calls.add("heard");
        }

        @Override
        public void decided(RunKey stream, long resumeAt) {
            calls.add("decided " + stream + "@" + resumeAt);
        }

        TakeoverRun run() {
            return new TakeoverRun(this, this, () -> {
                if (mayDecideAfter > 0) {
                    mayDecideAfter--;
                    return false;
                }
                return true;
            }, nanos -> calls.add("sleep " + nanos), Duration.ofMillis(5), BOUND, batchBytes);
        }
    }

    @Test
    void aBATCHTooLargeForOneUploadIsSplitByStreamBeforeAnyDecisionIsRecorded()
            throws Exception {
        // ⚠️ M13.33q: each record counts its id, payload and 16 bytes -- 21
        // here -- so 45 bytes hold two streams and not three.
        Fake f = new Fake();
        f.batchBytes = 45;
        f.collections.add(collected(Map.of("uid-m",
                List.of(at(7, S1, 0, 1), at(7, S2, 0, 1), at(7, SO, 0, 1))), Set.of()));

        f.run().run(List.of(term7(0)), 6, List.of());

        assertThat(f.recovered).containsExactly(List.of(S1, S2), List.of(SO));
        assertThat(f.calls).as("one decisions write, after every entry")
                .containsSubsequence("recover [" + S1 + ", " + S2 + "]", "recover [" + SO + "]",
                        "decisions {" + S1 + "=1, " + S2 + "=1, " + SO + "=1}");
    }

    @Test
    void aBUDGETMetExactlyKeepsTheStreamsInOneEntry() throws Exception {
        Fake f = new Fake();
        f.batchBytes = 42;
        f.collections.add(collected(Map.of("uid-m",
                List.of(at(7, S1, 0, 1), at(7, S2, 0, 1))), Set.of()));

        f.run().run(List.of(term7(0)), 6, List.of());

        assertThat(f.recovered).as("21 + 21 = 42: not over").containsExactly(List.of(S1, S2));
    }

    @Test
    void aSTREAMOverTheBudgetIsAnEntryOfItsOwn() throws Exception {
        Fake f = new Fake();
        f.batchBytes = 10;
        f.collections.add(collected(Map.of("uid-m",
                List.of(at(7, S1, 0, 1), at(7, S2, 0, 1))), Set.of()));

        f.run().run(List.of(term7(0)), 6, List.of());

        assertThat(f.recovered).containsExactly(List.of(S1), List.of(S2));
    }

    @Test
    void noUNCLOSEDTermLiftsTheHoldAndCollectsNothing() throws Exception {
        Fake f = new Fake();

        TakeoverRun.Outcome got = f.run().run(List.of(), 6, List.of());

        assertThat(got).isEqualTo(new TakeoverRun.Done(Map.of(), 0));
        assertThat(f.calls).containsExactly("heard");
    }

    @Test
    void decidedSTREAMSAreRecoveredRecordedReReadThenResumedThenReleasedAndClosed()
            throws Exception {
        Fake f = new Fake();
        f.chainNext.put(S1, 10L);
        f.collections.add(collected(Map.of("uid-m", List.of(at(7, S1, 10, 2), at(7, S2, 0, 1))),
                Set.of()));

        TakeoverRun.Outcome got = f.run().run(List.of(term7(0)), 6, List.of());

        assertThat(f.calls).containsExactly("collect", "undecided [" + S1 + ", " + S2 + "]",
                "heard", "recover [" + S1 + ", " + S2 + "]",
                "decisions {" + S1 + "=12, " + S2 + "=1}", "rosterWritten",
                "decided " + S1 + "@12", "decided " + S2 + "@1", "release", "close 1");
        assertThat(f.released).isEqualTo(Map.of(S1, 12L, S2, 1L));
        assertThat(got).isEqualTo(new TakeoverRun.Done(Map.of(S1, 12L, S2, 1L), 1));
    }

    @Test
    void aMEMBERStillWaitedForKeepsTheHoldAndIsCollectedAgain() throws Exception {
        Fake f = new Fake();
        f.collections.add(collected(Map.of("uid-n", List.of(at(7, S1, 0, 1))), Set.of("uid-m")));
        f.collections.add(collected(Map.of("uid-n", List.of(at(7, S1, 0, 1)),
                "uid-m", List.of(at(7, S1, 1, 1))), Set.of()));

        f.run().run(List.of(term7(1)), 6, List.of());

        assertThat(f.calls).as("q_min 1 and m unanswered: S1 waits, the hold kept")
                .startsWith("collect", "undecided [" + S1 + "]", "sleep 5000000", "collect",
                        "undecided [" + S1 + "]", "heard", "recover [" + S1 + "]");
        assertThat(f.calls).contains("decided " + S1 + "@2");
    }

    @Test
    void aSTREAMDecidedEarlyIsNeverDecidedAgain() throws Exception {
        Fake f = new Fake();
        f.collections.add(collected(Map.of("uid-n", List.of(at(7, S1, 0, 1), at(7, SO, 0, 1))),
                Set.of("uid-m")));
        f.collections.add(collected(Map.of("uid-n", List.of(at(7, S1, 0, 1), at(7, SO, 0, 1))),
                Set.of()));

        f.run().run(List.of(term7(1)), 6, List.of());

        assertThat(f.recovered).as("OTHER has no recorded q: it decides early")
                .containsExactly(List.of(SO), List.of(S1));
        assertThat(f.calls).contains("undecided [" + S1 + "]");
    }

    @Test
    void nothingIsDecidedOrLiftedUntilTheWaitHasPassed() throws Exception {
        Fake f = new Fake();
        f.mayDecideAfter = 2;
        f.collections.add(collected(Map.of("uid-m", List.of(at(7, S1, 0, 1))), Set.of()));

        f.run().run(List.of(term7(0)), 6, List.of());

        assertThat(f.calls).startsWith("collect", "undecided [" + S1 + "]", "sleep 5000000",
                "sleep 5000000", "heard", "recover [" + S1 + "]");
    }

    @Test
    void aDEPOSEDDecisionsWriteStopsBeforeAnythingResumes() throws Exception {
        Fake f = new Fake();
        f.decisions = new RosterDecisions.Deposed(9);
        f.collections.add(collected(Map.of("uid-m", List.of(at(7, S1, 0, 1))), Set.of()));

        TakeoverRun.Outcome got = f.run().run(List.of(term7(0)), 6, List.of());

        assertThat(got).isEqualTo(new TakeoverRun.Deposed(9));
        assertThat(f.calls.get(f.calls.size() - 1)).startsWith("decisions");
    }

    @Test
    void theSUCCESSORsOwnEntriesOfUnclosedTermsAreDecidedToo() throws Exception {
        Fake f = new Fake();
        f.collections.add(collected(Map.of(), Set.of()));

        f.run().run(List.of(term7(0)), 6, List.of(at(7, S1, 0, 2), at(8, S2, 0, 1),
                at(6, SO, 0, 1)));

        assertThat(f.recovered).as("only term 7 is unclosed: 8 is the successor's, 6 closed")
                .containsExactly(List.of(S1));
    }

    @Test
    void noDECIDEDStreamSendsNoRelease() throws Exception {
        Fake f = new Fake();
        f.collections.add(collected(Map.of("uid-m", List.of()), Set.of()));

        TakeoverRun.Outcome got = f.run().run(List.of(term7(0)), 6, List.of());

        assertThat(f.calls).containsExactly("collect", "undecided []", "heard", "close 1");
        assertThat(got).isEqualTo(new TakeoverRun.Done(Map.of(), 1));
    }

    @Test
    void withQUORUMLossTheHolesAreVoidedAndTheStreamResumesAtC0PlusB() throws Exception {
        Fake f = new Fake();
        f.collections.add(collected(Map.of("uid-m", List.of(at(7, S1, 0, 1), at(7, S1, 2, 1))),
                Set.of("uid-l"), Set.of()));

        f.run().run(List.of(term7(1)), 6, List.of());

        assertThat(f.calls).as("q 1 and term 7's leader gone: I lost quorum")
                .contains("decisions {" + S1 + "=10}", "decided " + S1 + "@10");
    }

    @Test
    void aGONEMemberCountsAgainstAnEarlyDecisionWithTheUnanswered() throws Exception {
        Fake f = new Fake();
        f.collections.add(collected(Map.of(), Set.of("uid-n"), Set.of("uid-m")));
        f.collections.add(collected(Map.of(), Set.of("uid-n"), Set.of()));

        f.run().run(List.of(term7(2)), 6, List.of(at(7, S1, 0, 1)));

        assertThat(f.calls).as("m unanswered in az-b and n gone in az-c: 2 zones of q_min 2")
                .startsWith("collect", "undecided [" + S1 + "]", "sleep 5000000", "collect");
    }

    @Test
    void aMEMBERSilentAfterItAnsweredStillHasItsStreamsDecided() throws Exception {
        Fake f = new Fake();
        f.collections.add(collected(Map.of("uid-m", List.of(at(7, S1, 0, 1))), Set.of("uid-n")));
        f.collections.add(collected(Map.of("uid-n", List.of()), Set.of("uid-m")));

        TakeoverRun.Outcome got = f.run().run(List.of(term7(1)), 6, List.of());

        assertThat(f.recovered).containsExactly(List.of(S1));
        assertThat(got).isEqualTo(new TakeoverRun.Done(Map.of(S1, 1L), 1));
    }

    @Test
    void anOWNEntryARosterDecisionSupersededIsDropped() throws Exception {
        Fake f = new Fake();
        f.collections.add(collected(Map.of(), Set.of()));
        Roster t = term7(0);
        Roster decided = new Roster(t.epoch(), t.predecessor(), t.leader(), t.members(),
                t.termRecord(), List.of(new Roster.Decision(0, S1, 0)), 0, 0, false);

        f.run().run(List.of(decided), 6, List.of(at(7, S1, 0, 1)));

        assertThat(f.calls).containsExactly("collect", "undecided []", "heard", "close 1");
    }

    @Test
    void aDECISIONWritingNothingIsRecordedWithoutARecoveryEntry() throws Exception {
        Fake f = new Fake();
        f.collections.add(collected(Map.of("uid-m", List.of(at(7, S1, 5, 1))), Set.of()));

        f.run().run(List.of(term7(0)), 6, List.of());

        assertThat(f.calls).as("a hole at c0 = 0: the entry at 5 discarded, nothing written")
                .containsExactly("collect", "undecided [" + S1 + "]", "heard",
                        "decisions {" + S1 + "=0}", "rosterWritten", "decided " + S1 + "@0",
                        "release", "close 1");
    }

    @Test
    void aFAILEDRecoveryRecordsAndResumesNothing() {
        Fake f = new Fake();
        f.recoverFails = true;
        f.collections.add(collected(Map.of("uid-m", List.of(at(7, S1, 0, 1))), Set.of()));

        assertThatThrownBy(() -> f.run().run(List.of(term7(0)), 6, List.of()))
                .isInstanceOf(IOException.class);
        assertThat(f.calls.get(f.calls.size() - 1)).isEqualTo("recover failed");
    }
}
