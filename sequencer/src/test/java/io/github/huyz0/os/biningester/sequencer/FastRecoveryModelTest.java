// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The takeover's decision against a model of what happened (M13.33y; SPEC
 * criterion 13, ADR-0081 §5 step 5): over seeds, one unclosed term on three
 * zones -- members departed or not, a leader -- assigns a stream's entries up
 * to its bound under {@code q} 1 to 3, each copied to the leader and to
 * members in {@code q - 1} other zones (some never completing), exposed in
 * offset order; then each pod is alive, gone (its UID deleted, its journal
 * lost) or unreachable (its UID surviving, its journal kept).
 * {@link QuorumLoss} and {@link StreamDecision} decide from what survivors
 * can know -- the roster, the deleted UIDs, the answered pods' entries -- and
 * an oracle that reads the MODEL holds them to criterion 13:
 *
 * <ul>
 *   <li>the index lost quorum exactly when the oracle's own copy of the
 *       predicate, over the model's pods rather than the roster, says so;
 *   <li>every exposed offset is committed with its content or lies in a void,
 *       and a void covers it only where every copy of it is gone;
 *   <li>a void covers a never-exposed offset only of an index that lost
 *       quorum, never an offset whose entry survives on a pod whose UID
 *       survives, and lies within {@code [c0, c0 + B)};
 *   <li>what is committed is the model's entry at its offset;
 *   <li>(liveness) once every member has answered or is gone, it decides.
 * </ul>
 *
 * <p>⚠️ THE DECISION LAYER, one term and one stream: the takeover's walk over
 * several unclosed terms, recorded {@code wal_quorum} changes, the named
 * cases needing a live fleet, and the commit-protocol simulation are the
 * rest of M13.33u. The named cases this layer can state are here (M13.33u1),
 * each asserted by the same oracle.
 */
class FastRecoveryModelTest {

    private static final UUID I = new UUID(1, 1);
    private static final RunKey S = new RunKey(I, 0);
    private static final long B = 6;
    private static final List<String> ZONES = List.of("az1", "az2", "az3");

    private enum Fate { ALIVE, GONE, UNREACHABLE }

    private record Pod(Roster.Incarnation incarnation, boolean departed, Fate fate) {
        String uid() {
            return incarnation.podUid();
        }

        String az() {
            return incarnation.az();
        }
    }

    /** One assigned entry, the pods holding its copy, and whether it was exposed. */
    private record Written(FastJournalRecord.Entry entry, Set<String> holders, boolean exposed) {
        long end() {
            return entry.firstOffset() + entry.records().size();
        }
    }

    private record Model(List<Pod> pods, Pod leader, int q, long c0, List<Written> written) {
        Pod pod(String uid) {
            return pods.stream().filter(p -> p.uid().equals(uid)).findFirst().orElseThrow();
        }

        boolean survives(Written w) {
            return w.holders().stream().anyMatch(uid -> pod(uid).fate() != Fate.GONE);
        }

        /**
         * ⚠️ THE ORACLE's OWN COPY of the quorum-loss predicate (ADR-0081 §5
         * step 5), over the model's pods and fates, never the roster: the
         * leader gone, and gone undeparted members in at least {@code q - 1}
         * zones other than the leader's.
         */
        boolean lost() {
            if (leader.fate() != Fate.GONE) {
                return false;
            }
            Set<String> zones = new HashSet<>();
            for (Pod p : pods) {
                if (p != leader && !p.departed() && p.fate() == Fate.GONE
                        && !p.az().equals(leader.az())) {
                    zones.add(p.az());
                }
            }
            return zones.size() >= q - 1;
        }
    }

    private static Model model(long seed) {
        Random r = new Random(seed);
        List<Pod> undecided = new ArrayList<>();
        for (String az : ZONES) {
            int n = 1 + r.nextInt(3);
            for (int i = 0; i < n; i++) {
                String name = "p-" + az + "-" + i;
                undecided.add(new Pod(new Roster.Incarnation(name, "uid-" + name, az, name),
                        false, Fate.ALIVE));
            }
        }
        Pod leaderPick = undecided.get(r.nextInt(undecided.size()));
        List<Pod> pods = new ArrayList<>();
        Pod leader = null;
        for (Pod p : undecided) {
            boolean isLeader = p == leaderPick;
            double f = r.nextDouble();
            Fate fate = f < 0.5 ? Fate.ALIVE : f < 0.8 ? Fate.GONE : Fate.UNREACHABLE;
            Pod decided = new Pod(p.incarnation(), !isLeader && r.nextDouble() < 0.15, fate);
            pods.add(decided);
            if (isLeader) {
                leader = decided;
            }
        }
        Set<String> zonesHeld = new HashSet<>();
        for (Pod p : pods) {
            if (!p.departed()) {
                zonesHeld.add(p.az());
            }
        }
        int q = Math.min(1 + r.nextInt(3), zonesHeld.size());
        long c0 = r.nextInt(4);
        List<Written> written = new ArrayList<>();
        long next = c0;
        boolean exposing = true;
        int seq = 0;
        while (next < c0 + B && r.nextDouble() < 0.85) {
            int size = (int) Math.min(1 + r.nextInt(2), c0 + B - next);
            List<SegmentRecord> records = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                records.add(new SegmentRecord("d" + (next + i), OpType.INDEX,
                        OptionalLong.empty(), new byte[] {1}));
            }
            FastJournalRecord.Entry entry = new FastJournalRecord.Entry(1, S, next, q, seq,
                    new FastJournalRecord.IdempotencyKey("w", new UUID(9, seed), seq++), records);
            List<String> intended = new ArrayList<>(List.of(leader.uid()));
            List<String> others = new ArrayList<>(ZONES);
            others.remove(leader.az());
            java.util.Collections.shuffle(others, r);
            for (String az : others) {
                if (intended.size() == q) {
                    break;
                }
                List<Pod> in = pods.stream()
                        .filter(p -> p.az().equals(az) && !p.departed()).toList();
                if (!in.isEmpty()) {
                    intended.add(in.get(r.nextInt(in.size())).uid());
                }
            }
            boolean complete = intended.size() == q && r.nextDouble() < 0.75;
            Set<String> holders = new HashSet<>(List.of(leader.uid()));
            for (String uid : intended.subList(1, intended.size())) {
                if (complete || r.nextBoolean()) {
                    holders.add(uid);
                }
            }
            if (!complete && holders.size() == intended.size() && intended.size() > 1) {
                holders.remove(intended.getLast());
            }
            exposing = exposing && complete && r.nextDouble() < 0.9;
            written.add(new Written(entry, holders, exposing));
            next += size;
        }
        return new Model(pods, leader, q, c0, written);
    }

    /** What the protocol decides from what survivors can know. */
    private static StreamDecision.Outcome decide(Model m) {
        List<Roster.Member> members = m.pods().stream().map(p -> new Roster.Member(
                p.incarnation(), p.departed() ? Roster.State.DEPARTED : Roster.State.ROSTERED))
                .toList();
        Roster term = new Roster(1, -1, m.leader().incarnation(), members,
                List.of(new Roster.TermRecord(0, new TreeMap<>(Map.of(I, m.q())))), List.of(),
                0, 0, false);
        Set<String> gone = new HashSet<>();
        Set<String> unanswered = new HashSet<>();
        for (Pod p : m.pods()) {
            if (p.fate() == Fate.GONE) {
                gone.add(p.uid());
            } else if (p.fate() == Fate.UNREACHABLE) {
                unanswered.add(p.uid());
            }
        }
        List<FastJournalRecord.Entry> live = new ArrayList<>();
        for (Written w : m.written()) {
            for (String uid : w.holders()) {
                if (m.pod(uid).fate() == Fate.ALIVE) {
                    live.add(w.entry());
                }
            }
        }
        Set<String> unansweredOrGone = new HashSet<>(gone);
        unansweredOrGone.addAll(unanswered);
        boolean lost = QuorumLoss.lost(List.of(term), I, gone);
        assertThat(lost).as("the protocol's predicate is the oracle's").isEqualTo(m.lost());
        return StreamDecision.decide(S, m.c0(), B, live, lost, unanswered.isEmpty(),
                QuorumLoss.mayDecideEarly(List.of(term), I, unansweredOrGone));
    }

    /** Criterion 13's safety, against the model. */
    private static void holds(Model m, StreamDecision.Decided d) {
        Map<Long, Written> byOffset = new LinkedHashMap<>();
        m.written().forEach(w -> byOffset.put(w.entry().firstOffset(), w));
        for (FastJournalRecord.Entry c : d.commits()) {
            assertThat(byOffset.get(c.firstOffset())).as("a commit is the model's entry")
                    .isNotNull().extracting(Written::entry).isEqualTo(c);
        }
        for (Recovery.VoidRange v : d.voids()) {
            assertThat(m.lost()).as("a void only after quorum loss").isTrue();
            assertThat(v.fromOffset()).isGreaterThanOrEqualTo(m.c0());
            assertThat(v.toOffsetExclusive()).isLessThanOrEqualTo(m.c0() + B);
            for (Written w : m.written()) {
                boolean covered = w.entry().firstOffset() < v.toOffsetExclusive()
                        && w.end() > v.fromOffset();
                if (covered) {
                    assertThat(m.survives(w)).as("no void over an entry that survives")
                            .isFalse();
                }
            }
        }
        for (Written w : m.written()) {
            if (!w.exposed()) {
                continue;
            }
            boolean committed = d.commits().contains(w.entry());
            boolean voided = d.voids().stream().anyMatch(v -> v.fromOffset()
                    <= w.entry().firstOffset() && w.end() <= v.toOffsetExclusive());
            assertThat(committed || voided).as("exposed offset %d kept",
                    w.entry().firstOffset()).isTrue();
        }
    }

    @Test
    void overSEEDSTheDecisionHoldsTheModelsSafetyAndDecidesOnceAllAnswered() {
        int voided = 0;
        int decidedEarly = 0;
        int waited = 0;
        for (long seed = 0; seed < 20_000; seed++) {
            Model m = model(seed);
            StreamDecision.Outcome outcome = decide(m);
            boolean allAnswered = m.pods().stream().noneMatch(p -> p.fate() == Fate.UNREACHABLE);
            if (outcome instanceof StreamDecision.Decided d) {
                holds(m, d);
                voided += d.voids().isEmpty() ? 0 : 1;
                decidedEarly += allAnswered ? 0 : 1;
            } else {
                assertThat(allAnswered).as("seed %d: liveness -- all answered, it decides", seed)
                        .isFalse();
                waited++;
            }
        }
        // ⚠️ NOT VACUOUS: each branch the oracle judges is reached.
        assertThat(voided).as("quorum loss decided").isGreaterThan(500);
        assertThat(decidedEarly).as("decided with a member unanswered").isGreaterThan(500);
        assertThat(waited).as("waited on an unanswered member").isGreaterThan(500);
    }

    @Test
    void theAZ1AZ2CaseIsQuorumLossThoughBothZonesKeepOtherPods() {
        // ⚠️ SPEC criterion 13's first named case: at q = 2, the leader's pod in
        // az1 and a replica's in az2 lost while both zones keep other pods.
        Pod l = new Pod(new Roster.Incarnation("l", "uid-l", "az1", "l"), false, Fate.GONE);
        Pod a = new Pod(new Roster.Incarnation("a", "uid-a", "az1", "a"), false, Fate.ALIVE);
        Pod r = new Pod(new Roster.Incarnation("r", "uid-r", "az2", "r"), false, Fate.GONE);
        Pod b = new Pod(new Roster.Incarnation("b", "uid-b", "az2", "b"), false, Fate.ALIVE);
        FastJournalRecord.Entry e = new FastJournalRecord.Entry(1, S, 0, 2, 0,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), 0),
                List.of(new SegmentRecord("d0", OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1})));
        Model m = new Model(List.of(l, a, r, b), l, 2, 0,
                List.of(new Written(e, Set.of("uid-l", "uid-r"), true)));

        StreamDecision.Outcome outcome = decide(m);

        assertThat(m.lost()).isTrue();
        assertThat(outcome).isInstanceOfSatisfying(StreamDecision.Decided.class, d -> {
            holds(m, d);
            assertThat(d.voids()).as("the exposed entry's every copy gone: voided")
                    .containsExactly(new Recovery.VoidRange(S, 0, B));
        });
    }

    // ⚠️ M13.33u1: SPEC criterion 13's named cases this layer can state.

    private static Pod pod(String name, String az, Fate fate) {
        return new Pod(new Roster.Incarnation(name, "uid-" + name, az, name), false, fate);
    }

    private static FastJournalRecord.Entry entry(long first, int size, int seq) {
        List<SegmentRecord> records = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            records.add(new SegmentRecord("d" + (first + i), OpType.INDEX, OptionalLong.empty(),
                    new byte[] {1}));
        }
        return new FastJournalRecord.Entry(1, S, first, 2, seq,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), seq), records);
    }

    private static Written held(FastJournalRecord.Entry e, boolean exposed, Pod... holders) {
        Set<String> uids = new HashSet<>();
        for (Pod h : holders) {
            uids.add(h.uid());
        }
        return new Written(e, uids, exposed);
    }

    private static StreamDecision.Decided decided(Model m) {
        StreamDecision.Outcome outcome = decide(m);
        assertThat(outcome).isInstanceOf(StreamDecision.Decided.class);
        StreamDecision.Decided d = (StreamDecision.Decided) outcome;
        holds(m, d);
        return d;
    }

    @Test
    void atQ2TheLeaderLostAndTheOnlyHolderPARTITIONEDWaitsThenHealedCommits() {
        // the only surviving holder partitioned (its UID surviving), then healed;
        // unready but reachable is the healed half at this layer -- it answers
        Pod l = pod("l", "az1", Fate.GONE);
        Pod a = pod("a", "az1", Fate.ALIVE);
        Pod c = pod("c", "az3", Fate.ALIVE);
        FastJournalRecord.Entry e = entry(0, 2, 0);
        Pod partitioned = pod("r", "az2", Fate.UNREACHABLE);
        Model cut = new Model(List.of(l, a, partitioned, c), l, 2, 0,
                List.of(held(e, true, l, partitioned)));
        Pod healed = pod("r", "az2", Fate.ALIVE);
        Model whole = new Model(List.of(l, a, healed, c), l, 2, 0,
                List.of(held(e, true, l, healed)));

        assertThat(decide(cut)).as("its copy may be the only one: no early decision")
                .isInstanceOf(StreamDecision.Wait.class);
        StreamDecision.Decided d = decided(whole);

        assertThat(d.commits()).containsExactly(e);
        assertThat(d.voids()).isEmpty();
    }

    @Test
    void entry102AnsweredBefore101AndTheLeaderLostWith101OnNoPodCommitsNothingAboveIt() {
        Pod l = pod("l", "az1", Fate.GONE);
        Pod r = pod("r", "az2", Fate.ALIVE);
        Pod c = pod("c", "az3", Fate.ALIVE);
        FastJournalRecord.Entry e101 = entry(101, 1, 0);
        FastJournalRecord.Entry e102 = entry(102, 1, 1);
        Model m = new Model(List.of(l, r, c), l, 2, 101,
                List.of(held(e101, false, l), held(e102, false, l, r)));

        StreamDecision.Decided d = decided(m);

        assertThat(m.lost()).as("az2 and az3 keep their pods: no quorum loss").isFalse();
        assertThat(d.commits()).as("never 102 above an unrecovered 101").isEmpty();
        assertThat(d.voids()).isEmpty();
        assertThat(d.resumeAt()).isEqualTo(101);
    }

    @Test
    void aQ1StreamFilledToItsBOUNDAtTheLeadersLossIsVoidedToTheBoundNotPastIt() {
        Pod l = pod("l", "az1", Fate.GONE);
        Pod c = pod("c", "az2", Fate.ALIVE);
        List<Written> written = new ArrayList<>();
        for (int i = 0; i < B; i++) {
            written.add(held(entry(2 + i, 1, i), i < 3, l));
        }
        Model m = new Model(List.of(l, c), l, 1, 2, written);

        StreamDecision.Decided d = decided(m);

        assertThat(d.voids()).containsExactly(new Recovery.VoidRange(S, 2, 2 + B));
        assertThat(d.resumeAt()).as("offset c0 + B is the next assignable").isEqualTo(2 + B);
    }

    @Test
    void aQ3IndexOnThreeZonesWithTheLeadersZoneLOSTVoidsNothingAndCommitsTheExposed() {
        // ⚠️ az2 loses a pod too: counting the leader's own zone would make two
        Pod l = pod("l", "az1", Fate.GONE);
        Pod a = pod("a", "az1", Fate.GONE);
        Pod r = pod("r", "az2", Fate.ALIVE);
        Pod gone = pod("g", "az2", Fate.GONE);
        Pod c = pod("c", "az3", Fate.ALIVE);
        FastJournalRecord.Entry e0 = entry(0, 2, 0);
        FastJournalRecord.Entry e1 = entry(2, 1, 1);
        Model m = new Model(List.of(l, a, r, gone, c), l, 3, 0,
                List.of(held(e0, true, l, r, c), held(e1, true, l, r, c)));

        StreamDecision.Decided d = decided(m);

        assertThat(m.lost()).as("one zone short of q - 1 others").isFalse();
        assertThat(d.commits()).containsExactly(e0, e1);
        assertThat(d.voids()).isEmpty();
    }

    @Test
    void anIDLEFullyReleasedQ1IndexLosingItsLeaderMayBeVoidedWithinTheBoundOnly() {
        Pod l = pod("l", "az1", Fate.GONE);
        Pod c = pod("c", "az2", Fate.ALIVE);
        Model m = new Model(List.of(l, c), l, 1, 5, List.of());

        StreamDecision.Decided d = decided(m);

        assertThat(d.commits()).isEmpty();
        assertThat(d.voids()).as("from c0, never below it")
                .containsExactly(new Recovery.VoidRange(S, 5, 5 + B));
    }

    @Test
    void atQ2TheLeaderAndAnAZ2PodWhoseCopiesWereALLReleasedLostIsQuorumLossByTheRoster() {
        Pod l = pod("l", "az1", Fate.GONE);
        Pod a = pod("a", "az1", Fate.ALIVE);
        Pod released = pod("r", "az2", Fate.GONE);
        Pod b = pod("b", "az2", Fate.ALIVE);
        Pod c = pod("c", "az3", Fate.ALIVE);
        Model m = new Model(List.of(l, a, released, b, c), l, 2, 3, List.of());

        StreamDecision.Decided d = decided(m);

        assertThat(m.lost()).as("a rostered pod, holding nothing, counts").isTrue();
        assertThat(d.voids()).containsExactly(new Recovery.VoidRange(S, 3, 3 + B));
    }

    @Test
    void aQ1IndexLosingItsLeadersZoneVoidsTheAckedTailAndNothingBelowACommittedStream() {
        Pod l = pod("l", "az1", Fate.GONE);
        Pod a = pod("a", "az1", Fate.GONE);
        Pod c = pod("c", "az2", Fate.ALIVE);
        FastJournalRecord.Entry e0 = entry(0, 2, 0);
        Model tail = new Model(List.of(l, a, c), l, 1, 0, List.of(held(e0, true, l)));
        Model committed = new Model(List.of(l, a, c), l, 1, 4, List.of());

        StreamDecision.Decided withTail = decided(tail);
        StreamDecision.Decided fully = decided(committed);

        assertThat(withTail.voids()).as("the acked tail's every copy gone")
                .containsExactly(new Recovery.VoidRange(S, 0, B));
        assertThat(fully.voids()).as("no committed offset voided")
                .containsExactly(new Recovery.VoidRange(S, 4, 4 + B));
    }
}
