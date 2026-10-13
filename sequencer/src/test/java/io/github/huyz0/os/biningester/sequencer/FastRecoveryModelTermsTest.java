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
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The takeover's decision over SEVERAL unclosed terms, against a model of
 * what happened (M13.33z; SPEC criterion 13, ADR-0081 §5 step 5): over
 * seeds, one to three unclosed terms, each with its own leader and members,
 * departures, and {@code wal_quorum} recorded once or twice for the index --
 * raised or lowered within the term, or not recorded at all. One of them
 * assigned the stream's entries, each under one of its recorded values and
 * copied by that value. Each pod is then alive, gone or unreachable.
 *
 * <p>Beyond M13.33y's one term, the oracle -- its own copy of the predicate
 * evaluated per term over the MODEL's recorded values, leaders, members and
 * fates -- also holds the decision to criterion 13's "no exposed offset
 * assigned again": assignment resumes past every exposed and every committed
 * offset, and each offset is committed once (M13.33y review round 1, T1 and
 * T3). And it counts that the seeds reach a void over an exposed entry, a
 * surviving entry committed above a hole after quorum loss, and each
 * {@code q_min} (its T2).
 */
class FastRecoveryModelTermsTest {

    private static final UUID I = new UUID(1, 1);
    private static final RunKey S = new RunKey(I, 0);
    private static final long B = 6;
    private static final List<String> ZONES = List.of("az1", "az2", "az3");

    private enum Fate { ALIVE, GONE, UNREACHABLE }

    private record Pod(Roster.Incarnation incarnation, Fate fate) {
        String uid() {
            return incarnation.podUid();
        }
    }

    /** One unclosed term: its leader, members (departed or not) and recorded values. */
    private record Term(long epoch, Pod leader, Map<Pod, Boolean> departedByMember,
            List<Integer> recorded) {
        boolean holdsCopies(Pod p) {
            return departedByMember.containsKey(p) && !departedByMember.get(p);
        }

        /** ⚠️ THE ORACLE's PREDICATE for this term, over the model, never the roster. */
        boolean lost() {
            if (recorded.isEmpty() || leader.fate() != Fate.GONE) {
                return false;
            }
            int qMin = recorded.stream().mapToInt(Integer::intValue).min().orElseThrow();
            Set<String> zones = new HashSet<>();
            departedByMember.forEach((p, departed) -> {
                if (p != leader && !departed && p.fate() == Fate.GONE
                        && !p.incarnation().az().equals(leader.incarnation().az())) {
                    zones.add(p.incarnation().az());
                }
            });
            return zones.size() >= qMin - 1;
        }

        Roster roster() {
            List<Roster.Member> members = new ArrayList<>();
            departedByMember.forEach((p, departed) -> members.add(new Roster.Member(
                    p.incarnation(), departed ? Roster.State.DEPARTED : Roster.State.ROSTERED)));
            List<Roster.TermRecord> records = new ArrayList<>();
            for (int i = 0; i < recorded.size(); i++) {
                records.add(new Roster.TermRecord(i, new TreeMap<>(Map.of(I, recorded.get(i)))));
            }
            return new Roster(epoch, epoch - 1, leader.incarnation(), members, records,
                    List.of(), 0, 0, false);
        }
    }

    private record Written(FastJournalRecord.Entry entry, Set<Pod> holders, boolean exposed) {
        long end() {
            return entry.firstOffset() + entry.records().size();
        }

        boolean survives() {
            return holders.stream().anyMatch(p -> p.fate() != Fate.GONE);
        }
    }

    private record Model(List<Pod> pods, List<Term> terms, long c0, List<Written> written) {
        boolean lost() {
            return terms.stream().anyMatch(Term::lost);
        }

        boolean allAnswered() {
            return terms.stream().flatMap(t -> t.departedByMember().keySet().stream())
                    .noneMatch(p -> p.fate() == Fate.UNREACHABLE);
        }
    }

    private static Model model(long seed) {
        Random r = new Random(seed);
        List<Pod> pods = new ArrayList<>();
        for (String az : ZONES) {
            int n = 1 + r.nextInt(3);
            for (int i = 0; i < n; i++) {
                String name = "p-" + az + "-" + i;
                double f = r.nextDouble();
                pods.add(new Pod(new Roster.Incarnation(name, "uid-" + name, az, name),
                        f < 0.5 ? Fate.ALIVE : f < 0.8 ? Fate.GONE : Fate.UNREACHABLE));
            }
        }
        int count = 1 + r.nextInt(3);
        int writing = r.nextInt(count);
        List<Term> terms = new ArrayList<>();
        for (int t = 0; t < count; t++) {
            Pod leader = pods.get(r.nextInt(pods.size()));
            Map<Pod, Boolean> members = new java.util.LinkedHashMap<>();
            members.put(leader, false);
            for (Pod p : pods) {
                if (p != leader && r.nextDouble() < 0.8) {
                    members.put(p, r.nextDouble() < 0.15);
                }
            }
            Set<String> zones = new HashSet<>();
            members.forEach((p, departed) -> {
                if (!departed) {
                    zones.add(p.incarnation().az());
                }
            });
            List<Integer> recorded = new ArrayList<>();
            if (t == writing || r.nextDouble() < 0.8) {
                int values = 1 + r.nextInt(2); // raised or lowered within the term
                for (int v = 0; v < values; v++) {
                    recorded.add(Math.min(1 + r.nextInt(3), zones.size()));
                }
            }
            terms.add(new Term(10 + t, leader, members, recorded));
        }
        Term writer = terms.get(writing);
        long c0 = r.nextInt(4);
        List<Written> written = new ArrayList<>();
        long next = c0;
        boolean exposing = true;
        int seq = 0;
        while (next < c0 + B && r.nextDouble() < 0.85) {
            int size = (int) Math.min(1 + r.nextInt(2), c0 + B - next);
            int q = writer.recorded().get(r.nextInt(writer.recorded().size()));
            List<SegmentRecord> records = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                records.add(new SegmentRecord("d" + (next + i), OpType.INDEX,
                        OptionalLong.empty(), new byte[] {1}));
            }
            FastJournalRecord.Entry entry = new FastJournalRecord.Entry(writer.epoch(), S, next,
                    q, seq, new FastJournalRecord.IdempotencyKey("w", new UUID(9, seed), seq++),
                    records);
            List<Pod> intended = new ArrayList<>(List.of(writer.leader()));
            List<String> others = new ArrayList<>(ZONES);
            others.remove(writer.leader().incarnation().az());
            java.util.Collections.shuffle(others, r);
            for (String az : others) {
                if (intended.size() == q) {
                    break;
                }
                List<Pod> in = pods.stream().filter(p -> p.incarnation().az().equals(az)
                        && writer.holdsCopies(p)).toList();
                if (!in.isEmpty()) {
                    intended.add(in.get(r.nextInt(in.size())));
                }
            }
            boolean complete = intended.size() == q && r.nextDouble() < 0.75;
            Set<Pod> holders = new HashSet<>(List.of(writer.leader()));
            for (Pod p : intended.subList(1, intended.size())) {
                if (complete || r.nextBoolean()) {
                    holders.add(p);
                }
            }
            if (!complete && holders.size() == intended.size() && intended.size() > 1) {
                holders.remove(intended.getLast());
            }
            exposing = exposing && complete && r.nextDouble() < 0.9;
            written.add(new Written(entry, holders, exposing));
            next += size;
        }
        return new Model(pods, terms, c0, written);
    }

    private static StreamDecision.Outcome decide(Model m) {
        List<Roster> unclosed = m.terms().stream().map(Term::roster).toList();
        Set<String> gone = new HashSet<>();
        Set<String> unansweredOrGone = new HashSet<>();
        boolean allAnswered = true;
        for (Pod p : m.pods()) {
            boolean member = m.terms().stream()
                    .anyMatch(t -> t.departedByMember().containsKey(p));
            if (p.fate() == Fate.GONE) {
                gone.add(p.uid());
                unansweredOrGone.add(p.uid());
            } else if (p.fate() == Fate.UNREACHABLE && member) {
                unansweredOrGone.add(p.uid());
                allAnswered = false;
            }
        }
        List<FastJournalRecord.Entry> live = new ArrayList<>();
        for (Written w : m.written()) {
            for (Pod p : w.holders()) {
                if (p.fate() == Fate.ALIVE) {
                    live.add(w.entry());
                }
            }
        }
        boolean lost = QuorumLoss.lost(unclosed, I, gone);
        assertThat(lost).as("the protocol's predicate is the oracle's").isEqualTo(m.lost());
        return StreamDecision.decide(S, m.c0(), B, live, lost, allAnswered,
                QuorumLoss.mayDecideEarly(unclosed, I, unansweredOrGone));
    }

    /** Criterion 13's safety, against the model; what it reached, counted. */
    private static void holds(Model m, StreamDecision.Decided d, int[] reached) {
        long previous = Long.MIN_VALUE;
        for (FastJournalRecord.Entry c : d.commits()) {
            assertThat(c.firstOffset()).as("each offset committed once, in order")
                    .isGreaterThan(previous);
            previous = c.firstOffset();
            assertThat(m.written()).extracting(Written::entry).as("the model's entry")
                    .contains(c);
            assertThat(c.firstOffset() + c.records().size()).as("never past the resume")
                    .isLessThanOrEqualTo(d.resumeAt());
        }
        for (Recovery.VoidRange v : d.voids()) {
            assertThat(m.lost()).as("a void only after quorum loss").isTrue();
            assertThat(v.fromOffset()).isGreaterThanOrEqualTo(m.c0());
            assertThat(v.toOffsetExclusive()).isLessThanOrEqualTo(m.c0() + B)
                    .isLessThanOrEqualTo(d.resumeAt());
            for (Written w : m.written()) {
                if (w.entry().firstOffset() < v.toOffsetExclusive()
                        && w.end() > v.fromOffset()) {
                    assertThat(w.survives()).as("no void over an entry that survives")
                            .isFalse();
                    reached[0] += w.exposed() ? 1 : 0;
                }
            }
        }
        boolean hole = false;
        long expect = m.c0();
        for (Written w : m.written()) {
            boolean committed = d.commits().contains(w.entry());
            if (committed && hole && m.lost()) {
                reached[1]++;
            }
            hole |= !committed;
            if (!w.exposed()) {
                continue;
            }
            boolean voided = d.voids().stream().anyMatch(v -> v.fromOffset()
                    <= w.entry().firstOffset() && w.end() <= v.toOffsetExclusive());
            assertThat(committed || voided).as("exposed offset %d kept",
                    w.entry().firstOffset()).isTrue();
            assertThat(w.end()).as("an exposed offset never assigned again")
                    .isLessThanOrEqualTo(d.resumeAt());
            expect = w.end();
        }
        assertThat(d.resumeAt()).isGreaterThanOrEqualTo(expect);
    }

    @Test
    void overSEEDSAndSeveralTermsTheDecisionHoldsTheModelsSafety() {
        int[] reached = new int[2];
        int[] byQMin = new int[4];
        int waited = 0;
        for (long seed = 0; seed < 30_000; seed++) {
            Model m = model(seed);
            StreamDecision.Outcome outcome = decide(m);
            if (outcome instanceof StreamDecision.Decided d) {
                holds(m, d, reached);
                m.terms().stream().filter(t -> !t.recorded().isEmpty()).forEach(t -> byQMin[
                        t.recorded().stream().mapToInt(Integer::intValue).min().orElseThrow()]++);
            } else {
                assertThat(m.allAnswered()).as("seed %d: all answered, it decides", seed)
                        .isFalse();
                waited++;
            }
        }
        // ⚠️ NOT VACUOUS (M13.33y review round 1, T2).
        assertThat(reached[0]).as("a void over an exposed entry").isGreaterThan(200);
        assertThat(reached[1]).as("a survivor committed above a hole after quorum loss")
                .isGreaterThan(200);
        assertThat(byQMin[1]).as("q_min 1").isGreaterThan(200);
        assertThat(byQMin[2]).as("q_min 2").isGreaterThan(200);
        assertThat(byQMin[3]).as("q_min 3").isGreaterThan(200);
        assertThat(waited).isGreaterThan(200);
    }
}
