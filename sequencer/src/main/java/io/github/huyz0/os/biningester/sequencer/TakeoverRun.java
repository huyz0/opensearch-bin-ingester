// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

/**
 * The takeover's loop (ADR-0081 §5 steps 4-7; M13.33k), after the term start:
 * what every member of the unclosed terms holds collected, with the
 * successor's own journal, each stream decided once the wait has passed, the
 * decided streams written as ONE recovery entry (invariant c), then the
 * roster's decisions, then the term's own roster re-read -- and only then
 * each stream resumed (invariant d), RELEASE sent and the terms closed.
 *
 * <p>⚠️ THE LEADER ASSIGNS NOTHING UNTIL EVERY MEMBER IS HEARD: a member not
 * yet answered may hold entries of a stream no one else named, so the caller
 * puts its leader {@link FastWriteLeader#takingOver} before it serves, and
 * only {@link Hold#everyMemberHeard} -- after the wait -- lifts that for the
 * streams this takeover does not decide. A stream decided early is resumed
 * at once.
 *
 * <p>⚠️ NO DECISION IS NUMBERED BEFORE THE ROSTER IS RE-READ (M13.33m review
 * round 1, P3): the next COMMIT's {@code assignedAfter} must count the
 * decisions just written, so {@link Steps#rosterWritten} comes before any
 * stream resumes.
 *
 * <p>⚠️ EVERY STREAM OF AN INDEX THAT LOST QUORUM IS DECIDED, entries or
 * none (ADR-0081 §5 step 5; M13.33aa): an exposed entry whose every copy is
 * gone leaves no entry to name its stream, and resumed at {@code c0} its
 * offset would be assigned again -- so the index's streams, from
 * {@link Steps#streamsOf}, are held and decided, each voided to
 * {@code c0 + B} where nothing survives. An index that did not lose quorum
 * names its streams by their entries: one with none has nothing to commit.
 */
public final class TakeoverRun {

    /** The leader's desk, held while a takeover decides: {@link FastWriteLeader}. */
    public interface Hold {
        /** Holds {@code streams} until each is {@link #decided}. */
        void undecided(Collection<RunKey> streams);

        /** Every member answered or is gone: streams named by no one are assigned. */
        void everyMemberHeard();

        /** {@code stream}'s chain entry and decision are durable: resumed at {@code resumeAt}. */
        void decided(RunKey stream, long resumeAt);
    }

    /** The store and frame steps, each its own class's. */
    public interface Steps {
        /** {@link TakeoverCollection#collect}, a bounded number of rounds. */
        TakeoverCollection.Collected collect(List<Roster> stillOpen, long closedThrough)
                throws InterruptedException;

        /** The chain's next offset of {@code stream}: its {@code c0}. */
        long chainNext(RunKey stream);

        /** Every stream of {@code index} this pod knows of: its catalog's partitions. */
        java.util.Set<RunKey> streamsOf(java.util.UUID index);

        /** {@link RecoveryWriter#write}: one chain entry. */
        Recovery recover(List<StreamDecision.Decided> batch) throws IOException;

        /** {@link RosterDecisions#record}. */
        RosterDecisions.Outcome recordDecisions(Map<RunKey, Long> resumeAt) throws IOException;

        /** {@link FastLeaderTerm#rosterWritten}. */
        void rosterWritten() throws IOException;

        /** {@link ReleaseSender#send}. */
        int release(List<Roster> unclosed, Map<RunKey, Long> below);

        /** {@link EmptyTermCloser#close(long, List, Predicate)}, at the successor's term. */
        int close(List<Roster> unclosed, Predicate<Roster> decided) throws IOException;
    }

    /** How a takeover ended. */
    public sealed interface Outcome permits Done, Deposed {
    }

    /** Every stream decided: where each resumed, and how many terms closed. */
    public record Done(Map<RunKey, Long> resumedAt, int closed) implements Outcome {
        public Done {
            resumedAt = Map.copyOf(resumedAt);
        }
    }

    /** A newer term fenced the successor's roster: it decides nothing more. */
    public record Deposed(long newer) implements Outcome {
    }

    private final Steps steps;
    private final Hold hold;
    private final BooleanSupplier mayDecide;
    private final JoinDesk.Sleeper sleeper;
    private final Duration poll;
    private final long bound;
    private final long batchBytes;

    /**
     * @param mayDecide whether the wall clock has passed {@code notBefore} and
     *     the monotonic wait is served ({@link FastTermStart.Started#mayAssign})
     * @param poll how long between asking {@code mayDecide}
     * @param bound {@code B}, the per-stream bound
     */
    public TakeoverRun(Steps steps, Hold hold, BooleanSupplier mayDecide,
            JoinDesk.Sleeper sleeper, Duration poll, long bound) {
        this(steps, hold, mayDecide, sleeper, poll, bound, Long.MAX_VALUE);
    }

    /**
     * @param batchBytes the most record bytes one recovery entry's uploads
     *     carry: a batch beyond it is split by stream (M13.33q; ADR-0082 §5)
     */
    public TakeoverRun(Steps steps, Hold hold, BooleanSupplier mayDecide,
            JoinDesk.Sleeper sleeper, Duration poll, long bound, long batchBytes) {
        if (batchBytes < 1) {
            throw new IllegalArgumentException("a recovery entry carries at least one byte");
        }
        this.batchBytes = batchBytes;
        this.steps = Objects.requireNonNull(steps, "steps");
        this.hold = Objects.requireNonNull(hold, "hold");
        this.mayDecide = Objects.requireNonNull(mayDecide, "mayDecide");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
        this.poll = Objects.requireNonNull(poll, "poll");
        this.bound = bound;
    }

    /**
     * Takes over {@code stillOpen}, newest first, as the term start left them.
     *
     * @param own the successor's own journal: its entries of the unclosed
     *     terms are live too
     * @throws IOException a store step failed: nothing decided after the last
     *     durable decision resumes, and the next takeover decides it afresh
     */
    public Outcome run(List<Roster> stillOpen, long closedThrough,
            List<FastJournalRecord.Entry> own) throws IOException, InterruptedException {
        if (stillOpen.isEmpty()) {
            hold.everyMemberHeard();
            return new Done(Map.of(), 0);
        }
        Set<Long> epochs = new HashSet<>();
        stillOpen.forEach(r -> epochs.add(r.epoch()));
        List<FastJournalRecord.Entry> mine = new ArrayList<>();
        for (FastJournalRecord.Entry e : own) {
            if (epochs.contains(e.epoch()) && e.firstOffset() < HeldStatusAnswers.supersededFrom(
                    e.key(), e.epoch(), e.assignedAfter(), stillOpen)) {
                mine.add(e);
            }
        }
        Map<RunKey, Long> resumed = new TreeMap<>();
        // ⚠️ ANSWERS AND GONE ACCUMULATE ACROSS ROUNDS (its review round 1,
        // P2): a member fenced answers once and holds nothing more of those
        // terms, so a stream it named stays live though it is silent or gone
        // later -- else the stream would never be decided, and its term closed.
        Map<String, List<FastJournalRecord.Entry>> answered = new TreeMap<>();
        Set<String> gone = new HashSet<>();
        for (int round = 0; ; round++) {
            if (round > 0) {
                sleeper.sleepNanos(poll.toNanos());
            }
            TakeoverCollection.Collected c = steps.collect(stillOpen, closedThrough);
            c.byMember().forEach(answered::putIfAbsent);
            gone.addAll(c.gone());
            Set<String> waiting = new HashSet<>(c.waiting());
            waiting.removeAll(answered.keySet());
            waiting.removeAll(gone);
            Map<RunKey, List<FastJournalRecord.Entry>> live = new TreeMap<>();
            answered.values().forEach(held -> add(live, held));
            add(live, mine);
            // ⚠️ M13.33aa: a lost index's every stream, entries or none.
            for (java.util.UUID index : recorded(stillOpen)) {
                if (QuorumLoss.lost(stillOpen, index, gone)) {
                    steps.streamsOf(index).forEach(s -> live.putIfAbsent(s, new ArrayList<>()));
                }
            }
            live.keySet().removeAll(resumed.keySet());
            hold.undecided(live.keySet());
            while (!mayDecide.getAsBoolean()) {
                sleeper.sleepNanos(poll.toNanos());
            }
            boolean heard = waiting.isEmpty();
            if (heard) {
                hold.everyMemberHeard();
            }
            Set<String> unansweredOrGone = new HashSet<>(waiting);
            unansweredOrGone.addAll(gone);
            List<StreamDecision.Decided> batch = new ArrayList<>();
            Map<RunKey, Long> at = new TreeMap<>();
            for (Map.Entry<RunKey, List<FastJournalRecord.Entry>> s : live.entrySet()) {
                RunKey stream = s.getKey();
                boolean lost = QuorumLoss.lost(stillOpen, stream.indexId(), gone);
                StreamDecision.Outcome o = StreamDecision.decide(stream,
                        steps.chainNext(stream), bound, s.getValue(), lost, heard,
                        QuorumLoss.mayDecideEarly(stillOpen, stream.indexId(), unansweredOrGone));
                if (o instanceof StreamDecision.Decided d) {
                    batch.add(d);
                    at.put(stream, d.resumeAt());
                }
            }
            if (!batch.isEmpty()) {
                // ⚠️ A BATCH WRITING NOTHING HAS NO CHAIN ENTRY (its review
                // round 1, P1): every entry below c0, or above a hole at c0,
                // decides to commit and void nothing -- a recovery refuses
                // that, yet the decision still supersedes what it discarded.
                List<StreamDecision.Decided> writes = batch.stream()
                        .filter(d -> !d.commits().isEmpty() || !d.voids().isEmpty()).toList();
                recoverByStream(writes);
                if (steps.recordDecisions(at) instanceof RosterDecisions.Deposed d) {
                    return new Deposed(d.newer());
                }
                steps.rosterWritten();
                at.forEach(hold::decided);
                resumed.putAll(at);
            }
            if (heard) {
                break;
            }
        }
        // ⚠️ NO EMPTY RELEASE (M13.33i review round 1, R1): nothing decided
        // releases nothing.
        if (!resumed.isEmpty()) {
            steps.release(stillOpen, resumed);
        }
        return new Done(resumed, steps.close(stillOpen, r -> true));
    }

    /**
     * {@code writes} as recovery entries of at most {@code batchBytes} each,
     * split between streams, never within one: a stream's commits and voids
     * stay in one entry (invariant c), and a stream over the budget is an
     * entry of its own.
     */
    private void recoverByStream(List<StreamDecision.Decided> writes) throws IOException {
        List<StreamDecision.Decided> entry = new ArrayList<>();
        long bytes = 0;
        for (StreamDecision.Decided d : writes) {
            long size = bytes(d);
            if (!entry.isEmpty() && bytes + size > batchBytes) {
                steps.recover(entry);
                entry = new ArrayList<>();
                bytes = 0;
            }
            entry.add(d);
            bytes += size;
        }
        if (!entry.isEmpty()) {
            steps.recover(entry);
        }
    }

    /** A decision's record bytes: each record's id, payload and 16 bytes of framing. */
    private static long bytes(StreamDecision.Decided d) {
        long bytes = 0;
        for (FastJournalRecord.Entry e : d.commits()) {
            for (io.github.huyz0.os.biningester.format.SegmentRecord r : e.records()) {
                bytes += r.id().getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                        + r.payload().length + 16L;
            }
        }
        return bytes;
    }

    /** Every index a term of {@code stillOpen} recorded a {@code wal_quorum} for. */
    private static Set<java.util.UUID> recorded(List<Roster> stillOpen) {
        Set<java.util.UUID> indices = new HashSet<>();
        for (Roster term : stillOpen) {
            term.termRecord().forEach(r -> indices.addAll(r.walQuorum().keySet()));
        }
        return indices;
    }

    private static void add(Map<RunKey, List<FastJournalRecord.Entry>> live,
            List<FastJournalRecord.Entry> entries) {
        for (FastJournalRecord.Entry e : entries) {
            live.computeIfAbsent(e.key(), k -> new ArrayList<>()).add(e);
        }
    }
}
