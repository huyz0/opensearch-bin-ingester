// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastTakeoverFrame;
import io.github.huyz0.os.biningester.format.Roster;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The successor's collection (ADR-0081 §5 step 4; M13.33d): a FENCE to every
 * ROSTERED member of every unclosed term, read after the fence writes, and
 * what each holds of those terms collected page by page.
 *
 * <p>⚠️ A MEMBER IS ANSWERED ONLY ON ITS LAST PAGE, with every earlier page
 * received (M13.22e review round 3, P3): its pages are asked in order, and
 * an answer addressed elsewhere, from another pod, of another term or of
 * another page is no answer.
 *
 * <p>⚠️ RETRIED UNTIL ANSWERED OR GONE: a member that does not answer is
 * asked again after {@code retry}; only {@link PodGone} saying its pod is
 * deleted ends the wait -- not ready, out of the EndpointSlice, partitioned,
 * or a lookup that could not answer is waited for (obligation 3). The
 * rounds are bounded here and the members still waiting returned, so the
 * caller decides what a long wait means.
 *
 * <p>⚠️ DEPARTED MEMBERS ARE NOT ASKED (§9), nor the successor itself:
 * the decision reads its own journal.
 *
 * <p>Collected entries a walked roster marks superseded (invariant b) are
 * dropped: a decision recorded after an entry was assigned freed its offset.
 */
public final class TakeoverCollection {

    /** What was collected, by member UID; who is gone; who is still waited for. */
    public record Collected(Map<String, List<FastJournalRecord.Entry>> byMember,
            Set<String> gone, Set<String> waiting) {
        public Collected {
            byMember = Map.copyOf(byMember);
            gone = Set.copyOf(gone);
            waiting = Set.copyOf(waiting);
        }
    }

    private final String selfUid;
    private final long epoch;
    private final TermJoiner.Transport transport;
    private final PodGone podGone;
    private final JoinDesk.Sleeper sleeper;
    private final Duration retry;

    /**
     * @param epoch the successor's term, E'
     * @param retry how long between rounds of asking the members still waited for
     */
    public TakeoverCollection(String selfUid, long epoch, TermJoiner.Transport transport,
            PodGone podGone, JoinDesk.Sleeper sleeper, Duration retry) {
        this.selfUid = Objects.requireNonNull(selfUid, "selfUid");
        this.epoch = epoch;
        this.transport = Objects.requireNonNull(transport, "transport");
        this.podGone = Objects.requireNonNull(podGone, "podGone");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
        this.retry = Objects.requireNonNull(retry, "retry");
    }

    /**
     * Collects from the members of {@code stillOpen}, the unclosed terms, in
     * at most {@code rounds} rounds.
     *
     * @param closedThrough the highest closed epoch, every entry at or below
     *     which a member drops
     */
    public Collected collect(List<Roster> stillOpen, long closedThrough, int rounds)
            throws InterruptedException {
        Objects.requireNonNull(stillOpen, "stillOpen");
        if (rounds < 1) {
            throw new IllegalArgumentException("at least one round");
        }
        Map<String, Roster.Incarnation> members = new LinkedHashMap<>();
        Set<String> departed = new LinkedHashSet<>();
        List<Long> epochs = new ArrayList<>();
        for (Roster r : stillOpen) {
            epochs.add(r.epoch());
            for (Roster.Member m : r.members()) {
                if (m.state() == Roster.State.DEPARTED) {
                    departed.add(m.incarnation().podUid());
                } else if (!m.incarnation().podUid().equals(selfUid)) {
                    members.putIfAbsent(m.incarnation().podUid(), m.incarnation());
                }
            }
        }
        // ⚠️ DEPARTED IN ANY OPEN ROSTER IS DEPARTED (its review round 1, P1):
        // a departure marks every open roster listing the pod, and one it
        // left ROSTERED would make the successor wait on a pod holding nothing.
        members.keySet().removeAll(departed);
        Map<String, List<FastJournalRecord.Entry>> byMember = new LinkedHashMap<>();
        Set<String> gone = new LinkedHashSet<>();
        Set<String> waiting = new LinkedHashSet<>(members.keySet());
        epochs.sort(Long::compare);
        for (int round = 0; round < rounds && !waiting.isEmpty(); round++) {
            if (round > 0) {
                sleeper.sleepNanos(retry.toNanos());
            }
            for (String uid : List.copyOf(waiting)) {
                Roster.Incarnation member = members.get(uid);
                Optional<List<FastJournalRecord.Entry>> held =
                        pages(member, epochs, closedThrough);
                if (held.isPresent()) {
                    byMember.put(uid, unsuperseded(held.get(), stillOpen));
                    waiting.remove(uid);
                } else if (isGone(member)) {
                    gone.add(uid);
                    waiting.remove(uid);
                }
            }
        }
        return new Collected(byMember, gone, waiting);
    }

    /** Every page of {@code member}'s answer, or empty where it gave none whole. */
    private Optional<List<FastJournalRecord.Entry>> pages(Roster.Incarnation member,
            List<Long> epochs, long closedThrough) {
        List<FastJournalRecord.Entry> held = new ArrayList<>();
        for (long page = 0; page <= FastFrame.MAX_U32; page++) {
            FastFrame.Frame answer;
            try {
                answer = FastFrame.decode(transport.exchange(member.endpoint(),
                        FastFrame.encode(epoch, selfUid, member.podUid(),
                                new FastTakeoverFrame.Fence(epochs, closedThrough, page))));
            } catch (IOException unanswered) {
                return Optional.empty();
            }
            if (answer.header().epoch() != epoch
                    || !answer.header().senderUid().equals(member.podUid())
                    || !answer.header().targetUid().equals(selfUid)
                    || !(answer.body() instanceof FastTakeoverFrame.Collected collected)
                    || collected.page() != page) {
                return Optional.empty();
            }
            held.addAll(collected.entries());
            if (collected.last()) {
                return Optional.of(held);
            }
        }
        return Optional.empty();
    }

    private boolean isGone(Roster.Incarnation member) {
        try {
            return podGone.gone(member);
        } catch (IOException noAnswer) {
            return false;
        }
    }

    private static List<FastJournalRecord.Entry> unsuperseded(
            List<FastJournalRecord.Entry> entries, List<Roster> rosters) {
        List<FastJournalRecord.Entry> kept = new ArrayList<>();
        for (FastJournalRecord.Entry e : entries) {
            long from = HeldStatusAnswers.supersededFrom(e.key(), e.epoch(), e.assignedAfter(),
                    rosters);
            if (e.firstOffset() < from) {
                kept.add(e);
            }
        }
        return kept;
    }
}
