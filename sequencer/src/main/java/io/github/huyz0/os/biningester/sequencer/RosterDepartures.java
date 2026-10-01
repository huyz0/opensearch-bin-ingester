// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.format.Roster;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The leader's {@code DEPARTED} writes (ADR-0081 §9; M13.26h): a departing
 * pod, once nothing it holds is pending, is marked departed in the current
 * roster and in every unclosed roster listing it -- one write per such term,
 * scaling with nodes -- so its later loss is never counted as quorum loss.
 *
 * <p>⚠️ A ROSTER FENCED BY A NEWER TERM DEPOSES this leader, which writes
 * nothing more; earlier unclosed rosters are fenced by this term, which is
 * the leader's own fence and does not stop it. A roster not listing the pod,
 * or listing it departed already, is not written.
 */
public final class RosterDepartures {

    private final BinStore store;
    private final String prefix;
    private final long epoch;

    public RosterDepartures(BinStore store, String prefix, long epoch) {
        this.store = Objects.requireNonNull(store, "store");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        if (epoch < 1) {
            throw new IllegalArgumentException("a term's epoch is at least 1: " + epoch);
        }
        this.epoch = epoch;
    }

    /** How a departure's writes ended. */
    public sealed interface Outcome permits Departed, Deposed {
    }

    /** Marked departed; {@code written} are the terms whose roster changed. */
    public record Departed(List<Long> written) implements Outcome {
        public Departed {
            written = List.copyOf(written);
        }
    }

    /** A newer term fenced a roster: nothing more is written. */
    public record Deposed(long newer) implements Outcome {
    }

    /**
     * Marks {@code podUid} departed in this term's roster and in each of
     * {@code unclosedEarlier}'s.
     *
     * @throws IOException the store failed, a roster is missing, or one kept
     *     changing; the departure is retried, and the pod waits
     */
    public Outcome depart(String podUid, List<Long> unclosedEarlier) throws IOException {
        Objects.requireNonNull(podUid, "podUid");
        List<Long> terms = new ArrayList<>();
        terms.add(epoch);
        for (long e : unclosedEarlier) {
            if (e >= epoch) {
                throw new IllegalArgumentException("an earlier term is below " + epoch + ": " + e);
            }
            terms.add(e);
        }
        List<Long> written = new ArrayList<>();
        for (long term : terms) {
            Optional<Long> newer = mark(term, podUid, written);
            if (newer.isPresent()) {
                return new Deposed(newer.get());
            }
        }
        return new Departed(written);
    }

    private Optional<Long> mark(long term, String podUid, List<Long> written)
            throws IOException {
        String key = Roster.key(prefix, term);
        for (int attempt = 0; attempt < FastTermStart.MAX_ATTEMPTS; attempt++) {
            Optional<ObjectStat> stat = store.stat(key);
            if (stat.isEmpty()) {
                throw new IOException("roster " + term + " is missing");
            }
            Roster r;
            try (InputStream in = store.get(key)) {
                r = Roster.decode(in.readAllBytes());
            }
            if (r.fencedBy() > epoch) {
                return Optional.of(r.fencedBy());
            }
            Optional<Roster.Member> member = r.member(podUid);
            if (member.isEmpty() || member.get().state() == Roster.State.DEPARTED) {
                return Optional.empty();
            }
            List<Roster.Member> members = new ArrayList<>();
            for (Roster.Member m : r.members()) {
                members.add(m.incarnation().podUid().equals(podUid)
                        ? new Roster.Member(m.incarnation(), Roster.State.DEPARTED) : m);
            }
            Roster departed = new Roster(r.epoch(), r.predecessor(), r.leader(), members,
                    r.termRecord(), r.decisions(), r.notBefore(), r.fencedBy(), r.closed());
            if (store.putIfMatch(key, Body.ofBytes(departed.encode()), stat.get().version())
                    .isPresent()) {
                written.add(term);
                return Optional.empty();
            }
        }
        throw new IOException("roster " + term + " kept changing while a departure was marked");
    }
}
