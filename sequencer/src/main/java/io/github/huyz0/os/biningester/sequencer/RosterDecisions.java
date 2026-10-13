// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;

/**
 * A takeover's decisions, written to the successor's own roster (ADR-0081 §5
 * step 6; M13.33h): each decided stream's resume offset, so every entry
 * assigned at or above it in an earlier term is superseded on every pod
 * (invariant b).
 *
 * <p>⚠️ THE NEXT NUMBER, NEVER THE LIST's SIZE: a pruned list is shorter than
 * its numbering (§1), and an entry's {@code assignedAfter} names the number
 * the next decision takes.
 *
 * <p>⚠️ A DECISION ALREADY WRITTEN IS NOT WRITTEN TWICE: a write whose
 * response was lost is retried by recording again, and the decisions it
 * landed are found and kept. A roster fenced by a newer term deposes this
 * takeover, which writes nothing.
 */
public final class RosterDecisions {

    /** How a decisions write ended. */
    public sealed interface Outcome permits Recorded, Deposed {
    }

    /** Written; {@code added} are the decisions this call added. */
    public record Recorded(List<Roster.Decision> added) implements Outcome {
        public Recorded {
            added = List.copyOf(added);
        }
    }

    /** A newer term fenced the roster: nothing was written. */
    public record Deposed(long newer) implements Outcome {
    }

    private final BinStore store;
    private final String prefix;
    private final long epoch;

    public RosterDecisions(BinStore store, String prefix, long epoch) {
        this.store = Objects.requireNonNull(store, "store");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        if (epoch < 1) {
            throw new IllegalArgumentException("a term's epoch is at least 1: " + epoch);
        }
        this.epoch = epoch;
    }

    /**
     * Records each stream of {@code resumeAt} at its resume offset.
     *
     * @throws IOException the store failed, the roster is missing, or it kept
     *     changing; the takeover records again
     */
    public Outcome record(Map<RunKey, Long> resumeAt) throws IOException {
        return record(resumeAt, 0);
    }

    /**
     * As {@link #record(Map)}, a decision already written found only among
     * those numbered {@code since} or later (M13.34p): a live leader's switch
     * decides a stream at a resume offset an EARLIER decision may share -- a
     * takeover's, numbered before the entries the switch discards -- and that
     * one supersedes none of them (ADR-0081 §5 invariant b, §8), so it must
     * not stand in for the switch's. {@code since} is the next decision number
     * the caller read when it decided.
     */
    public Outcome record(Map<RunKey, Long> resumeAt, int since) throws IOException {
        Objects.requireNonNull(resumeAt, "resumeAt");
        if (since < 0) {
            throw new IllegalArgumentException("a decision number is never negative: " + since);
        }
        String key = Roster.key(prefix, epoch);
        for (int attempt = 0; attempt < FastTermStart.MAX_ATTEMPTS; attempt++) {
            Optional<ObjectStat> stat = store.stat(key);
            if (stat.isEmpty()) {
                throw new IOException("roster " + epoch + " is missing");
            }
            Roster r;
            try (InputStream in = store.get(key)) {
                r = Roster.decode(in.readAllBytes());
            }
            if (r.fencedBy() > epoch) {
                return new Deposed(r.fencedBy());
            }
            List<Roster.Decision> decisions = new ArrayList<>(r.decisions());
            int next = decisions.isEmpty() ? 0 : decisions.get(decisions.size() - 1).seq() + 1;
            List<Roster.Decision> added = new ArrayList<>();
            for (Map.Entry<RunKey, Long> e : new TreeMap<>(resumeAt).entrySet()) {
                boolean written = r.decisions().stream().anyMatch(d -> d.seq() >= since
                        && d.stream().equals(e.getKey()) && d.resumeAt() == e.getValue());
                if (!written) {
                    added.add(new Roster.Decision(next++, e.getKey(), e.getValue()));
                }
            }
            if (added.isEmpty()) {
                return new Recorded(added);
            }
            decisions.addAll(added);
            Roster decided = new Roster(r.epoch(), r.predecessor(), r.leader(), r.members(),
                    r.termRecord(), decisions, r.notBefore(), r.fencedBy(), r.closed());
            if (store.putIfMatch(key, Body.ofBytes(decided.encode()), stat.get().version())
                    .isPresent()) {
                return new Recorded(added);
            }
        }
        throw new IOException("roster " + epoch + " kept changing while decisions were written");
    }
}
