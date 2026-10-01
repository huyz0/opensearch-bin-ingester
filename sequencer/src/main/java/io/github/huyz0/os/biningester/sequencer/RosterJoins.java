// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.binstore.Version;
import io.github.huyz0.os.biningester.format.Roster;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The leader's JOINs (ADR-0081 §1; M13.26d): every pending join appended to
 * this term's roster in ONE write, at most one write per
 * {@code min_upload_interval}, and each join answered only once that write is
 * durable -- so the incarnations that can hold a term's copies are exactly
 * its roster's members.
 *
 * <p>⚠️ ONE WRITE PER BATCH, never per join: a roster write per pod per term,
 * coalesced, scales with pods and terms (non-negotiable 6). A join by an
 * incarnation already rostered is answered at once, writing nothing.
 *
 * <p>⚠️ A DEPARTED INCARNATION IS NOT RE-ROSTERED: it uploaded and left
 * (§9), and a later copy on it would be counted toward no quorum, so its join
 * is refused. ⚠️ A ROSTER FENCED BY A NEWER TERM DEPOSES THE LEADER, which
 * writes nothing more.
 *
 * <p>Call only once this term's {@code LATEST} write has landed
 * ({@link FastTermStart}): an orphan roster's leader accepts no JOIN.
 */
public final class RosterJoins {

    private final BinStore store;
    private final String prefix;
    private final long epoch;
    private final MonotonicClock mono;
    private final long minIntervalNanos;
    private final Map<String, Roster.Incarnation> pending = new LinkedHashMap<>();
    private boolean wrote;
    private long lastWriteNanos;

    public RosterJoins(BinStore store, String prefix, long epoch, MonotonicClock mono,
            Duration minUploadInterval) {
        this.store = Objects.requireNonNull(store, "store");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        this.mono = Objects.requireNonNull(mono, "mono");
        if (epoch < 1) {
            throw new IllegalArgumentException("a term's epoch is at least 1: " + epoch);
        }
        this.epoch = epoch;
        this.minIntervalNanos = minUploadInterval.toNanos();
    }

    /** A flush's outcome. */
    public sealed interface Flush permits Idle, NotDue, Answered, Deposed {
    }

    /** No join is pending. */
    public record Idle() implements Flush {
    }

    /** Joins are pending, and the last roster write was under one interval ago. */
    public record NotDue(long dueInNanos) implements Flush {
    }

    /**
     * Each pending join answered: {@code joined} are rostered, durably, and
     * {@code refused} departed earlier.
     */
    public record Answered(Roster roster, List<Roster.Incarnation> joined,
            List<Roster.Incarnation> refused) implements Flush {
        public Answered {
            joined = List.copyOf(joined);
            refused = List.copyOf(refused);
        }
    }

    /** A newer term fenced this one: nothing more is written. */
    public record Deposed(long newer) implements Flush {
    }

    /** A JOIN arrived from {@code pod}; it is answered by a later {@link #flush}. */
    public synchronized void request(Roster.Incarnation pod) {
        pending.put(Objects.requireNonNull(pod, "pod").podUid(), pod);
    }

    /**
     * Appends every pending join in one roster write, when one is due.
     *
     * @throws IOException the store failed, or the roster kept changing; the
     *     joins stay pending and are answered by a later flush
     */
    public synchronized Flush flush() throws IOException {
        if (pending.isEmpty()) {
            return new Idle();
        }
        // ⚠️ BEFORE ANY READ: a flush not yet due costs no request, so the
        // leader may call this as often as it likes.
        long now = mono.nanos();
        if (wrote && now - lastWriteNanos < minIntervalNanos) {
            return new NotDue(minIntervalNanos - (now - lastWriteNanos));
        }
        String key = Roster.key(prefix, epoch);
        for (int attempt = 0; attempt < FastTermStart.MAX_ATTEMPTS; attempt++) {
            Optional<ObjectStat> stat = store.stat(key);
            if (stat.isEmpty()) {
                throw new IOException("roster " + epoch + " is missing under its own leader");
            }
            Roster roster;
            try (InputStream in = store.get(key)) {
                roster = Roster.decode(in.readAllBytes());
            }
            if (roster.fencedBy() > epoch) {
                return new Deposed(roster.fencedBy());
            }
            List<Roster.Member> members = new ArrayList<>(roster.members());
            List<Roster.Incarnation> joined = new ArrayList<>();
            List<Roster.Incarnation> refused = new ArrayList<>();
            boolean appended = false;
            for (Roster.Incarnation pod : pending.values()) {
                Optional<Roster.Member> listed = roster.member(pod.podUid());
                if (listed.isEmpty()) {
                    members.add(new Roster.Member(pod, Roster.State.ROSTERED));
                    joined.add(pod);
                    appended = true;
                } else if (listed.get().state() == Roster.State.DEPARTED) {
                    refused.add(pod);
                } else {
                    joined.add(pod);
                }
            }
            if (!appended) {
                pending.clear();
                return new Answered(roster, joined, refused);
            }
            Roster grown = new Roster(roster.epoch(), roster.predecessor(), roster.leader(),
                    members, roster.termRecord(), roster.decisions(), roster.notBefore(),
                    roster.fencedBy(), roster.closed());
            Version version = stat.get().version();
            if (store.putIfMatch(key, Body.ofBytes(grown.encode()), version).isPresent()) {
                wrote = true;
                lastWriteNanos = now;
                pending.clear();
                return new Answered(grown, joined, refused);
            }
            // ⚠️ REFUSED: a successor's fence, or this leader's own other
            // write, landed first. Re-read: the fence deposes, anything else
            // is kept and the joins appended to it.
        }
        throw new IOException("roster " + epoch + " kept changing while joins were appended");
    }
}
