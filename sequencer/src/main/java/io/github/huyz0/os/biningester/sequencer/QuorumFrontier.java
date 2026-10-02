// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.RunKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * The leader's replica set and quorum frontier (ADR-0081 §2.3, §2.5;
 * M13.27b): which holders each uncommitted fast entry has, which it still
 * needs, and how far each stream may be exposed.
 *
 * <p>⚠️ A COPY COUNTS ONLY ONCE JOURNALED WITH ITS OFFSETS, from the
 * incarnation that was addressed (the caller checks the frame's addressee):
 * an offsetless copy cannot be recovered at its offset. The leader holds every
 * entry of its term, so its own AZ counts from {@link #assigned} -- called
 * once the leader's journal group holding the entry is fsynced.
 *
 * <p>⚠️ EXPOSURE IS IN OFFSET ORDER (obligations 9, 10): each stream's
 * frontier is the end of the longest prefix of uncommitted entries each of
 * whose quorum is complete, and an entry is exposed -- acked, published,
 * served -- only once the frontier passes it, whatever order holders answer
 * in.
 *
 * <p>⚠️ A LOST HOLDER STOPS COUNTING for every entry not yet exposed: an entry
 * complete but unexposed at the loss would otherwise be exposed on one live
 * copy. Its streams are reported, and the caller discards their entries at or
 * above the frontier by a decision -- the only discard (§2.3): a slow or full
 * holder that stays ready causes waiting, never a discard.
 */
public final class QuorumFrontier {

    /**
     * An entry's identity (ADR-0081 §5 invariant (b)): after a discard one
     * term may hold two entries of a stream at one offset, told apart only by
     * {@code assignedAfter}.
     */
    public record EntryId(long epoch, long assignedAfter, RunKey stream, long firstOffset) {
        public EntryId {
            Objects.requireNonNull(stream, "stream");
            if (epoch < 1 || assignedAfter < 0 || firstOffset < 0) {
                throw new IllegalArgumentException("an entry is of a term, after a "
                        + "non-negative number of decisions, at a non-negative offset");
            }
        }
    }

    /** A rostered incarnation that can hold copies. */
    public record Holder(String podUid, String az) {
        public Holder {
            Objects.requireNonNull(podUid, "podUid");
            Objects.requireNonNull(az, "az");
        }
    }

    private static final class Entry {
        final EntryId id;
        final long end;
        final int q;
        final Map<String, String> copies = new LinkedHashMap<>();
        final Map<String, String> asked = new LinkedHashMap<>();
        /** Pods that answered they did not journal it: not asked for it again until returned. */
        final Set<String> declined = new HashSet<>();
        boolean exposed;

        Entry(EntryId id, int count, int q) {
            this.id = id;
            this.end = id.firstOffset() + count;
            this.q = q;
        }

        Set<String> zones() {
            return new HashSet<>(copies.values());
        }

        boolean complete() {
            return zones().size() >= q;
        }
    }

    private static final class Stream {
        long frontier;
        /** A loss left an entry short: no exposure until its decision's discard. */
        boolean held;
        final NavigableMap<Long, Entry> entries = new TreeMap<>();
    }

    private final String leaderUid;
    private final String leaderAz;
    private final Map<RunKey, Stream> streams = new HashMap<>();
    /** Pods reported lost: no copy or ask of theirs counts until {@link #returned}. */
    private final Set<String> lostPods = new HashSet<>();

    public QuorumFrontier(String leaderUid, String leaderAz) {
        this.leaderUid = Objects.requireNonNull(leaderUid, "leaderUid");
        this.leaderAz = Objects.requireNonNull(leaderAz, "leaderAz");
    }

    /** Tracks {@code stream} from its committed next offset, where its frontier starts. */
    public synchronized void open(RunKey stream, long committedNext) {
        Stream s = streams.computeIfAbsent(Objects.requireNonNull(stream, "stream"),
                k -> new Stream());
        s.frontier = Math.max(s.frontier, committedNext);
    }

    /**
     * The leader journaled and fsynced an entry of {@code count} records
     * assigned under {@code q}: its own copy counts.
     */
    public synchronized void assigned(EntryId id, int count, int q) {
        Stream s = stream(id.stream());
        if (count < 1 || q < 1 || q > 3) {
            throw new IllegalArgumentException("an entry holds records under a q of 1 to 3");
        }
        if (id.firstOffset() < s.frontier) {
            throw new IllegalArgumentException("an entry below the frontier " + s.frontier
                    + " is assigned twice: " + id);
        }
        Entry e = new Entry(id, count, q);
        e.copies.put(leaderUid, leaderAz);
        s.entries.put(id.firstOffset(), e);
    }

    /** A copy was asked of {@code holder}; it counts once {@link #copied}. */
    public synchronized void asked(EntryId id, Holder holder) {
        Entry e = entry(id);
        if (e != null && !lostPods.contains(holder.podUid())
                && !e.copies.containsKey(holder.podUid())) {
            e.asked.put(holder.podUid(), holder.az());
        }
    }

    /**
     * {@code podUid} answered that it did NOT journal the entry: its ask stops
     * covering its zone, so the copy is replaced (ADR-0081 section 2.3).
     */
    public synchronized void withdraw(EntryId id, String podUid) {
        Entry e = entry(id);
        if (e != null) {
            e.asked.remove(podUid);
            // AND NOT CHOSEN AGAIN FOR IT UNTIL IT RETURNS (M13.27c review
            // round 3, P8; M13.27e round 2, P12): the copy goes to ANOTHER
            // available rostered pod, the decliner itself once returned
            // (section 2.3).
            e.declined.add(podUid);
        }
    }

    /**
     * {@code holder} journaled the entry with its offsets and said so -- a
     * REPLICA_ACK, or the writer's CONFIRM -- after its group's fsync.
     */
    public synchronized void copied(EntryId id, Holder holder) {
        Entry e = entry(id);
        // ⚠️ NEVER FROM A POD REPORTED LOST (M13.27b review round 2, P4): its
        // ack in flight at the loss would complete an entry the loss left
        // short, exposing it on one live copy.
        if (e == null || lostPods.contains(holder.podUid())) {
            return;
        }
        e.asked.remove(holder.podUid());
        e.copies.put(holder.podUid(), holder.az());
    }

    /**
     * The holders to send {@code id} to now: one available pod in each AZ
     * the entry still lacks, never one already holding or asked, nor an AZ a
     * pending ask to an AVAILABLE pod already covers.
     *
     * <p>⚠️ AN ASK COVERS ITS ZONE ONLY WHILE ITS HOLDER IS AVAILABLE
     * (M13.27b review round 1, P1): a holder that refused with backpressure,
     * or passed the replica timeout, is not available (§2.3), and its copy
     * is replaced in another pod -- never awaited for ever.
     */
    public synchronized List<Holder> holdersFor(EntryId id, List<Holder> available) {
        Entry e = entry(id);
        List<Holder> chosen = new ArrayList<>();
        if (e == null) {
            return chosen;
        }
        Set<String> availableUids = new HashSet<>();
        available.forEach(h -> availableUids.add(h.podUid()));
        Set<String> covered = new TreeSet<>(e.copies.values());
        e.asked.forEach((uid, az) -> {
            if (availableUids.contains(uid)) {
                covered.add(az);
            }
        });
        int missing = e.q - covered.size();
        for (Holder h : available) {
            if (missing <= 0) {
                break;
            }
            if (lostPods.contains(h.podUid()) || e.declined.contains(h.podUid())) {
                continue;
            }
            if (covered.contains(h.az()) || e.copies.containsKey(h.podUid())
                    || e.asked.containsKey(h.podUid())) {
                continue;
            }
            chosen.add(h);
            covered.add(h.az());
            missing--;
        }
        return chosen;
    }

    /**
     * Advances each stream's frontier over its longest prefix of complete
     * entries.
     *
     * @return the entries newly exposed, in offset order per stream
     */
    public synchronized List<EntryId> expose() {
        List<EntryId> exposed = new ArrayList<>();
        for (Stream s : streams.values()) {
            if (s.held) {
                continue;
            }
            for (Entry e : s.entries.tailMap(s.frontier, true).values()) {
                if (e.id.firstOffset() != s.frontier || !e.complete()) {
                    break;
                }
                s.frontier = e.end;
                if (!e.exposed) {
                    e.exposed = true;
                    exposed.add(e.id);
                }
            }
        }
        return exposed;
    }

    /** {@code stream}'s quorum frontier: every offset below it is exposable. */
    public synchronized long frontier(RunKey stream) {
        return stream(stream).frontier;
    }

    /**
     * {@code podUid} is gone or out of the EndpointSlice: its copies and asks
     * stop counting for every unexposed entry.
     *
     * @return the streams with an unexposed entry it held or was asked for
     *     and that is now short of its quorum -- each to be discarded at or
     *     above its frontier by a decision; an entry still holding {@code q}
     *     zones without it needs none (M13.27b review round 1, P3)
     */
    public synchronized Set<RunKey> lost(String podUid) {
        lostPods.add(Objects.requireNonNull(podUid, "podUid"));
        Set<RunKey> affected = new TreeSet<>();
        for (Map.Entry<RunKey, Stream> s : streams.entrySet()) {
            for (Entry e : s.getValue().entries.values()) {
                if (e.exposed) {
                    continue;
                }
                boolean relied = e.copies.remove(podUid) != null;
                relied |= e.asked.remove(podUid) != null;
                if (relied && !e.complete()) {
                    affected.add(s.getKey());
                    // ⚠️ HELD UNTIL ITS DECISION (M13.27b review round 2, P6;
                    // ADR-0081 §2.3): the frontier must not pass the resume
                    // offset the discard is about to take.
                    s.getValue().held = true;
                }
            }
        }
        return affected;
    }

    /**
     * A decision superseded {@code stream}'s entries at or above {@code from};
     * a stream held by a loss is exposed again.
     */
    public synchronized void discard(RunKey stream, long from) {
        Stream s = stream(stream);
        if (from < s.frontier) {
            throw new IllegalArgumentException("an exposed entry below the frontier "
                    + s.frontier + " is never discarded: " + from);
        }
        s.entries.tailMap(from, true).clear();
        s.held = false;
    }

    /** {@code podUid}, reported lost, is ready again: its later copies count. */
    public synchronized void returned(String podUid) {
        lostPods.remove(podUid);
        // AND ITS DECLINES FORGOTTEN (M13.27e review round 1, P9): a copy is
        // replaced in another pod, "W itself, once it returns, included"
        // (ADR-0081 section 2.3) -- kept for ever, a zone with one pod could
        // never complete.
        for (Stream s : streams.values()) {
            for (Entry e : s.entries.values()) {
                e.declined.remove(podUid);
            }
        }
    }

    /** The chain committed {@code stream} up to {@code committedNext}: its entries below go. */
    public synchronized void committed(RunKey stream, long committedNext) {
        Stream s = stream(stream);
        s.entries.headMap(committedNext, false).values()
                .removeIf(e -> e.end <= committedNext);
        s.frontier = Math.max(s.frontier, committedNext);
    }

    private Stream stream(RunKey stream) {
        Stream s = streams.get(Objects.requireNonNull(stream, "stream"));
        if (s == null) {
            throw new IllegalStateException("stream " + stream + " is not open");
        }
        return s;
    }

    private Entry entry(EntryId id) {
        Entry e = stream(id.stream()).entries.get(id.firstOffset());
        return e != null && e.id.equals(id) ? e : null;
    }
}
