// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.format.Roster;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.TreeMap;
import java.util.UUID;

/**
 * The term record (ADR-0081 §4; M13.26e): the leader assigns for a fast index
 * only under a {@code wal_quorum} its term's roster already records -- a new
 * fast index's first value and every change, raise or lower, written BEFORE
 * any assignment under it -- so after quorum loss a successor judges the
 * term by the values recorded, never by the catalog's current one or by the
 * entries that would have said so, which may be the lost ones.
 *
 * <p>⚠️ COALESCED: every change since the last write goes in ONE roster write,
 * at most one per {@code min_upload_interval}, however many indices changed
 * (cost.md rule 6) -- one element of the term record per write. Until its
 * value is recorded an index's batches WAIT ({@link #assignable} is empty),
 * backpressure rather than a refusal. A change undone before its write is
 * dropped, never written.
 *
 * <p>⚠️ So every entry of term {@code T} was assigned under some
 * {@code q >= q_min(T, I)} ({@link #qMin}), the smallest value recorded for
 * its index in {@code T} -- the value the quorum-loss predicate reads.
 */
public final class TermRecordWriter {

    private final BinStore store;
    private final String prefix;
    private final long epoch;
    private final MonotonicClock mono;
    private final long minIntervalNanos;
    private final Map<UUID, Integer> recorded = new HashMap<>();
    private final Map<UUID, Integer> pending = new TreeMap<>();
    /** The batch a flush is writing, empty between flushes. */
    private Map<UUID, Integer> inFlight = Map.of();
    private final java.util.concurrent.locks.ReentrantLock flushing =
            new java.util.concurrent.locks.ReentrantLock();
    private boolean attempted;
    private long lastAttemptNanos;

    /**
     * @param started this term's roster as {@link FastTermStart} wrote it,
     *     whose term record's values are the ones assignable at once
     */
    public TermRecordWriter(BinStore store, String prefix, Roster started, MonotonicClock mono,
            Duration minUploadInterval) {
        this.store = Objects.requireNonNull(store, "store");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        this.mono = Objects.requireNonNull(mono, "mono");
        this.epoch = started.epoch();
        this.minIntervalNanos = minUploadInterval.toNanos();
        for (Roster.TermRecord element : started.termRecord()) {
            recorded.putAll(element.walQuorum());
        }
    }

    /** The smallest {@code wal_quorum} roster {@code r} records for {@code index}, if any. */
    public static OptionalInt qMin(Roster r, UUID index) {
        return r.termRecord().stream()
                .map(Roster.TermRecord::walQuorum)
                .filter(q -> q.containsKey(index))
                .mapToInt(q -> q.get(index))
                .min();
    }

    /** A flush's outcome. */
    public sealed interface Flush permits Idle, NotDue, Written, Deposed {
    }

    /** No change is pending. */
    public record Idle() implements Flush {
    }

    /** Changes are pending, and the last write was under one interval ago. */
    public record NotDue(long dueInNanos) implements Flush {
    }

    /** The pending changes are recorded, as one element of the term record. */
    public record Written(Roster roster) implements Flush {
    }

    /** A newer term fenced this one: nothing more is written. */
    public record Deposed(long newer) implements Flush {
    }

    /**
     * The catalog says {@code index} is fast at {@code walQuorum}: pending
     * until recorded, unless it is the value already recorded.
     */
    public synchronized void want(UUID index, int walQuorum) {
        Objects.requireNonNull(index, "index");
        if (walQuorum < 1 || walQuorum > 3) {
            throw new IllegalArgumentException("wal_quorum is 1 to 3: " + walQuorum);
        }
        // ⚠️ NOT DROPPED WHILE ITS INDEX IS IN FLIGHT (M13.26e review round 2,
        // P1): `recorded` does not yet hold the value being written, so an undo
        // compared with it would be lost and the in-flight value kept. It stays
        // pending; the commit clears only the value it wrote.
        if (!inFlight.containsKey(index)
                && Integer.valueOf(walQuorum).equals(recorded.get(index))) {
            pending.remove(index);
        } else {
            pending.put(index, walQuorum);
        }
    }

    /**
     * The {@code wal_quorum} a batch of {@code index} may be assigned under
     * now, or empty while its value is not recorded -- the batch waits.
     */
    public synchronized OptionalInt assignable(UUID index) {
        Integer q = recorded.get(index);
        if (q == null || pending.containsKey(index)) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(q);
    }

    /**
     * Records every pending change in one roster write, when one is due.
     *
     * <p>⚠️ THE STORE IS NEVER CALLED UNDER THE LOCK {@link #assignable} TAKES
     * (M13.26e review round 1, P1): the pending set is copied under it, written
     * outside it, and committed under it again, so an index with nothing
     * pending is assignable -- and its append acked -- while another index's
     * change is being written (SPEC criterion 8). A change made meanwhile stays
     * pending. Flushes are serialized by their own lock (java-style.md: a
     * {@code ReentrantLock} across I/O).
     *
     * <p>⚠️ EVERY ATTEMPT STARTS THE INTERVAL, not only a successful write
     * (round 1, P2): after a failure or a deposition the next flush still
     * waits, so the request rate follows the interval, never the caller.
     *
     * @throws IOException the store failed, or the roster kept changing; the
     *     changes stay pending and their batches keep waiting
     */
    public Flush flush() throws IOException {
        flushing.lock();
        try {
            Map<UUID, Integer> batch;
            synchronized (this) {
                if (pending.isEmpty()) {
                    return new Idle();
                }
                long now = mono.nanos();
                if (attempted && now - lastAttemptNanos < minIntervalNanos) {
                    return new NotDue(minIntervalNanos - (now - lastAttemptNanos));
                }
                attempted = true;
                lastAttemptNanos = now;
                batch = new TreeMap<>(pending);
                inFlight = batch;
            }
            try {
                return write(batch);
            } finally {
                synchronized (this) {
                    inFlight = Map.of();
                    // A change undone during a write that did not land is a
                    // change to nothing: dropped, never written.
                    pending.entrySet().removeIf(
                            e -> e.getValue().equals(recorded.get(e.getKey())));
                }
            }
        } finally {
            flushing.unlock();
        }
    }

    private Flush write(Map<UUID, Integer> batch) throws IOException {
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
            List<Roster.TermRecord> record = new ArrayList<>(roster.termRecord());
            record.add(new Roster.TermRecord(record.size(), new TreeMap<>(batch)));
            Roster grown = new Roster(roster.epoch(), roster.predecessor(), roster.leader(),
                    roster.members(), record, roster.decisions(), roster.notBefore(),
                    roster.fencedBy(), roster.closed());
            if (store.putIfMatch(key, Body.ofBytes(grown.encode()), stat.get().version())
                    .isPresent()) {
                synchronized (this) {
                    recorded.putAll(batch);
                    batch.forEach(pending::remove);
                }
                return new Written(grown);
            }
            // ⚠️ REFUSED: a join or a successor's fence landed first.
            // Re-read: the fence deposes, anything else is kept and the
            // change appended.
        }
        throw new IOException("roster " + epoch + " kept changing while its term record grew");
    }
}
