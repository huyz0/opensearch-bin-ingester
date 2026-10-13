// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

/**
 * A fast term's streams switching to the default path (ADR-0081 §8; M13.34r):
 * each paused by a default-path run while it held a fast tail, then -- one
 * drain tick at a time -- its unexposed tail discarded by a decision, its
 * exposed tail uploaded, and the run admitted once the chain reaches the
 * frontier; then the stream unpaused.
 *
 * <p>⚠️ EVERY SWITCHED STREAM's DECISION IN ONE ROSTER WRITE, AT MOST ONCE PER
 * {@code min_upload_interval} (round 3, R3-6): the request rate scales with
 * time and leaders, never with streams (non-negotiable 6).
 */
final class SwitchPause {

    private enum State { PAUSED, DECIDED, READY }

    /** A paused stream: its frontier when paused, the decision number then, and its state. */
    private record Paused(long frontier, long since, State state) {
        Paused to(State next) {
            return new Paused(frontier, since, next);
        }
    }

    /** What a tick writes and drops through. */
    interface Leader {
        java.util.Set<io.github.huyz0.os.biningester.format.FastJournalRecord.IdempotencyKey>
                discardFrom(RunKey stream, long from) throws IOException;

        void unpause(RunKey stream, long chainNext);
    }

    /** A roster write the term re-reads after (FastLeaderTerm.rosterWritten). */
    @FunctionalInterface
    interface RosterWritten {
        void run() throws IOException;
    }

    private static final System.Logger LOG = System.getLogger(SwitchPause.class.getName());

    private final MonotonicClock mono;
    private final long intervalNanos;
    private final Map<RunKey, Paused> streams = new HashMap<>();
    private boolean written;
    private long lastWriteNanos;

    SwitchPause(MonotonicClock mono, long intervalNanos) {
        this.mono = Objects.requireNonNull(mono, "mono");
        this.intervalNanos = intervalNanos;
    }

    /** {@code stream} paused at {@code frontier}, {@code since} the decision number then. */
    synchronized void paused(RunKey stream, long frontier, long since) {
        streams.putIfAbsent(stream, new Paused(frontier, since, State.PAUSED));
    }

    /** Whether a default-path run of {@code stream} still waits for its switch. */
    synchronized boolean holds(RunKey stream) {
        Paused p = streams.get(stream);
        return p != null && p.state() != State.READY;
    }

    /**
     * One drain tick: the due decisions written in one roster write, then
     * their tails discarded; a decided stream ready once {@code chainNext}
     * reaches its frontier; a ready stream unpaused once its default run
     * committed past it, or once the catalog has it fast again.
     *
     * @return whether a newer term deposed this one: nothing more is decided
     */
    boolean tick(Leader leader, RosterDecisions decisions, RosterWritten rosterWritten,
            ToLongFunction<RunKey> chainNext, Predicate<RunKey> fastNow) throws IOException {
        Map<RunKey, Long> due = new TreeMap<>();
        Map<RunKey, Integer> since = new HashMap<>();
        synchronized (this) {
            long now = mono.nanos();
            if (!written || now - lastWriteNanos >= intervalNanos) {
                for (Map.Entry<RunKey, Paused> e : streams.entrySet()) {
                    if (e.getValue().state() == State.PAUSED) {
                        due.put(e.getKey(), e.getValue().frontier());
                        since.put(e.getKey(), (int) e.getValue().since());
                    }
                }
            }
            if (!due.isEmpty()) {
                written = true;
                lastWriteNanos = now;
            }
        }
        if (!due.isEmpty()) {
            if (decisions.record(due, since::get) instanceof RosterDecisions.Deposed) {
                return true;
            }
            rosterWritten.run();
            for (Map.Entry<RunKey, Long> e : due.entrySet()) {
                // ⚠️ ONE STREAM's FAILURE IS ITS OWN (its review round 1, P2): it
                // stays PAUSED, its decision found written next tick, its
                // discard retried; the others go on.
                try {
                    leader.discardFrom(e.getKey(), e.getValue());
                } catch (IOException | RuntimeException failed) {
                    LOG.log(System.Logger.Level.WARNING, "the switch of " + e.getKey()
                            + " could not discard its tail; the next tick retries", failed);
                    continue;
                }
                synchronized (this) {
                    streams.computeIfPresent(e.getKey(), (k, p) -> p.to(State.DECIDED));
                }
            }
        }
        Map<RunKey, Paused> now;
        synchronized (this) {
            now = new HashMap<>(streams);
        }
        for (Map.Entry<RunKey, Paused> e : now.entrySet()) {
            RunKey stream = e.getKey();
            Paused p = e.getValue();
            long next = chainNext.applyAsLong(stream);
            if (p.state() == State.DECIDED && next >= p.frontier()) {
                synchronized (this) {
                    streams.computeIfPresent(stream, (k, q) -> q.to(State.READY));
                }
            } else if (p.state() == State.READY && (next > p.frontier() || fastNow.test(stream))) {
                leader.unpause(stream, next);
                synchronized (this) {
                    streams.remove(stream);
                }
            }
        }
        return false;
    }
}
