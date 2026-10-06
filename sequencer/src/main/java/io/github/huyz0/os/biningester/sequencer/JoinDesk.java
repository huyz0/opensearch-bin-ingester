// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.Roster;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Where the leader's JOIN handlers wait for their roster write (ADR-0081 §1;
 * M13.27j): each JOIN is answered only once a roster write lists it, and joins
 * waiting together share one write, at most one per {@code min_upload_interval}
 * ({@link RosterJoins}).
 *
 * <p>⚠️ THE STORE IS NEVER REACHED UNDER THIS DESK'S LOCK (M13.27e review
 * patterns): a handler flushes outside it, and only the outcomes it learns are
 * recorded under it, so a slow roster write holds back no other handler's
 * bookkeeping. A flush that is not due costs no request and is waited out.
 *
 * <p>⚠️ A JOIN WHOSE FLUSH FAILED STAYS PENDING: the handler throws, the pod
 * retries, and the retry -- or another handler's flush -- answers it.
 */
public final class JoinDesk {

    /** Waits {@code nanos} before the next flush: a sleep in production. */
    @FunctionalInterface
    public interface Sleeper {
        void sleepNanos(long nanos) throws InterruptedException;
    }

    /** How a JOIN ended. */
    public sealed interface Outcome permits Rostered, Departed, Deposed {
    }

    /** The pod is a member of {@code roster}, durably. */
    public record Rostered(Roster roster) implements Outcome {
    }

    /** The pod departed this term earlier, and is not re-rostered. */
    public record Departed() implements Outcome {
    }

    /** A newer term fenced this one: no JOIN is answered any more. */
    public record Deposed(long newer) implements Outcome {
    }

    private final RosterJoins joins;
    private final Sleeper sleeper;
    private final Map<String, Outcome> outcomes = new HashMap<>();
    private Deposed deposed;

    public JoinDesk(RosterJoins joins, Sleeper sleeper) {
        this.joins = Objects.requireNonNull(joins, "joins");
        this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    }

    /**
     * Joins {@code pod} to the term, returning once the answer is known.
     *
     * @throws IOException the roster write failed; the join stays pending
     */
    public Outcome join(Roster.Incarnation pod) throws IOException {
        Objects.requireNonNull(pod, "pod");
        synchronized (this) {
            if (deposed != null) {
                return deposed;
            }
            outcomes.remove(pod.podUid());
        }
        joins.request(pod);
        while (true) {
            synchronized (this) {
                Outcome known = outcomes.remove(pod.podUid());
                if (known != null) {
                    return known;
                }
                if (deposed != null) {
                    return deposed;
                }
            }
            RosterJoins.Flush flush = joins.flush();
            long waitNanos = 0;
            synchronized (this) {
                switch (flush) {
                    case RosterJoins.Answered answered -> {
                        for (Roster.Incarnation joined : answered.joined()) {
                            outcomes.put(joined.podUid(), new Rostered(answered.roster()));
                        }
                        for (Roster.Incarnation refused : answered.refused()) {
                            outcomes.put(refused.podUid(), new Departed());
                        }
                    }
                    case RosterJoins.Deposed newer -> deposed = new Deposed(newer.newer());
                    case RosterJoins.NotDue notDue -> waitNanos = notDue.dueInNanos();
                    case RosterJoins.Idle idle -> {
                        // ⚠️ ANOTHER HANDLER'S FLUSH TOOK THIS JOIN: its
                        // outcome is recorded above, unless this pod joined
                        // again meanwhile and took it -- then ask again.
                        if (!outcomes.containsKey(pod.podUid()) && deposed == null) {
                            joins.request(pod);
                        }
                    }
                }
            }
            if (waitNanos > 0) {
                try {
                    sleeper.sleepNanos(waitNanos);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted while a JOIN waited for its roster write",
                            interrupted);
                }
            }
        }
    }
}
