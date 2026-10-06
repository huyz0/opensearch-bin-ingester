// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.Lease;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

/**
 * How a pod learns its leader (ADR-0081 §1, amended by M13.27n): one look
 * reads the lease and JOINs the term it names when that term is newer than the
 * last this pod joined and another incarnation leads it.
 *
 * <p>⚠️ A LOOK NEVER THROWS: an unreadable lease, a refused or unanswered JOIN
 * -- the leader's term start not finished yet, or the leader gone -- is asked
 * again at the next look. A term the pod's fence already passed is never
 * joined, and a term it departed is not asked again.
 */
public final class LeaderWatch {

    private static final System.Logger LOG = System.getLogger(LeaderWatch.class.getName());

    /** Waits between looks: a sleep in production. */
    @FunctionalInterface
    public interface Sleeper {
        void sleep(java.time.Duration interval) throws InterruptedException;
    }

    /** Reads the lease: one GET; absent or unreadable is an {@link IOException}. */
    @FunctionalInterface
    public interface LeaseReader {
        Lease read() throws IOException;
    }

    private final String selfUid;
    private final LeaseReader lease;
    private final TermJoiner joiner;
    private final java.util.function.BooleanSupplier leading;
    /**
     * The newest term joined, or given up: never asked again. ⚠️ NO LOCK
     * (M13.27r review round 1, P2): only the watch's own thread looks, and a
     * lock held across the lease GET and the JOIN would hold any other caller
     * for a peer timeout (java-style rule 6).
     */
    private volatile long done;

    /**
     * @param leading whether this pod holds a term now: it then reads nothing,
     *     since a leader is a member of its roster from its creation and its
     *     idle cost is its renewals alone (M8's criterion 3)
     */
    public LeaderWatch(String selfUid, LeaseReader lease, TermJoiner joiner,
            java.util.function.BooleanSupplier leading) {
        this.selfUid = Objects.requireNonNull(selfUid, "selfUid");
        this.lease = Objects.requireNonNull(lease, "lease");
        this.joiner = Objects.requireNonNull(joiner, "joiner");
        this.leading = Objects.requireNonNull(leading, "leading");
    }

    /**
     * Looks once per {@code every} until {@code stopped} -- the watch's whole
     * rate, one lease GET per interval (ADR-0081 §1, amended by M13.27n; its
     * review round 2, T9).
     *
     * <p>⚠️ STOPPED BY A FLAG AS WELL AS AN INTERRUPT (its review round 2, P4):
     * a store or client that turns an interrupt into an exception without
     * setting it again would otherwise keep the watch looking after its pod
     * stopped.
     */
    public void run(java.time.Duration every, Sleeper sleeper,
            java.util.function.BooleanSupplier stopped) {
        while (!stopped.getAsBoolean() && !Thread.currentThread().isInterrupted()) {
            look();
            try {
                sleeper.sleep(every);
            } catch (InterruptedException stopping) {
                return;
            }
        }
    }

    /**
     * Reads the lease once and joins the term it names, if it should.
     *
     * <p>⚠️ UNCHECKED FAILURES ARE CAUGHT TOO (M13.27n review round 1, P3): the
     * watch is one loop, and an escape would end it -- the pod would join no
     * later term until it restarts.
     */
    public Optional<TermJoiner.Outcome> look() {
        try {
            return lookOnce();
        } catch (RuntimeException bug) {
            LOG.log(System.Logger.Level.WARNING, "a look at the lease failed; the next "
                    + "look tries again", bug);
            return Optional.empty();
        }
    }

    private Optional<TermJoiner.Outcome> lookOnce() {
        if (leading.getAsBoolean()) {
            return Optional.empty();
        }
        Lease current;
        try {
            current = lease.read();
        } catch (IOException noLease) {
            return Optional.empty();
        }
        String leader = current.holderPodUid();
        // ⚠️ A LEGACY LEASE NAMING NO UID cannot be addressed: a JOIN is to an
        // incarnation, never to a pod name a restart may reuse.
        if (leader.isBlank() || leader.equals(selfUid) || current.epoch() <= done) {
            return Optional.empty();
        }
        TermJoiner.Outcome outcome;
        try {
            outcome = joiner.join(current.epoch(), leader, current.holderEndpoint());
        } catch (TermJoiner.Fenced fenced) {
            done = current.epoch();
            return Optional.empty();
        } catch (IOException notNow) {
            LOG.log(System.Logger.Level.DEBUG, () -> "joining term " + current.epoch()
                    + " did not finish; the next look asks again: " + notNow);
            return Optional.empty();
        }
        switch (outcome) {
            case TermJoiner.Joined joined -> done = current.epoch();
            case TermJoiner.AlreadyJoined already -> done = current.epoch();
            case TermJoiner.Refused refused -> {
                if (refused.reason() == io.github.huyz0.os.biningester.format.FastFrame.Reason
                        .DEPARTING) {
                    done = current.epoch();
                }
            }
            case TermJoiner.Superseded superseded -> {
            }
        }
        return Optional.of(outcome);
    }
}
