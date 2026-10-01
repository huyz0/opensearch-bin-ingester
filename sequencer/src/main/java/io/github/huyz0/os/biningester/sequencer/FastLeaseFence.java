// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.Version;
import io.github.huyz0.os.biningester.format.Lease;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

/**
 * Fast mode's lease-time fence (ADR-0081 §3, M13.26): whether this pod, as
 * leader, may expose a fast entry now, and whether, as a successor, it may
 * assign or decide yet.
 *
 * <p>⚠️ THE LEASE BECOMES PART OF SAFETY HERE, for one case only: an exposure
 * that needs no other pod ({@code q = 1} written through the leader) has no
 * epoch fence behind it (ADR-0081 §11's exception to ADR-0002). Everywhere else
 * the lease stays liveness.
 *
 * <p>⚠️ TWO CLOCKS, EACH FROM THE SEND. The holder is valid while its wall
 * clock is before the stamped expiry less the margin AND its monotonic clock is
 * before the renewal's send plus the TTL less the margin: a stopped or
 * backwards clock of either kind is caught by the other, and a renewal answered
 * late buys nothing, since validity runs from the send. The successor waits
 * until its wall clock passes the replaced lease's expiry plus the margin AND
 * its monotonic clock passes its first read of the replaced VERSION plus the
 * TTL plus the margin. That version was written after the old holder's last
 * send, so the monotonic wait outlasts the old holder's monotonic validity
 * whatever either wall clock says, provided the two clocks' rates differ by
 * less than {@code 2 · margin / (TTL + margin)}, about 18 %.
 *
 * <p>⚠️ NOT YET THE WHOLE WAIT: a successor's {@code notBefore} also takes the
 * inherited {@code notBefore} of every unclosed roster it walks, and a graceful
 * handover exempts it from both waits -- the roster's, M13.26b. This class
 * answers the lease's part.
 */
public final class FastLeaseFence implements LeaseTimeline {

    private final Clock wall;
    private final MonotonicClock mono;
    private final long ttlNanos;
    private final long marginMillis;
    private final long marginNanos;

    private boolean holding;
    private boolean stopped;
    private long expiresAtMillis;
    /** The send of the last write known landed; valid only while {@link #holding}. */
    private long confirmedSent;
    /** The first send since the last landed one, which a later read may settle. */
    private long pendingSent;
    private boolean pending;
    /** Whether the pending write acquires a lease, and the lease it replaces, if any. */
    private boolean pendingAcquisition;
    private Lease pendingReplaced;

    private Version watched;
    private long seenMono;
    private boolean waiting;
    private long notBeforeWallMillis;
    private long waitFromMono;

    public FastLeaseFence(Duration ttl, Clock wall, MonotonicClock mono) {
        Objects.requireNonNull(ttl, "ttl");
        if (ttl.isZero() || ttl.isNegative()) {
            throw new IllegalArgumentException("ttl must be positive: " + ttl);
        }
        this.wall = Objects.requireNonNull(wall, "wall");
        this.mono = Objects.requireNonNull(mono, "mono");
        this.ttlNanos = ttl.toNanos();
        // ⚠️ TTL / 10 (ADR-0081 §3): 1 s at the shipped 10 s TTL.
        this.marginMillis = ttl.toMillis() / 10;
        this.marginNanos = ttlNanos / 10;
    }

    /**
     * Whether this pod, as leader, may expose a fast entry now -- checked
     * immediately before each exposing send, never only at assignment.
     */
    public synchronized boolean mayExpose() {
        if (!holding || stopped) {
            return false;
        }
        return wall.millis() < expiresAtMillis - marginMillis
                && mono.nanos() - confirmedSent < ttlNanos - marginNanos;
    }

    /**
     * A leader shutting down stops exposing FIRST (ADR-0081 §9): a CONFIRM
     * arriving after its release must not be answered as complete.
     */
    public synchronized void stopExposing() {
        stopped = true;
    }

    /**
     * Whether the lease's part of a successor's wait has passed: true for a
     * term that replaced no lease, and for one whose wait both clocks have
     * served.
     */
    public synchronized boolean successorMayAssign() {
        if (!waiting) {
            return true;
        }
        return wall.millis() >= notBeforeWallMillis
                && mono.nanos() - waitFromMono >= ttlNanos + marginNanos;
    }

    /**
     * The wall-clock instant before which this term's leader may not assign:
     * the replaced lease's expiry plus the margin, or {@link Long#MIN_VALUE}
     * when it replaced none. The roster records it as {@code notBefore}.
     */
    public synchronized long notBeforeWallMillis() {
        return waiting ? notBeforeWallMillis : Long.MIN_VALUE;
    }

    @Override
    public synchronized long sending() {
        long now = mono.nanos();
        if (!pending) {
            pending = true;
            pendingSent = now;
            pendingAcquisition = false;
        }
        return now;
    }

    @Override
    public synchronized long acquiring(Lease replaced) {
        long now = mono.nanos();
        pending = true;
        pendingSent = now;
        pendingAcquisition = true;
        pendingReplaced = replaced;
        return now;
    }

    @Override
    public synchronized void won(Lease lease, long sent) {
        if (pending && pendingAcquisition) {
            startTerm();
        }
        holding = true;
        expiresAtMillis = lease.expiresAtMillis();
        confirmedSent = sent;
        pending = false;
    }

    @Override
    public synchronized void settled(Lease lease) {
        // ⚠️ THE EARLIEST SEND IT CAN BE. Settling a renewal, the lease found
        // may be the lost write's or the one before it, so validity keeps the
        // last CONFIRMED send; settling an acquisition, there is none, and the
        // only write that could have landed is the pending one.
        // ⚠️ THE ACQUISITION FIRST (M13.26 review round 2, P1): a pod taking
        // over its OWN expired lease still holds the old term here, and read
        // as a renewal it would start the new term owing no wait.
        if (pending && pendingAcquisition) {
            startTerm();
            holding = true;
            expiresAtMillis = lease.expiresAtMillis();
            confirmedSent = pendingSent;
        } else if (holding) {
            expiresAtMillis = lease.expiresAtMillis();
        }
        pending = false;
    }

    /** A new term: it owes the successor's wait exactly when it replaced a lease. */
    private void startTerm() {
        waiting = pendingReplaced != null;
        if (waiting) {
            notBeforeWallMillis = pendingReplaced.expiresAtMillis() + marginMillis;
            waitFromMono = seenMono;
        }
        pendingAcquisition = false;
        pendingReplaced = null;
    }

    @Override
    public synchronized void dropped() {
        holding = false;
        pending = false;
    }

    @Override
    public synchronized void observed(Version version, Lease current) {
        // ⚠️ RESET AT EVERY NEWER VERSION: each renewal is one, and the wait
        // must follow the LAST renewal's send, not the first one this pod saw.
        if (!version.equals(watched)) {
            watched = version;
            seenMono = mono.nanos();
        }
    }
}
