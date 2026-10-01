// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.binstore.Version;
import io.github.huyz0.os.biningester.format.Lease;

/**
 * What a {@link LeaseManager} tells fast mode's lease-time fence about its
 * writes and reads (ADR-0081 §3, M13.26): when each conditional write was SENT,
 * which landed, which lease version it read, and which lease it replaced.
 *
 * <p>⚠️ CALLED UNDER THE MANAGER'S LOCK, in the order the store saw the
 * writes, so an implementation never sees a renewal confirmed before the one
 * it followed.
 */
public interface LeaseTimeline {

    /** Does nothing: the default path's lease needs no timing (ADR-0002). */
    LeaseTimeline NONE = new LeaseTimeline() {
        @Override
        public long sending() {
            return 0;
        }

        @Override
        public long acquiring(Lease replaced) {
            return 0;
        }

        @Override
        public void won(Lease lease, long sent) {
        }

        @Override
        public void settled(Lease lease) {
        }

        @Override
        public void dropped() {
        }

        @Override
        public void observed(Version version, Lease current) {
        }
    };

    /** Immediately before a renewal of this holder's lease; the send instant. */
    long sending();

    /**
     * Immediately before a write that would acquire a lease: replacing
     * {@code replaced}, last {@link #observed}, or none ({@code null}) for the
     * first lease ever; the send instant.
     *
     * <p>⚠️ SAID BEFORE THE WRITE, not after it lands: a takeover whose answer
     * is lost is found landed by a later read ({@link #settled}), and it must
     * owe the successor's wait as much as one answered at once (M13.26 review
     * round 1, P1).
     */
    long acquiring(Lease replaced);

    /** That write landed and its answer arrived: {@code lease} is held from {@code sent}. */
    void won(Lease lease, long sent);

    /**
     * A write whose answer was lost is found landed by a later read: {@code lease}
     * is held, from a send this timeline already saw.
     */
    void settled(Lease lease);

    /** This instance holds no lease any more: fenced, released, or never won. */
    void dropped();

    /** Another holder's lease was read, at this version. */
    void observed(Version version, Lease current);
}
