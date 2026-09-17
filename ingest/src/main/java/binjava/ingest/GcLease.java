// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

/**
 * The lease that makes GC a single-writer role (M7.9, FR-9, research 06 §4).
 *
 * <p>⚠️ NEVER TWO PODS AT ONCE. Two passes sweeping together are two processes
 * deciding what is unread from two different views of the watermark table, and
 * the loser's view can be the stale one — so what one keeps the other deletes.
 *
 * <p>⚠️ A SEAM RATHER THAN THE LEASE ITSELF. {@code LeaseManager} lives in
 * {@code sequencer} and carries the CAS, the TTL and the epoch fencing this
 * project already built; this interface is the two questions GC needs of it, so
 * the GC path can be driven by a test clock without a store round trip. Wiring
 * it to the real lease is the deployment's job, and no production {@code main()}
 * exists to do that yet (M5.91).
 */
public interface GcLease {

    /** Takes the role for this pass, or answers false because someone else has it. */
    boolean acquire();

    /**
     * Whether this pod STILL holds it.
     *
     * <p>⚠️ ASKED AGAIN BEFORE EVERY IRREVERSIBLE STEP, not once per pass. A
     * check only at the start lets a whole pass's worth of deletes through
     * after the lease has moved — and a pause long enough to lose a lease is
     * exactly the pause that makes the rest of the pass slow enough to matter.
     */
    boolean stillHeld();

    /** Gives it back, so the next pod does not wait a whole TTL for it. */
    void release();
}
