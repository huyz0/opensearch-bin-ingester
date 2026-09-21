// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.sequencer.LeaseManager;
import java.io.IOException;
import java.lang.System.Logger;
import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * The GC role, held as a real lease on the object store (M8.5, FR-9, research
 * 06 §4).
 *
 * <p>⚠️ **{@link GcLease} HAD ONLY A TEST IMPLEMENTATION**, so {@link LeasedGc}
 * -- the fence that stops a pod that has lost the role from deleting -- could
 * not be constructed by anything that shipped. This is the production half, and
 * it adds nothing to the lease protocol: {@link LeaseManager} already carries
 * the CAS, the TTL and the epoch fencing, and this class asks it the three
 * questions GC needs.
 *
 * <p>⚠️ **A SEPARATE LEASE OBJECT FROM THE SEQUENCER'S**, and on purpose. The
 * sequencer term lives under {@code <prefix>/ctl/lease/}; this one is built
 * over a {@code LeaseConfig} whose prefix is {@code <prefix>/gc}, so its key is
 * {@code <prefix>/gc/ctl/lease/0.json}. Sharing the sequencer's object would
 * make every GC pass contend with the commit path for the one lease the whole
 * fleet's writes depend on -- and a release at the end of a pass would hand
 * the SEQUENCER term to whoever asked next. ⚠️ The key is a new persisted
 * object and is recorded as one: nothing else lists {@code <prefix>/gc/}, and
 * the orphan sweep lists {@code <prefix>/data/} only.
 *
 * <p>⚠️ **{@link #stillHeld} ISSUES NO REQUEST.** It is asked before every
 * DELETE batch, and a lease check that cost a GET would make the fence the
 * most expensive thing in the pass. It reads what this instance believes and
 * compares the expiry to the clock, with a margin, which is the question that
 * matters: a pause long enough to outlive the lease is exactly the pause after
 * which the next batch must not go out.
 */
public final class StoreGcLease implements GcLease {

    private static final Logger LOG = java.lang.System.getLogger(StoreGcLease.class.getName());

    /**
     * ⚠️ **HOW LONG BEFORE EXPIRY THE ROLE COUNTS AS GONE.** A DELETE batch is
     * one request, sent after this check; a batch that started with a
     * millisecond of lease left would land after another pod took the role. One
     * second covers a request's own latency with room, and it errs the safe way
     * -- a batch refused early is judged again next pass, a batch sent late is
     * two pods deleting from two views of the watermark table.
     */
    static final Duration EXPIRY_MARGIN = Duration.ofSeconds(1);

    private final LeaseManager manager;
    private final Clock clock;

    public StoreGcLease(LeaseManager manager, Clock clock) {
        this.manager = Objects.requireNonNull(manager, "manager");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Takes the role, or answers that someone else has it.
     *
     * <p>⚠️ **AN EMPTY {@code tryAcquire} IS NOT "SOMEONE ELSE HAS IT"** --
     * {@code LeaseManager} documents that it is also what an instance that
     * ALREADY holds the lease is told. So the answer is taken from
     * {@link LeaseManager#held()} after asking, never from the return value.
     *
     * <p>⚠️ **A STORE FAILURE IS "NO"**, never an exception: this runs on the
     * GC schedule, and an exception here would stop GC on this pod for good.
     * Not deleting is the safe answer to not knowing.
     */
    @Override
    public boolean acquire() {
        try {
            manager.tryAcquire();
        } catch (IOException unreachable) {
            LOG.log(Logger.Level.WARNING, () -> "the GC lease could not be taken; this pass "
                    + "is skipped and the next one asks again: " + unreachable);
            return false;
        }
        return stillHeld();
    }

    @Override
    public boolean stillHeld() {
        Optional<Lease> held = manager.held();
        return held.isPresent()
                && !held.get().isExpiredAt(clock.millis() + EXPIRY_MARGIN.toMillis());
    }

    /**
     * Gives the role back.
     *
     * <p>⚠️ **SO THE NEXT POD DOES NOT WAIT A TTL**, which matters more here
     * than it looks: GC passes are short and the lease is taken per pass, so a
     * pod that only ever let its GC lease expire would make every other pod's
     * pass wait out a TTL after each of its own.
     */
    @Override
    public void release() {
        try {
            manager.release();
        } catch (IOException unreachable) {
            LOG.log(Logger.Level.WARNING, () -> "the GC lease was not released; the next pod "
                    + "waits for its TTL: " + unreachable);
        }
    }
}
