// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import java.io.IOException;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;

/**
 * The renew loop a {@link LocalSequencer} runs on its {@code lease-renewer}
 * thread. Moved out of {@code LocalSequencer} unchanged by M13.1d; the term's
 * state stays there and is reached through the arguments.
 */
final class LeaseRenewer implements Runnable {

    private static final System.Logger LOG =
            System.getLogger(LocalSequencer.class.getName());

    private final LeaseManager leases;
    private final LocalSequencer.RenewTicker ticker;
    private final BooleanSupplier closed;
    private final BooleanSupplier fenced;
    /** Marks the term fenced and stops its checkpoint writer, in that order. */
    private final Runnable fence;
    private final LongSupplier epoch;

    LeaseRenewer(LeaseManager leases, LocalSequencer.RenewTicker ticker, BooleanSupplier closed,
            BooleanSupplier fenced, Runnable fence, LongSupplier epoch) {
        this.leases = leases;
        this.ticker = ticker;
        this.closed = closed;
        this.fenced = fenced;
        this.fence = fence;
        this.epoch = epoch;
    }

    @Override
    public void run() {
        while (!closed.getAsBoolean() && !fenced.getAsBoolean()) {
            try {
                ticker.awaitNextRenew();
                if (closed.getAsBoolean()) {
                    return;
                }
                if (leases.renew().isEmpty()) {
                    // ⚠️ EMPTY MEANS FENCED, and it is the ONLY thing that does.
                    // Another node holds the term now, and it has sealed this
                    // chain; committing on would reassign offsets a successor
                    // has already issued, which is I2.
                    // ⚠️ THE WRITER STOPS WITH THE LEASE. A fenced node still
                    // holds a live CheckpointWriter, and a checkpoint PUT is a
                    // write into the chain's own prefix -- the thing being
                    // fenced is exactly the right to make it.
                    fence.run();
                    // ⚠️ NOT NECESSARILY A TAKEOVER, and an earlier draft of
                    // this line said it was. ADR-0027 accepts a case where the
                    // renew comes back empty because this node's OWN late write
                    // landed between the refresh and the CAS -- nobody took the
                    // term. From in here the two are indistinguishable, which
                    // is why stopping is right and why the message must not
                    // send an operator hunting for a successor that may not
                    // exist.
                    // ⚠️ ERROR, ABOVE the renewer-died case below, because this
                    // one is PERMANENT until restart while a lapsed renewer
                    // merely fails over at the TTL. An earlier draft had the
                    // severities the other way round.
                    LOG.log(System.Logger.Level.ERROR,
                            "the lease renew at epoch " + epoch.getAsLong() + " returned empty: "
                                    + "either another node took the term or this one self-fenced "
                                    + "after an ambiguous write (ADR-0027). This node will not "
                                    + "sequence again and must be restarted to rejoin.");
                    return;
                }
            } catch (InterruptedException stopping) {
                Thread.currentThread().interrupt();
                return;
            } catch (IOException transientFailure) {
                // ⚠️ NOT FENCED. `renew`'s contract separates an IOException --
                // the store was unreachable, or the lock could not be taken --
                // from an empty result, and only the latter means the term is
                // gone. Standing down here would turn every store hiccup into a
                // cluster-wide failover, which is the outage this task removes
                // rather than one it should add. The next tick retries.
                LOG.log(System.Logger.Level.WARNING,
                        "a lease renew failed; the term is untouched and the next tick retries",
                        transientFailure);
            } catch (Throwable died) {
                // ⚠️ THE RENEWER MUST NOT DIE SILENTLY. If it does, the lease
                // lapses and the cluster fails over at the TTL -- survivable,
                // and exactly the behaviour that existed before this task, but
                // an operator has no other way to learn it happened.
                LOG.log(System.Logger.Level.WARNING,
                        "the lease renewer terminated; this node's term will lapse at its TTL "
                                + "and the cluster will fail over -- survivable, unlike a fence",
                        died);
                return;
            }
        }
    }
}
