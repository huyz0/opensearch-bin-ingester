// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.Lease;

/**
 * Evidence that a lease's holder is gone, on which a follower may take the
 * term before it expires (M8.13, NFR-9, ADR-0007).
 *
 * <p>⚠️ **EVIDENCE, NOT SUSPICION.** A lease's TTL is what bounds failover
 * when nothing else is known: about 10 s, twice NFR-9's 5 s. The early
 * challenge closes that gap, and it may fire only on a positive signal that
 * the holder is gone -- an endpoint the platform removed. A silence is not
 * such a signal, and a challenge on one would turn every slow renewal into a
 * failover.
 *
 * <p>⚠️ **BEING WRONG COSTS A FAILOVER, NEVER A WRITE** (ADR-0002). The lease
 * is liveness; safety is the epoch in the object path and the write-once
 * chain. A holder that was challenged while alive finds its chain SEALed at
 * its next commit and is fenced, and its producers retry elsewhere.
 */
@FunctionalInterface
public interface LeaseChallenge {

    /** No evidence is ever available: the TTL alone bounds failover. */
    LeaseChallenge NEVER = lease -> false;

    /** Whether there is positive evidence that {@code current}'s holder is gone. */
    boolean holderGone(Lease current);
}
