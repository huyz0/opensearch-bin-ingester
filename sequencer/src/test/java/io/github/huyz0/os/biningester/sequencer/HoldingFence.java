// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import io.github.huyz0.os.biningester.format.Lease;
import java.time.Clock;
import java.time.Duration;

/**
 * A lease fence already holding its lease (M13.32a): a term built on it may
 * expose until its clocks pass the lease's validity, as a leader's would right
 * after it won the lease -- for a rig that is not itself about the lease.
 */
final class HoldingFence {

    private HoldingFence() {
    }

    static FastLeaseFence of(Duration ttl, Clock wall, MonotonicClock mono) {
        FastLeaseFence fence = new FastLeaseFence(ttl, wall, mono);
        long sent = fence.acquiring(null);
        fence.won(new Lease(1, "self", "uid-self", "self:1", wall.millis() + ttl.toMillis()),
                sent);
        return fence;
    }
}
