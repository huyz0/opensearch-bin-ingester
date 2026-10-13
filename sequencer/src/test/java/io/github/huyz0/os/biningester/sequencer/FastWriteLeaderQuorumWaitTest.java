// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.ALL;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.WA;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.roster;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Each batch's quorum wait -- from its assignment to its exposure -- told to
 * the observer the server sets (M13.36), once per batch.
 */
class FastWriteLeaderQuorumWaitTest {

    @Test
    void aBATCHsQuorumWaitIsToldOnceAtItsExposure() throws Exception {
        FastWriteLeader l = FastWriteLeaderTest.leader(1 << 20, 1_000).leader();
        AtomicLong clock = new AtomicLong(1_000);
        List<Long> waits = new ArrayList<>();
        l.observe(clock::get, waits::add);

        l.commit(WA, commit(1, S1), roster(), ALL);
        l.flushGroup();
        clock.addAndGet(4_000_000);
        l.expose();
        l.expose();

        assertThat(waits).containsExactly(4_000_000L);
    }

    @Test
    void aBATCHAssignedBeforeTheObserverIsNotTimed() throws Exception {
        FastWriteLeader l = FastWriteLeaderTest.leader(1 << 20, 1_000).leader();
        l.commit(WA, commit(1, S1), roster(), ALL);
        l.flushGroup();
        List<Long> waits = new ArrayList<>();
        l.observe(() -> 9_000, waits::add);

        l.expose();

        assertThat(waits).as("no assignment time to measure from").isEmpty();
    }
}
