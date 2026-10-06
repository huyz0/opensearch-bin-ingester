// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import io.github.huyz0.os.biningester.sequencer.FastTermStartRaceTest.HookedStore;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * An outcome nobody waited for is never a later JOIN's answer (M13.27j review
 * round 2, T2): a pod's join whose handler failed is answered by another
 * handler's flush with nobody waiting, and the pod's next JOIN reads the
 * roster again -- a pod departed since is refused, not answered from before.
 */
class JoinDeskStaleOutcomeTest {

    @Test
    void anOUTCOMENobodyWaitedForIsNotALaterJoinsAnswer() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        new FastTermStart(backing, "p").start(7, FastTermStartTest.SELF, Map.of(), 0,
                Optional.empty());
        HookedStore store = new HookedStore(backing, Roster.key("p", 7), b -> {
            throw new IOException("store down");
        });
        JoinDesk desk = new JoinDesk(new RosterJoins(store, "p", 7, new Mono(),
                Duration.ZERO), nanos -> { });
        Roster.Incarnation a = FastTermStartTest.incarnation("a");
        assertThatThrownBy(() -> desk.join(a)).isInstanceOf(IOException.class);
        assertThat(desk.join(FastTermStartTest.incarnation("b")))
                .as("b's flush rosters a too, with nobody waiting for a")
                .isInstanceOf(JoinDesk.Rostered.class);
        Roster r = FastTermStartTest.read(backing, 7);
        FastTermStartTest.put(backing, new Roster(7, r.predecessor(), r.leader(),
                List.of(r.members().get(0), new Roster.Member(a, Roster.State.DEPARTED),
                        r.members().get(2)),
                r.termRecord(), r.decisions(), r.notBefore(), r.fencedBy(), r.closed()));

        assertThat(desk.join(a)).as("a departed since: refused, not the stale answer")
                .isInstanceOf(JoinDesk.Departed.class);
    }
}
