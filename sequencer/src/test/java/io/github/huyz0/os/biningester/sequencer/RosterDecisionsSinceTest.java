// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A switch's decision is written though an earlier one shares its resume
 * offset (M13.34p; ADR-0081 §8): the takeover's, numbered 0, supersedes none
 * of the entries assigned after it, so a switch deciding the stream at the
 * same offset must take its own number -- and a retry of the switch's own
 * write is still not written twice.
 */
class RosterDecisionsSinceTest {

    private static final RunKey S = new RunKey(new UUID(1, 1), 0);

    @Test
    void anEARLIERDecisionAtTheSameOffsetDoesNotStandInForALaterOne() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Roster r = FastTermStartTest.roster(3, 2, "self", false, 0, 0, false);
        FastTermStartTest.put(store, new Roster(r.epoch(), r.predecessor(), r.leader(),
                r.members(), r.termRecord(), List.of(new Roster.Decision(0, S, 100)),
                r.notBefore(), r.fencedBy(), r.closed()));
        RosterDecisions decisions = new RosterDecisions(store, "p", 3);

        decisions.record(Map.of(S, 100L), 1);
        decisions.record(Map.of(S, 100L), 1);

        assertThat(FastTermStartTest.read(store, 3).decisions()).as("its own, once")
                .containsExactly(new Roster.Decision(0, S, 100), new Roster.Decision(1, S, 100));
    }
}
