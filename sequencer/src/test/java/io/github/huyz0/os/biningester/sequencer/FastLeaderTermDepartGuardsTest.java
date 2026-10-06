// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastLeaderTermDepartTest.POD;
import static io.github.huyz0.os.biningester.sequencer.FastLeaderTermDepartTest.STREAM;
import static io.github.huyz0.os.biningester.sequencer.FastLeaderTermDepartTest.header;
import static io.github.huyz0.os.biningester.sequencer.FastLeaderTermDepartTest.term3;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.put;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.read;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A departure's guards (M13.27k review round 1, P2, T2, T3): only a pod
 * departs itself, another term's DEPART or HELD is refused whichever side of
 * this term it is, and phase 1 is judged on the roster as it is now.
 */
class FastLeaderTermDepartGuardsTest {

    @Test
    void aPODCannotDepartAnother() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = term3(store, Map.of());
        term.answerJoin(header(FastFrame.KIND_JOIN, 3), new FastFrame.Join(POD,
                FastFrame.Held.NONE));

        FastFrame.Body answer = term.answerDepart(
                new FastFrame.Header(FastFrame.KIND_DEPART, 3, "uid-other", "uid-self"),
                new FastFrame.Depart(POD, 2, FastFrame.Held.NONE));

        assertThat(((FastFrame.Refused) answer).reason()).isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        assertThat(read(store, 3).member(POD.podUid()).orElseThrow().state())
                .isEqualTo(Roster.State.ROSTERED);
    }

    @Test
    void anotherTERMsDepartOrHeldIsRefusedOnEitherSide() throws Exception {
        FastLeaderTerm term = term3(new MemoryBinStore(), Map.of());

        for (long epoch : new long[] {2, 4}) {
            assertThat(((FastFrame.Refused) term.answerDepart(
                    header(FastFrame.KIND_DEPART, epoch),
                    new FastFrame.Depart(POD, 2, FastFrame.Held.NONE))).reason())
                    .as("DEPART of term %d", epoch).isEqualTo(FastFrame.Reason.NOT_ROSTERED);
            assertThat(((FastFrame.Refused) term.answerHeld(header(FastFrame.KIND_HELD, epoch),
                    new FastFrame.HeldReport(FastFrame.Held.NONE))).reason())
                    .as("HELD of term %d", epoch).isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        }
    }

    @Test
    void aPHASE1DepartIsJudgedOnTheRosterAsItIsNow() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = term3(store, Map.of());
        Roster own = read(store, 3);
        put(store, new Roster(3, own.predecessor(), own.leader(), own.members(),
                own.termRecord(), List.of(new Roster.Decision(0, STREAM, 5)), own.notBefore(),
                own.fencedBy(), own.closed()));

        FastFrame.Body answer = term.answerDepart(header(FastFrame.KIND_DEPART, 3),
                new FastFrame.Depart(POD, 1, new FastFrame.Held(List.of(
                        new FastFrame.HeldStream(STREAM,
                                List.of(new FastFrame.HeldGroup(2, 0, 6, 8)))))));

        assertThat(((FastFrame.HeldStatusReport) answer).status().streams().get(0).groups()
                .get(0).status()).isEqualTo(FastFrame.Status.SUPERSEDED);
    }
}
