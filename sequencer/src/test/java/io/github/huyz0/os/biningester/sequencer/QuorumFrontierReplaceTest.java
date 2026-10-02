// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.QuorumFrontier.EntryId;
import io.github.huyz0.os.biningester.sequencer.QuorumFrontier.Holder;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * An unavailable holder replaced, a loss reported only where a quorum is
 * lost, the number of holders asked, committed entries dropped, and frontiers
 * never lowered (M13.27b review round 1, P1, P3, T1-T5).
 */
class QuorumFrontierReplaceTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);
    private static final RunKey B = new RunKey(new UUID(1, 2), 0);
    private static final Holder B1 = new Holder("uid-b1", "az-b");
    private static final Holder C1 = new Holder("uid-c1", "az-c");

    private static QuorumFrontier frontier() {
        QuorumFrontier f = new QuorumFrontier("uid-l", "az-a");
        f.open(A, 0);
        f.open(B, 0);
        return f;
    }

    private static EntryId at(RunKey stream, long first) {
        return new EntryId(7, 0, stream, first);
    }

    @Test
    void anASKToAHolderNoLongerAvailableIsReplacedInAnotherZone() {
        QuorumFrontier f = frontier();
        f.assigned(at(A, 0), 1, 2);
        f.asked(at(A, 0), B1);
        assertThat(f.holdersFor(at(A, 0), List.of(B1, C1))).as("b1 is asked and available")
                .isEmpty();

        assertThat(f.holdersFor(at(A, 0), List.of(C1)))
                .as("b1 refused with backpressure: no longer available").containsExactly(C1);
    }

    @Test
    void aLOSSIsReportedOnlyForAStreamThatRelied() {
        QuorumFrontier f = frontier();
        f.assigned(at(A, 0), 1, 2);
        f.copied(at(A, 0), B1);
        f.assigned(at(B, 0), 1, 2);
        f.copied(at(B, 0), C1);

        assertThat(f.lost("uid-b1")).containsExactly(A);
    }

    @Test
    void aLOSSNeverTouchesAnExposedEntryNorOneStillAtQuorum() {
        QuorumFrontier f = frontier();
        f.assigned(at(A, 0), 1, 2);
        f.copied(at(A, 0), B1);
        f.expose();
        f.assigned(at(B, 0), 1, 2);
        f.copied(at(B, 0), B1);
        f.copied(at(B, 0), C1);

        assertThat(f.lost("uid-b1"))
                .as("A's entry is exposed; B's still holds az-a and az-c").isEmpty();
        assertThat(f.expose()).containsExactly(at(B, 0));
    }

    @Test
    void atQTwoOneHolderIsAskedNotOnePerZone() {
        QuorumFrontier f = frontier();
        f.assigned(at(A, 0), 1, 2);

        assertThat(f.holdersFor(at(A, 0), List.of(B1, C1))).containsExactly(B1);
    }

    @Test
    void aCOMMITTEDEntryIsDroppedAndAnswersAboutItCountForNothing() {
        QuorumFrontier f = frontier();
        f.assigned(at(A, 0), 5, 2);

        f.committed(A, 5);

        assertThat(f.holdersFor(at(A, 0), List.of(B1, C1))).as("no longer tracked").isEmpty();
    }

    @Test
    void aFRONTIERIsNeverLoweredAndNeverSkipsAHole() {
        QuorumFrontier f = frontier();
        f.assigned(at(A, 0), 5, 1);
        f.expose();
        f.open(A, 2);
        f.committed(A, 3);
        assertThat(f.frontier(A)).isEqualTo(5);

        f.assigned(at(A, 8), 2, 1);

        assertThat(f.expose()).as("5-7 is a hole: 8 is not exposed past it").isEmpty();
        assertThat(f.frontier(A)).isEqualTo(5);
    }
}
