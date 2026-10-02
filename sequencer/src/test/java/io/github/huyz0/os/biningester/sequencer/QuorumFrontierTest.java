// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.QuorumFrontier.EntryId;
import io.github.huyz0.os.biningester.sequencer.QuorumFrontier.Holder;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The leader's replica set and quorum frontier (ADR-0081 §2.3, §2.5; M13.27b).
 */
class QuorumFrontierTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);
    private static final RunKey B = new RunKey(new UUID(1, 2), 0);
    private static final Holder SAME_AZ = new Holder("uid-a2", "az-a");
    private static final Holder B1 = new Holder("uid-b1", "az-b");
    private static final Holder B2 = new Holder("uid-b2", "az-b");
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
    void atQOneTheLeadersOwnCopyExposesTheEntry() {
        QuorumFrontier f = frontier();
        f.assigned(at(A, 0), 5, 1);

        assertThat(f.expose()).containsExactly(at(A, 0));
        assertThat(f.frontier(A)).isEqualTo(5);
        assertThat(f.expose()).as("exposed once").isEmpty();
    }

    @Test
    void atQTwoOnlyACopyInAnotherZoneCompletesTheQuorum() {
        QuorumFrontier f = frontier();
        f.assigned(at(A, 0), 5, 2);
        f.copied(at(A, 0), SAME_AZ);
        assertThat(f.expose()).as("a second copy in the leader's own zone").isEmpty();

        f.copied(at(A, 0), B1);

        assertThat(f.expose()).containsExactly(at(A, 0));
    }

    @Test
    void EXPOSUREIsInOffsetOrderWhateverOrderCopiesArrive() {
        QuorumFrontier f = frontier();
        f.assigned(at(A, 0), 10, 2);
        f.assigned(at(A, 10), 5, 2);
        f.copied(at(A, 10), B1);
        assertThat(f.expose()).as("10-14 is complete, 0-9 is not").isEmpty();
        assertThat(f.frontier(A)).isZero();

        f.copied(at(A, 0), C1);

        assertThat(f.expose()).containsExactly(at(A, 0), at(A, 10));
        assertThat(f.frontier(A)).isEqualTo(15);
    }

    @Test
    void anANSWERAboutADiscardedEntryAtTheSameOffsetNeverCounts() {
        QuorumFrontier f = frontier();
        f.assigned(at(A, 0), 5, 2);
        f.discard(A, 0);
        EntryId again = new EntryId(7, 1, A, 0);
        f.assigned(again, 5, 2);

        f.copied(at(A, 0), B1);

        assertThat(f.expose()).as("the late answer named the superseded entry").isEmpty();
        f.copied(again, B1);
        assertThat(f.expose()).containsExactly(again);
    }

    @Test
    void HOLDERSAreChosenOnePerMissingZoneNeverTwiceNorForACoveredZone() {
        QuorumFrontier f = frontier();
        f.assigned(at(A, 0), 1, 3);

        List<Holder> first = f.holdersFor(at(A, 0), List.of(SAME_AZ, B1, B2, C1));
        assertThat(first).containsExactly(B1, C1);
        first.forEach(h -> f.asked(at(A, 0), h));

        assertThat(f.holdersFor(at(A, 0), List.of(SAME_AZ, B1, B2, C1)))
                .as("every zone is covered or asked").isEmpty();
    }

    @Test
    void aLOSTHolderStopsCountingForUnexposedEntriesOnly() {
        QuorumFrontier f = frontier();
        f.assigned(at(A, 0), 5, 2);
        f.copied(at(A, 0), B1);
        f.expose();
        f.assigned(at(A, 5), 5, 2);
        f.copied(at(A, 5), B1);
        f.assigned(at(B, 0), 5, 2);
        f.asked(at(B, 0), B1);

        assertThat(f.lost("uid-b1")).as("A's unexposed entry held a copy, B's was asked")
                .containsExactlyInAnyOrder(A, B);

        assertThat(f.expose()).as("A at 5 complete before the loss, now one copy short")
                .isEmpty();
        assertThat(f.frontier(A)).as("A's exposed entry is untouched").isEqualTo(5);
        assertThat(f.holdersFor(at(B, 0), List.of(B1, B2)))
                .as("a lost pod is never asked again; its zone is").containsExactly(B2);
    }

    @Test
    void aDISCARDTakesEntriesFromItsOffsetButNeverBelowTheFrontier() {
        QuorumFrontier f = frontier();
        f.assigned(at(A, 0), 5, 1);
        f.expose();
        f.assigned(at(A, 5), 5, 2);
        f.assigned(at(A, 10), 5, 2);

        f.discard(A, 5);

        f.copied(at(A, 5), B1);
        assertThat(f.expose()).isEmpty();
        assertThatThrownBy(() -> f.discard(A, 4)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aCOMMITDropsEntriesBelowItAndRaisesTheFrontier() {
        QuorumFrontier f = frontier();
        f.assigned(at(A, 0), 5, 2);
        f.assigned(at(A, 5), 5, 2);

        f.committed(A, 5);

        assertThat(f.frontier(A)).isEqualTo(5);
        f.copied(at(A, 5), B1);
        assertThat(f.expose()).containsExactly(at(A, 5));
        assertThatThrownBy(() -> f.assigned(at(A, 3), 1, 1))
                .as("below the frontier").isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anUNOPENEDStreamOrABadQuorumIsRefused() {
        QuorumFrontier f = new QuorumFrontier("uid-l", "az-a");

        assertThatThrownBy(() -> f.assigned(at(A, 0), 1, 1))
                .isInstanceOf(IllegalStateException.class);
        f.open(A, 0);
        assertThatThrownBy(() -> f.assigned(at(A, 0), 1, 4))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> f.assigned(at(A, 0), 0, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
