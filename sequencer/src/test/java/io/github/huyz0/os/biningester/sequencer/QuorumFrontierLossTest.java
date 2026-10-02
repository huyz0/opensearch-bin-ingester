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
 * An answer from a pod already reported lost, a stream that never relied on
 * it, and a stream held between a loss and its decision (M13.27b review round
 * 2, P4, P6, T6, T7).
 */
class QuorumFrontierLossTest {

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
    void anACKInFlightFromALostPodNeverCompletesAnEntry() {
        QuorumFrontier f = frontier();
        f.assigned(at(A, 0), 1, 2);
        f.asked(at(A, 0), B1);
        f.lost("uid-b1");

        f.copied(at(A, 0), B1);
        f.discard(A, 0);
        f.assigned(new EntryId(7, 1, A, 0), 1, 2);
        f.copied(new EntryId(7, 1, A, 0), B1);

        assertThat(f.expose()).as("b1 is lost: none of its copies count").isEmpty();
        assertThat(f.holdersFor(new EntryId(7, 1, A, 0), List.of(B1, C1))).containsExactly(C1);
        f.returned("uid-b1");
        f.copied(new EntryId(7, 1, A, 0), B1);
        assertThat(f.expose()).as("ready again, its copies count").hasSize(1);
    }

    @Test
    void aSTREAMThatNeverReliedOnTheLostPodIsNotReportedThoughIncomplete() {
        QuorumFrontier f = frontier();
        f.assigned(at(A, 0), 1, 2);
        f.copied(at(A, 0), B1);
        f.assigned(at(B, 0), 1, 2);
        f.asked(at(B, 0), C1);

        assertThat(f.lost("uid-b1")).containsExactly(A);
    }

    @Test
    void aSTREAMLeftShortIsHeldUntilItsDiscard() {
        QuorumFrontier f = frontier();
        f.assigned(at(A, 0), 1, 2);
        f.copied(at(A, 0), B1);
        f.lost("uid-b1");

        f.copied(at(A, 0), C1);

        assertThat(f.expose()).as("held: the decision will discard from 0").isEmpty();
        f.discard(A, 0);
        f.assigned(new EntryId(7, 1, A, 0), 1, 1);
        assertThat(f.expose()).hasSize(1);
    }
}
