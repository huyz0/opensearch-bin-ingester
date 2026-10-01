// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.format.Roster;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The fence at the ANSWER, not only before the send, and before an earlier
 * join (M13.26g review round 2, P1, T1).
 */
class TermJoinerAnswerFenceTest {

    private static final Roster.Incarnation POD =
            new Roster.Incarnation("ingester-2", "uid-2", "az-b", "http://pod-2");
    private static final String LEASE = "p/ctl/lease/0.json";

    private static EpochFence fenceAt(long epoch) throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        store.put(LEASE, Body.ofBytes(new Lease(epoch, "leader", "", 1_000).encode()));
        return EpochFence.start(store, LEASE, Optional.empty());
    }

    @Test
    void aFENCERaisedWhileTheJoinIsInFlightRefusesItsAnswer() throws Exception {
        EpochFence fence = fenceAt(7);
        JoinedTerms terms = new JoinedTerms();
        TermJoiner joiner = new TermJoiner(POD, (endpoint, frame) -> {
            fence.raise(8);
            return FastFrame.encode(7, "uid-l", "uid-2",
                    new FastFrame.Joined(0, FastFrame.HeldStatus.NONE));
        }, fence, terms, () -> FastFrame.Held.NONE);

        assertThatThrownBy(() -> joiner.join(7, "uid-l", "e"))
                .as("a successor's FENCE reached the pod while term 7's leader answered")
                .isInstanceOf(TermJoiner.Fenced.class)
                .satisfies(t -> assertThat(((TermJoiner.Fenced) t).fence()).isEqualTo(8));
        assertThat(terms.mayHold(7)).isFalse();
    }

    @Test
    void aTERMJoinedThenDeposedIsFencedNotAlreadyJoined() throws Exception {
        EpochFence fence = fenceAt(6);
        JoinedTerms terms = new JoinedTerms();
        terms.joined(6, 0);
        fence.raise(7);
        TermJoiner joiner = new TermJoiner(POD, (endpoint, frame) -> {
            throw new AssertionError("nothing is sent");
        }, fence, terms, () -> FastFrame.Held.NONE);

        assertThatThrownBy(() -> joiner.join(6, "uid-l", "e"))
                .isInstanceOf(TermJoiner.Fenced.class);
    }
}
