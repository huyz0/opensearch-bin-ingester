// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.format.Roster;
import java.io.IOException;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * JOINED's closed terms, an older term's answer, a misaddressed answer that
 * must not raise the fence, and a term already below it (M13.26g review round
 * 1, P2, T1-T3).
 */
class TermJoinerFenceTest {

    private static final Roster.Incarnation POD =
            new Roster.Incarnation("ingester-2", "uid-2", "az-b", "http://pod-2");
    private static final String LEASE = "p/ctl/lease/0.json";

    private static EpochFence fenceAt(long epoch) throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        store.put(LEASE, Body.ofBytes(new Lease(epoch, "leader", "", 1_000).encode()));
        return EpochFence.start(store, LEASE, Optional.empty());
    }

    private static byte[] joined(long epoch, String sender, String target, long closedThrough) {
        return FastFrame.encode(epoch, sender, target,
                new FastFrame.Joined(closedThrough, FastFrame.HeldStatus.NONE));
    }

    @Test
    void JOINEDsClosedThroughForgetsAClosedTerm() throws Exception {
        JoinedTerms terms = new JoinedTerms();
        terms.joined(3, 0);
        TermJoiner joiner = new TermJoiner(POD, (e, f) -> joined(7, "uid-l", "uid-2", 4),
                fenceAt(7), terms, () -> FastFrame.Held.NONE);

        joiner.join(7, "uid-l", "e");

        assertThat(terms.mayHold(3)).as("term 3 is closed: its entries are dropped").isFalse();
        assertThat(terms.mayHold(7)).isTrue();
    }

    @Test
    void anOLDERTermsAnswerTheFenceAdmitsJoinsNothing() throws Exception {
        JoinedTerms terms = new JoinedTerms();
        TermJoiner joiner = new TermJoiner(POD, (e, f) -> joined(6, "uid-l", "uid-2", 0),
                fenceAt(5), terms, () -> FastFrame.Held.NONE);

        assertThat(joiner.join(7, "uid-l", "e")).isEqualTo(new TermJoiner.Superseded(6));
        assertThat(terms.mayHold(7)).isFalse();
        assertThat(terms.mayHold(6)).isFalse();
    }

    @Test
    void aMISADDRESSEDAnswerNeverRaisesTheFence() throws Exception {
        EpochFence fence = fenceAt(7);
        TermJoiner misaddressed = new TermJoiner(POD, (e, f) -> joined(9, "uid-l", "uid-9", 0),
                fence, new JoinedTerms(), () -> FastFrame.Held.NONE);
        TermJoiner impostor = new TermJoiner(POD, (e, f) -> joined(9, "uid-x", "uid-2", 0),
                fence, new JoinedTerms(), () -> FastFrame.Held.NONE);

        assertThatThrownBy(() -> misaddressed.join(7, "uid-l", "e")).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> impostor.join(7, "uid-l", "e")).isInstanceOf(IOException.class);

        assertThat(fence.highest()).isEqualTo(7);
    }

    @Test
    void aTERMBelowTheFenceIsFinalAndSendsNothing() throws Exception {
        TermJoiner joiner = new TermJoiner(POD, (e, f) -> {
            throw new AssertionError("nothing is sent to a deposed leader");
        }, fenceAt(7), new JoinedTerms(), () -> FastFrame.Held.NONE);

        assertThatThrownBy(() -> joiner.join(6, "uid-l", "e"))
                .isInstanceOf(TermJoiner.Fenced.class)
                .satisfies(t -> assertThat(((TermJoiner.Fenced) t).fence()).isEqualTo(7));
    }
}
