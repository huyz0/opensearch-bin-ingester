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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The pod side of JOIN (ADR-0081 §1; M13.26g): a JOIN sent to each term's
 * leader, its answer fenced and addressed before it is read, and JOINED
 * recorded only for the term asked.
 */
class TermJoinerTest {

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
    void aJOINIsSentToTheLeaderAndItsJoinedRecorded() throws Exception {
        List<byte[]> sent = new ArrayList<>();
        List<String> endpoints = new ArrayList<>();
        JoinedTerms terms = new JoinedTerms();
        TermJoiner joiner = new TermJoiner(POD, (endpoint, frame) -> {
            endpoints.add(endpoint);
            sent.add(frame);
            return joined(7, "uid-l", "uid-2", 4);
        }, fenceAt(7), terms, () -> FastFrame.Held.NONE);

        TermJoiner.Outcome outcome = joiner.join(7, "uid-l", "http://leader");

        assertThat(outcome).isEqualTo(new TermJoiner.Joined(7, 4, FastFrame.HeldStatus.NONE));
        assertThat(endpoints).containsExactly("http://leader");
        FastFrame.Frame frame = FastFrame.decode(sent.get(0));
        assertThat(frame.header()).isEqualTo(new FastFrame.Header(FastFrame.KIND_JOIN, 7, "uid-2",
                "uid-l"));
        assertThat(frame.body()).isEqualTo(new FastFrame.Join(POD, FastFrame.Held.NONE));
        assertThat(terms.mayHold(7)).isTrue();
    }

    @Test
    void anALREADYJoinedTermSendsNothing() throws Exception {
        JoinedTerms terms = new JoinedTerms();
        terms.joined(7, 0);
        TermJoiner joiner = new TermJoiner(POD, (endpoint, frame) -> {
            throw new AssertionError("nothing is sent");
        }, fenceAt(7), terms, () -> FastFrame.Held.NONE);

        assertThat(joiner.join(7, "uid-l", "http://leader"))
                .isEqualTo(new TermJoiner.AlreadyJoined(7));
    }

    @Test
    void aDEPOSEDLeadersAnswerIsFencedAndNothingRecorded() throws Exception {
        JoinedTerms terms = new JoinedTerms();
        TermJoiner joiner = new TermJoiner(POD, (endpoint, frame) -> joined(6, "uid-l", "uid-2", 0),
                fenceAt(7), terms, () -> FastFrame.Held.NONE);

        assertThatThrownBy(() -> joiner.join(6, "uid-l", "http://old")).isInstanceOf(IOException.class);
        assertThat(terms.mayHold(6)).isFalse();
    }

    @Test
    void anANSWERAddressedElsewhereOrFromAnotherPodIsRefused() throws Exception {
        JoinedTerms terms = new JoinedTerms();
        TermJoiner misaddressed = new TermJoiner(POD,
                (endpoint, frame) -> joined(7, "uid-l", "uid-9", 0), fenceAt(7), terms,
                () -> FastFrame.Held.NONE);
        TermJoiner impostor = new TermJoiner(POD,
                (endpoint, frame) -> joined(7, "uid-x", "uid-2", 0), fenceAt(7), terms,
                () -> FastFrame.Held.NONE);

        assertThatThrownBy(() -> misaddressed.join(7, "uid-l", "e")).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> impostor.join(7, "uid-l", "e")).isInstanceOf(IOException.class);
        assertThat(terms.mayHold(7)).isFalse();
    }

    @Test
    void aNEWERTermsAnswerRaisesTheFenceButJoinsNothing() throws Exception {
        JoinedTerms terms = new JoinedTerms();
        EpochFence fence = fenceAt(7);
        TermJoiner joiner = new TermJoiner(POD, (endpoint, frame) -> joined(8, "uid-l", "uid-2", 0),
                fence, terms, () -> FastFrame.Held.NONE);

        assertThat(joiner.join(7, "uid-l", "e")).isEqualTo(new TermJoiner.Superseded(8));
        assertThat(fence.highest()).isEqualTo(8);
        assertThat(terms.mayHold(7)).isFalse();
        assertThat(terms.mayHold(8)).isFalse();
    }

    @Test
    void aREFUSEDJoinIsReportedAndNothingRecorded() throws Exception {
        JoinedTerms terms = new JoinedTerms();
        TermJoiner joiner = new TermJoiner(POD, (endpoint, frame) -> FastFrame.encode(7, "uid-l",
                "uid-2", new FastFrame.Refused(FastFrame.Reason.NOT_ROSTERED, Optional.empty(),
                        "departed")), fenceAt(7), terms, () -> FastFrame.Held.NONE);

        assertThat(joiner.join(7, "uid-l", "e"))
                .isEqualTo(new TermJoiner.Refused(FastFrame.Reason.NOT_ROSTERED, "departed"));
        assertThat(terms.mayHold(7)).isFalse();
    }

    @Test
    void theJOINReportsWhatTheJournalHolds() throws Exception {
        FastFrame.Held holding = new FastFrame.Held(List.of(new FastFrame.HeldStream(
                new io.github.huyz0.os.biningester.format.RunKey(new java.util.UUID(1, 1), 0),
                List.of(new FastFrame.HeldGroup(6, 0, 10, 20)))));
        List<byte[]> sent = new ArrayList<>();
        TermJoiner joiner = new TermJoiner(POD, (endpoint, frame) -> {
            sent.add(frame);
            return joined(7, "uid-l", "uid-2", 0);
        }, fenceAt(7), new JoinedTerms(), () -> holding);

        joiner.join(7, "uid-l", "e");

        assertThat(((FastFrame.Join) FastFrame.decode(sent.get(0)).body()).held())
                .isEqualTo(holding);
    }
}
