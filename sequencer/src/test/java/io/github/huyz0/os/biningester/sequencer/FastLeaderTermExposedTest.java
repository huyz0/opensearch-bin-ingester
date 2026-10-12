// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.Roster;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The leader sends each writer the EXPOSED it is owed (M13.27w; ADR-0081
 * §2.5): drained from the term's desk, addressed to the writer's incarnation
 * at the endpoint its roster lists.
 *
 * <p>⚠️ **NOTHING SENT AN EXPOSED**, so a batch whose quorum completed was
 * never answered to its writer, and the producer was never acked.
 *
 * <p>⚠️ A SEND THAT FAILS IS DROPPED, NOT OWED AGAIN: the writer, unanswered,
 * retries its COMMIT, and a retry of an exposed batch is exposed again
 * (M13.27s review round 1, P1).
 */
class FastLeaderTermExposedTest {

    private static final Roster.Incarnation W =
            new Roster.Incarnation("w", "uid-w", "az-w", "http://w:1");

    /** Every frame sent, by endpoint. */
    private static final class Recording implements TermJoiner.Transport {
        final List<String> endpoints = new ArrayList<>();
        final List<FastFrame.Frame> frames = new ArrayList<>();
        boolean failing;
        boolean failFirst;

        @Override
        public byte[] exchange(String endpoint, byte[] frame) throws IOException {
            endpoints.add(endpoint);
            frames.add(FastFrame.decode(frame));
            if (failing || failFirst && endpoints.size() == 1) {
                throw new IOException("unreachable");
            }
            return FastFrame.encode(3, W.podUid(), "uid-self",
                    new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
        }
    }

    @Test
    void anEXPOSEDBatchIsSentToItsWriterAtItsRosteredEndpoint() throws Exception {
        FastLeaderTerm term = FastLeaderTermCommitTest.term3(new MemoryBinStore());
        join(term);
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        CommitDesk desk = FastLeaderTermCommitTest.recorded(l);
        assertThat(term.answerCommit(header(), commit(W.podId(), 1, S1), desk))
                .isInstanceOf(FastWriteFrame.Assigned.class);
        Recording transport = new Recording();

        int sent = term.sendExposed(desk, transport);

        assertThat(sent).isEqualTo(1);
        assertThat(transport.endpoints).containsExactly("http://w:1");
        FastFrame.Frame frame = transport.frames.get(0);
        assertThat(frame.header().epoch()).isEqualTo(3);
        assertThat(frame.header().senderUid()).isEqualTo("uid-self");
        assertThat(frame.header().targetUid()).as("the writer's incarnation").isEqualTo("uid-w");
        assertThat(frame.body()).isEqualTo(new FastWriteFrame.Exposed(3,
                List.of(new FastWriteFrame.RunAt(S1, 100))));
    }

    @Test
    void anEXPOSEDIsSentOnce() throws Exception {
        FastLeaderTerm term = FastLeaderTermCommitTest.term3(new MemoryBinStore());
        join(term);
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        CommitDesk desk = FastLeaderTermCommitTest.recorded(l);
        term.answerCommit(header(), commit(W.podId(), 1, S1), desk);
        Recording transport = new Recording();
        term.sendExposed(desk, transport);

        assertThat(term.sendExposed(desk, transport)).as("nothing owed now").isZero();
        assertThat(transport.endpoints).hasSize(1);
    }

    @Test
    void aFAILEDSendIsDroppedAndNeverThrows() throws Exception {
        FastLeaderTerm term = FastLeaderTermCommitTest.term3(new MemoryBinStore());
        join(term);
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        CommitDesk desk = FastLeaderTermCommitTest.recorded(l);
        term.answerCommit(header(), commit(W.podId(), 1, S1), desk);
        Recording transport = new Recording();
        transport.failing = true;

        assertThat(term.sendExposed(desk, transport)).isZero();
        transport.failing = false;
        assertThat(term.sendExposed(desk, transport)).as("the writer's retry re-owes it")
                .isZero();
        assertThat(transport.endpoints).hasSize(1);
    }

    @Test
    void aFAILEDSendDoesNotStopTheNext() throws Exception {
        // ⚠️ M13.27w review round 1, T2: a failure ending the loop would leave
        // every later writer unanswered.
        FastLeaderTerm term = FastLeaderTermCommitTest.term3(new MemoryBinStore());
        join(term);
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        CommitDesk desk = FastLeaderTermCommitTest.recorded(l);
        term.answerCommit(header(), commit(W.podId(), 1, S1), desk);
        term.answerCommit(header(), commit(W.podId(), 2, S1), desk);
        Recording transport = new Recording();
        transport.failFirst = true;

        assertThat(term.sendExposed(desk, transport)).isEqualTo(1);
        assertThat(transport.endpoints).hasSize(2);
    }

    @Test
    void aWRITERTheRosterNoLongerListsIsNotSentTo() throws Exception {
        FastLeaderTerm term = FastLeaderTermCommitTest.term3(new MemoryBinStore());
        join(term);
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        CommitDesk desk = FastLeaderTermCommitTest.recorded(l);
        // B is in the fixture leader's roster, never in term 3's
        l.leader().commit(FastWriteLeaderTest.B, commit(1, S1), FastWriteLeaderTest.roster(),
                FastWriteLeaderTest.ALL);
        l.leader().flushGroup();
        Recording transport = new Recording();

        assertThat(term.sendExposed(desk, transport)).isZero();
        assertThat(transport.endpoints).isEmpty();
    }

    private static void join(FastLeaderTerm term) throws Exception {
        assertThat(term.answerJoin(new FastFrame.Header(FastFrame.KIND_JOIN, 3, W.podUid(),
                "uid-self"), new FastFrame.Join(W, FastFrame.Held.NONE)))
                .isInstanceOf(FastFrame.Joined.class);
    }

    private static FastFrame.Header header() {
        return new FastFrame.Header(FastWriteFrame.KIND_COMMIT, 3, W.podUid(), "uid-self");
    }
}
