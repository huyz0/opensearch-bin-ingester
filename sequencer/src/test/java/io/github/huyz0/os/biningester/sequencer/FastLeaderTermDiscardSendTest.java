// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * A discarded batch's writer is told (M13.34t; ADR-0081 §2.3, §8): once a
 * switch's decision is durable and the tail dropped, the leader sends the
 * writer REFUSED, reason {@code discarded}, naming the batch's idempotency
 * key -- so it retries at once rather than after its EXPOSED wait.
 */
class FastLeaderTermDiscardSendTest {

    private static final Roster.Incarnation W =
            new Roster.Incarnation("w", "uid-w", "az-w", "http://w:1");

    /** Every frame sent, by endpoint. */
    private static final class Recording implements TermJoiner.Transport {
        final List<String> endpoints = new ArrayList<>();
        final List<FastFrame.Frame> frames = new ArrayList<>();

        @Override
        public byte[] exchange(String endpoint, byte[] frame) throws IOException {
            endpoints.add(endpoint);
            frames.add(FastFrame.decode(frame));
            return FastFrame.encode(3, W.podUid(), "uid-self",
                    new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
        }
    }

    private static FastLeaderTerm joined() throws Exception {
        FastLeaderTerm term = FastLeaderTermCommitTest.term3(new MemoryBinStore());
        assertThat(term.answerJoin(new FastFrame.Header(FastFrame.KIND_JOIN, 3, W.podUid(),
                "uid-self"), new FastFrame.Join(W, FastFrame.Held.NONE)))
                .isInstanceOf(FastFrame.Joined.class);
        return term;
    }

    private static FastFrame.Header header() {
        return new FastFrame.Header(FastWriteFrame.KIND_COMMIT, 3, W.podUid(), "uid-self");
    }

    private static FastFrame.Refused discarded(FastWriteFrame.Commit commit) {
        return new FastFrame.Refused(FastFrame.Reason.DISCARDED, Optional.of(commit.key()),
                "discarded by a switch to the default path");
    }

    /** A batch of {@code streams} assigned, then each stream paused and its tail discarded. */
    private static CommitDesk discarding(FastLeaderTerm term, FastWriteFrame.Commit commit,
            RunKey... streams) throws Exception {
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 1_000);
        CommitDesk desk = FastLeaderTermCommitTest.recorded(l);
        assertThat(term.answerCommit(header(), commit, desk))
                .isInstanceOf(FastWriteFrame.Assigned.class);
        for (RunKey stream : streams) {
            long frontier = l.leader().pauseForSwitch(stream).orElseThrow();
            l.leader().discardFrom(stream, frontier);
        }
        return desk;
    }

    @Test
    void aDISCARDEDBatchsWriterIsToldOnceAtItsRosteredEndpoint() throws Exception {
        FastLeaderTerm term = joined();
        FastWriteFrame.Commit commit = commit(W.podId(), 1, S1);
        CommitDesk desk = discarding(term, commit, S1);
        Recording transport = new Recording();

        assertThat(term.sendExposed(desk, transport)).isEqualTo(1);
        assertThat(term.sendExposed(desk, transport)).as("told once").isZero();

        assertThat(transport.endpoints).containsExactly("http://w:1");
        FastFrame.Frame frame = transport.frames.get(0);
        assertThat(frame.header().epoch()).isEqualTo(3);
        assertThat(frame.header().targetUid()).as("the writer's incarnation").isEqualTo("uid-w");
        assertThat(frame.body()).isEqualTo(discarded(commit));
    }

    @Test
    void aDISCARDIsToldThoughTheLeaderHoldsNoBatchAnyMore() throws Exception {
        // ⚠️ an idle tick reads no roster (M13.87), but a discard owed is not idle:
        // the default-path run can commit past the dropped batch before the drain.
        FastLeaderTerm term = joined();
        FastWriteFrame.Commit commit = commit(W.podId(), 1, S1);
        CommitDesk desk = discarding(term, commit, S1);
        desk.leader().unpause(S1, 110);
        assertThat(desk.leader().heldBatches()).as("the premise: none held").isZero();
        Recording transport = new Recording();

        assertThat(term.sendExposed(desk, transport)).isEqualTo(1);
        assertThat(transport.frames).extracting(FastFrame.Frame::body)
                .containsExactly(discarded(commit));
    }
}
