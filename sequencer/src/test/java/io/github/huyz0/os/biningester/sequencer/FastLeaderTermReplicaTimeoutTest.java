// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S2;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The replica timeout (M13.27y; ADR-0081 §2.3): a holder that does not answer,
 * or refuses with backpressure, is unavailable for one timeout -- its ask no
 * longer covers its zone, so the copy goes to another -- and a waiting
 * REPLICA is re-sent to it every timeout, never awaited for ever.
 *
 * <p>⚠️ **AN UNREACHABLE HOLDER's ASK COVERED ITS ZONE FOR EVER**, so one
 * silent pod stalled every stream whose copy it was asked for.
 */
class FastLeaderTermReplicaTimeoutTest {

    private static final Roster.Incarnation W =
            new Roster.Incarnation("w", "uid-w", "az-a", "http://w:1");
    private static final Roster.Incarnation H =
            new Roster.Incarnation("h", "uid-h", "az-h", "http://h:1");
    private static final Roster.Incarnation G =
            new Roster.Incarnation("g", "uid-g", "az-g", "http://g:1");

    private enum Reply { ACK, FAIL, BACKPRESSURE }

    private static final class Scripted implements TermJoiner.Transport {
        final Deque<Reply> replies = new ArrayDeque<>();
        final List<String> endpoints = new ArrayList<>();

        @Override
        public byte[] exchange(String endpoint, byte[] frame) throws IOException {
            endpoints.add(endpoint);
            FastFrame.Frame sent = FastFrame.decode(frame);
            FastWriteFrame.Replica replica = (FastWriteFrame.Replica) sent.body();
            Reply reply = replies.isEmpty() ? Reply.ACK : replies.removeFirst();
            return switch (reply) {
                case ACK -> FastFrame.encode(3, sent.header().targetUid(), "uid-self",
                        new FastWriteFrame.ReplicaAck(replica.entries().get(0).assignedAfter(),
                                replica.entries().stream().map(e -> new FastWriteFrame.RunAt(
                                        e.key(), e.firstOffset())).toList()));
                case BACKPRESSURE -> FastFrame.encode(3, sent.header().targetUid(), "uid-self",
                        new FastFrame.Refused(FastFrame.Reason.BACKPRESSURE, Optional.empty(),
                                "full"));
                case FAIL -> throw new IOException("no answer within the timeout");
            };
        }
    }

    private final Mono mono = new Mono();

    private FastLeaderTerm term3(Roster.Incarnation... pods) throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastTermStartTest.put(store, FastTermStartTest.roster(1, -1, "x", false, 0, 0, false));
        FastTermStartTest.put(store, FastTermStartTest.roster(2, 1, "y", false, 0, 0, false));
        FastTermStartTest.latest(store, 2);
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(store, "p"),
                new EmptyTermCloser(store, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), new SimulatedClock(1_000_000L),
                        new Mono()), FastTermStartTest.SELF, Map::of)
                .open(3, () -> { }).orElseThrow();
        FastLeaderTerm term = new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(store, "p", 3, mono, Duration.ofMillis(250)),
                        nanos -> mono.advance(Duration.ofNanos(nanos))),
                key -> 0L, store, "p", mono, Duration.ofMillis(250));
        for (Roster.Incarnation pod : pods) {
            assertThat(term.answerJoin(new FastFrame.Header(FastFrame.KIND_JOIN, 3,
                    pod.podUid(), "uid-self"), new FastFrame.Join(pod, FastFrame.Held.NONE)))
                    .isInstanceOf(FastFrame.Joined.class);
        }
        mono.advance(Duration.ofSeconds(1));
        return term;
    }

    private static CommitDesk assigned(FastLeaderTerm term, long... seqs) throws Exception {
        CommitDesk desk = FastLeaderTermCommitTest.recorded(
                FastWriteLeaderTest.leader(1 << 20, 1_000));
        for (long seq : seqs) {
            assertThat(term.answerCommit(new FastFrame.Header(FastWriteFrame.KIND_COMMIT, 3,
                    W.podUid(), "uid-self"), commit(W.podId(), seq, S2), desk))
                    .isInstanceOf(FastWriteFrame.Assigned.class);
        }
        return desk;
    }

    @Test
    void anUNREACHABLEHoldersCopyGoesToAnotherZone() throws Exception {
        FastLeaderTerm term = term3(W, H, G);
        CommitDesk desk = assigned(term, 1);
        Scripted transport = new Scripted();
        transport.replies.add(Reply.FAIL);
        term.sendReplicas(desk, transport);

        assertThat(term.sendReplicas(desk, transport)).as("its ask covers no zone now")
                .isEqualTo(1);

        assertThat(transport.endpoints).containsExactly("http://h:1", "http://g:1");
        assertThat(desk.exposures()).hasSize(1);
    }

    @Test
    void aWAITINGReplicaIsResentAfterTheTimeoutNeverBefore() throws Exception {
        FastLeaderTerm term = term3(W, H);
        CommitDesk desk = assigned(term, 1);
        Scripted transport = new Scripted();
        transport.replies.add(Reply.FAIL);
        term.sendReplicas(desk, transport);

        mono.advance(FastLeaderTerm.REPLICA_TIMEOUT.minusMillis(1));
        assertThat(term.sendReplicas(desk, transport)).as("within the timeout").isZero();
        mono.advance(Duration.ofMillis(1));
        assertThat(term.sendReplicas(desk, transport)).as("re-sent at it").isEqualTo(1);

        assertThat(transport.endpoints).containsExactly("http://h:1", "http://h:1");
        assertThat(desk.exposures()).hasSize(1);
    }

    @Test
    void aBACKPRESSUREDPodIsAvailableAgainAfterTheTimeout() throws Exception {
        FastLeaderTerm term = term3(W, H);
        CommitDesk desk = assigned(term, 1);
        Scripted transport = new Scripted();
        transport.replies.add(Reply.BACKPRESSURE);
        term.sendReplicas(desk, transport);
        assertThat(term.answerCommit(new FastFrame.Header(FastWriteFrame.KIND_COMMIT, 3,
                W.podUid(), "uid-self"), commit(W.podId(), 2, S2), desk))
                .isInstanceOf(FastWriteFrame.Assigned.class);

        assertThat(term.sendReplicas(desk, transport)).as("unavailable within the timeout")
                .isZero();
        mono.advance(FastLeaderTerm.REPLICA_TIMEOUT);
        assertThat(term.sendReplicas(desk, transport)).as("batch 2, and H returns").isEqualTo(1);
        assertThat(term.sendReplicas(desk, transport)).as("then batch 1 it declined")
                .isEqualTo(1);

        assertThat(desk.exposures()).hasSize(2);
    }

    @Test
    void theREPLICATimeoutIsFarBelowTheLeaseTtl() {
        // ⚠️ ADR-0081's seventh constant: it bounds how long one slow holder
        // stalls a stream's acks, measured by M13.37.
        assertThat(FastLeaderTerm.REPLICA_TIMEOUT).isEqualTo(Duration.ofSeconds(1));
    }
}
