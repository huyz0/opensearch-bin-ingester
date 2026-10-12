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
 * The leader sends a batch's missing copies as REPLICAs and counts each
 * holder's REPLICA_ACK, its answer (M13.27x; ADR-0081 §2.3-2.4): a refusal
 * declines the copy, replaced in another pod, and a pod that answers without
 * refusing is chosen again (M13.27e's review round 2, P11).
 *
 * <p>⚠️ **NOTHING SENT A REPLICA**, so an index at {@code wal_quorum 2} whose
 * writer shares the leader's zone never reached its quorum, and no batch of
 * it was ever exposed.
 */
class FastLeaderTermReplicaTest {

    /** In the fixture leader's zone, az-a: the second zone must come from a holder. */
    private static final Roster.Incarnation W =
            new Roster.Incarnation("w", "uid-w", "az-a", "http://w:1");
    private static final Roster.Incarnation H =
            new Roster.Incarnation("h", "uid-h", "az-h", "http://h:1");
    private static final Roster.Incarnation G =
            new Roster.Incarnation("g", "uid-g", "az-g", "http://g:1");

    /** Answers scripted per call: an ack of what was sent, a refusal, a failure, a stranger's ack. */
    private enum Reply { ACK, REFUSE, FAIL, STRANGER, OTHER_TERM, DEPOSED }

    private static final class Scripted implements TermJoiner.Transport {
        final Deque<Reply> replies = new ArrayDeque<>();
        final List<String> endpoints = new ArrayList<>();

        @Override
        public byte[] exchange(String endpoint, byte[] frame) throws IOException {
            endpoints.add(endpoint);
            FastFrame.Frame sent = FastFrame.decode(frame);
            FastWriteFrame.Replica replica = (FastWriteFrame.Replica) sent.body();
            Reply reply = replies.isEmpty() ? Reply.ACK : replies.removeFirst();
            FastWriteFrame.ReplicaAck ack = new FastWriteFrame.ReplicaAck(
                    replica.entries().get(0).assignedAfter(), replica.entries().stream()
                            .map(e -> new FastWriteFrame.RunAt(e.key(), e.firstOffset()))
                            .toList());
            return switch (reply) {
                case ACK -> FastFrame.encode(3, sent.header().targetUid(), "uid-self", ack);
                case STRANGER -> FastFrame.encode(3, "uid-x", "uid-self", ack);
                case OTHER_TERM -> FastFrame.encode(4, sent.header().targetUid(), "uid-self", ack);
                case DEPOSED -> FastFrame.encode(3, sent.header().targetUid(), "uid-self",
                        new FastFrame.Refused(FastFrame.Reason.LOWER_EPOCH, Optional.empty(),
                                "fenced"));
                case REFUSE -> FastFrame.encode(3, sent.header().targetUid(), "uid-self",
                        new FastFrame.Refused(FastFrame.Reason.BACKPRESSURE, Optional.empty(),
                                "full"));
                case FAIL -> throw new IOException("unreachable");
            };
        }
    }

    /** The term's clock, advanced as it waits and by hand. */
    private final Mono mono = new Mono();

    /** Term 3, with every pod in {@code pods} joined; its clock advances as it waits. */
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
        // ⚠️ PAST min_upload_interval, so the roster the term reads lists them all.
        mono.advance(Duration.ofSeconds(1));
        return term;
    }

    /** W's batch {@code seq} on S2 at wal_quorum 2, assigned by {@code desk}. */
    private static void commitS2(FastLeaderTerm term, CommitDesk desk, long seq)
            throws Exception {
        assertThat(term.answerCommit(new FastFrame.Header(FastWriteFrame.KIND_COMMIT, 3,
                W.podUid(), "uid-self"), commit(W.podId(), seq, S2), desk))
                .isInstanceOf(FastWriteFrame.Assigned.class);
    }

    @Test
    void aREPLICAGoesToAHolderInAnUncoveredZoneAndItsAckExposesTheBatch() throws Exception {
        FastLeaderTerm term = term3(W, H);
        CommitDesk desk = FastLeaderTermCommitTest.recorded(
                FastWriteLeaderTest.leader(1 << 20, 1_000));
        commitS2(term, desk, 1);
        Scripted transport = new Scripted();

        assertThat(term.sendReplicas(desk, transport)).as("acked").isEqualTo(1);

        assertThat(transport.endpoints).containsExactly("http://h:1");
        assertThat(desk.exposures()).extracting(FastWriteLeader.Exposure::writerUid)
                .containsExactly(W.podUid());
    }

    @Test
    void anACKFromAnotherIncarnationCountsNothing() throws Exception {
        FastLeaderTerm term = term3(W, H);
        CommitDesk desk = FastLeaderTermCommitTest.recorded(
                FastWriteLeaderTest.leader(1 << 20, 1_000));
        commitS2(term, desk, 1);
        Scripted transport = new Scripted();
        transport.replies.add(Reply.STRANGER);

        assertThat(term.sendReplicas(desk, transport)).isZero();
        assertThat(desk.exposures()).isEmpty();
    }

    @Test
    void aREFUSEDCopyIsReplacedInAnotherPod() throws Exception {
        FastLeaderTerm term = term3(W, H, G);
        CommitDesk desk = FastLeaderTermCommitTest.recorded(
                FastWriteLeaderTest.leader(1 << 20, 1_000));
        commitS2(term, desk, 1);
        Scripted transport = new Scripted();
        transport.replies.add(Reply.REFUSE);
        term.sendReplicas(desk, transport);
        String refusedBy = transport.endpoints.get(0);

        assertThat(term.sendReplicas(desk, transport)).isEqualTo(1);

        assertThat(transport.endpoints).hasSize(2);
        assertThat(transport.endpoints.get(1)).as("the other zone's pod")
                .isNotEqualTo(refusedBy);
        assertThat(desk.exposures()).hasSize(1);
    }

    @Test
    void aPODThatDeclinedIsChosenAgainOnceItAnswersWithoutRefusing() throws Exception {
        FastLeaderTerm term = term3(W, H);
        CommitDesk desk = FastLeaderTermCommitTest.recorded(
                FastWriteLeaderTest.leader(1 << 20, 1_000));
        commitS2(term, desk, 1);
        Scripted transport = new Scripted();
        transport.replies.add(Reply.REFUSE);
        term.sendReplicas(desk, transport);
        assertThat(term.sendReplicas(desk, transport)).as("H declined batch 1: no other zone")
                .isZero();

        // ⚠️ A BACKPRESSURE REFUSAL MAKES H UNAVAILABLE FOR ONE REPLICA TIMEOUT
        // (M13.27y): this edit is intentional -- H is offered batch 2 once it
        // has passed, which is when the case below begins.
        mono.advance(FastLeaderTerm.REPLICA_TIMEOUT);
        commitS2(term, desk, 2);
        assertThat(term.sendReplicas(desk, transport)).as("H acks batch 2").isEqualTo(1);
        assertThat(term.sendReplicas(desk, transport)).as("and is offered batch 1 again")
                .isEqualTo(1);

        assertThat(transport.endpoints).containsOnly("http://h:1").hasSize(3);
        assertThat(desk.exposures()).hasSize(2);
    }

    @Test
    void anACKOfAnotherTermCountsNothing() throws Exception {
        // ⚠️ M13.27x review round 1, T1: only the sender varied before.
        FastLeaderTerm term = term3(W, H);
        CommitDesk desk = FastLeaderTermCommitTest.recorded(
                FastWriteLeaderTest.leader(1 << 20, 1_000));
        commitS2(term, desk, 1);
        Scripted transport = new Scripted();
        transport.replies.add(Reply.OTHER_TERM);

        assertThat(term.sendReplicas(desk, transport)).isZero();
        assertThat(desk.exposures()).isEmpty();
    }

    @Test
    void aDEPOSEDTermsRefusalLeavesTheCopyAskedNotDeclined() throws Exception {
        // ⚠️ M13.27x review round 1, T2: a fence is no holder's verdict on the
        // copy -- declined, it would be replaced on a word that says nothing
        // about the holder.
        FastLeaderTerm term = term3(W, H, G);
        CommitDesk desk = FastLeaderTermCommitTest.recorded(
                FastWriteLeaderTest.leader(1 << 20, 1_000));
        commitS2(term, desk, 1);
        Scripted transport = new Scripted();
        transport.replies.add(Reply.DEPOSED);
        term.sendReplicas(desk, transport);

        assertThat(term.sendReplicas(desk, transport)).as("still asked of the first").isZero();
        assertThat(transport.endpoints).hasSize(1);
    }

    @Test
    void aDEPARTEDPodIsNotSentTo() throws Exception {
        // ⚠️ M13.27x review round 1, T3: every pod was ROSTERED before.
        FastLeaderTerm term = term3(W, H);
        CommitDesk desk = FastLeaderTermCommitTest.recorded(
                FastWriteLeaderTest.leader(1 << 20, 1_000));
        commitS2(term, desk, 1);
        assertThat(term.answerDepart(new FastFrame.Header(FastFrame.KIND_DEPART, 3,
                H.podUid(), "uid-self"), new FastFrame.Depart(H, 2, FastFrame.Held.NONE)))
                .isInstanceOf(FastFrame.HeldStatusReport.class);
        mono.advance(Duration.ofSeconds(1));
        Scripted transport = new Scripted();

        assertThat(term.sendReplicas(desk, transport)).isZero();
        assertThat(transport.endpoints).isEmpty();
    }

    @Test
    void anUNREACHABLEHolderIsNeitherCountedNorThrown() throws Exception {
        FastLeaderTerm term = term3(W, H);
        CommitDesk desk = FastLeaderTermCommitTest.recorded(
                FastWriteLeaderTest.leader(1 << 20, 1_000));
        commitS2(term, desk, 1);
        Scripted transport = new Scripted();
        transport.replies.add(Reply.FAIL);

        assertThat(term.sendReplicas(desk, transport)).isZero();
        assertThat(desk.exposures()).isEmpty();
    }
}
