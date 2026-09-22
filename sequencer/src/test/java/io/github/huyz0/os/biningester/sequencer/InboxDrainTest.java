// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.counts;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.deltasCarrying;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A deferred flush is applied, once, in order -- never overtaken (M8.14a,
 * ADR-0058).
 *
 * <p>⚠️ **REVIEW MEASURED THE LOSS THIS FILE EXISTS FOR**: the dedupe window
 * keeps ONE high mark per pod incarnation, so a flush N deferred to the inbox
 * and then overtaken by a forwarded N+1 is read as a replay when the drain
 * reaches it, and refused -- an acked write never committed. So a pod that has
 * deferred forwards nothing until the leaseholder confirms a drain.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class InboxDrainTest {

    private static final String A = "pod-a:9000";
    private static final String B = "pod-b:9000";

    private static LeaseConfig config(String podId, String endpoint) {
        return new LeaseConfig(PREFIX, podId, endpoint,
                Duration.ofSeconds(10), Duration.ofSeconds(3));
    }

    private static FleetSequencer pod(BinStore store, String podId, String endpoint,
            InProcessTransport transport) throws Exception {
        LeaseManager manager = new LeaseManager(store, config(podId, endpoint),
                Clock.systemUTC());
        FleetSequencer fleet = new FleetSequencer(store, config(podId, endpoint),
                transport, () -> LocalSequencer.start(store, PREFIX, manager, 8,
                        BoundedRecoveryFixture.noRenew()));
        if (fleet.leading()) {
            transport.at(endpoint, fleet);
        }
        return fleet;
    }

    private static CommitRequest flush(String pod, String incarnation, long seq) {
        return new CommitRequest(pod, incarnation, seq, "seg/" + pod + "-" + seq, counts(2));
    }

    @Test
    void aDEFERRINGPodForwardsNothingPastItsIntentsAndEVERYFlushLandsONCE() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport transport = new InProcessTransport().withInbox(store, PREFIX);
        try (FleetSequencer leader = pod(store, "poda", A, transport);
                FleetSequencer follower = pod(store, "podb", B, transport)) {
            follower.commit(flush("podb", "i1", 0));
            transport.unreachable(A);

            assertThatThrownBy(() -> follower.commit(flush("podb", "i1", 1)))
                    .isInstanceOf(CommitDeferredException.class);
            assertThatThrownBy(() -> follower.commit(flush("podb", "i1", 2)))
                    .as("still cut off: deferred again")
                    .isInstanceOf(CommitDeferredException.class);

            transport.at(A, leader);
            CommitDelta third = follower.commit(flush("podb", "i1", 3));
            assertThat(third.segments()).isNotEmpty();
        }
        for (long seq = 1; seq <= 3; seq++) {
            assertThat(deltasCarrying(store, "seg/podb-" + seq))
                    .as("⚠️ FLUSH %d IS COMMITTED EXACTLY ONCE -- 0 is the acked write "
                            + "lost to an overtaking forward, 2 is a double assignment", seq)
                    .isEqualTo(1);
        }
        assertThat(store.list(Inbox.prefixFor(PREFIX), null, 100).objects())
                .as("and the drained intents are gone").isEmpty();
    }

    @Test
    void aREFUSALIsANSWEREDNotDeferred() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        InProcessTransport transport = new InProcessTransport().withInbox(store, PREFIX);
        try (FleetSequencer leader = pod(store, "poda", A, transport);
                FleetSequencer follower = pod(store, "podb", B, transport)) {
            // ⚠️ THE LEASE STILL NAMES A, AND A's ENDPOINT ANSWERS "NOT ME": a
            // definite not-applied, which the caller retries where the lease is.
            transport.gone(A);

            assertThatThrownBy(() -> follower.commit(flush("podb", "i1", 0)))
                    .isInstanceOf(IOException.class)
                    .isNotInstanceOf(CommitDeferredException.class);
            assertThat(store.list(Inbox.prefixFor(PREFIX), null, 100).objects())
                    .as("no intent for a refusal").isEmpty();
        }
    }

    @Test
    void theKEYNamesTheINCARNATIONSoARestartedPodsFlushZeroIsANewIntent() {
        assertThat(Inbox.keyFor(PREFIX, flush("podb", "i1", 0)))
                .as("⚠️ flushSeq restarts at 0 on every start: without the incarnation a "
                        + "restarted pod's flush 0 finds its predecessor's intent and is acked "
                        + "on it")
                .isNotEqualTo(Inbox.keyFor(PREFIX, flush("podb", "i2", 0)));
    }

    @Test
    void aPODsIntentsApplyINOrderAndSTOPAtItsFirstFailure() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Inbox.write(store, PREFIX, flush("podx", "i1", 5));
        Inbox.write(store, PREFIX, flush("podx", "i1", 6));
        Inbox.write(store, PREFIX, flush("pody", "i1", 1));
        List<Long> seen = new java.util.ArrayList<>();
        Sequencer failsOnFive = new Sequencer() {
            @Override
            public CommitDelta commitAll(List<CommitRequest> requests) throws IOException {
                CommitRequest r = requests.get(0);
                seen.add(r.flushSeq());
                if (r.podId().equals("podx") && r.flushSeq() == 5) {
                    throw new IOException("the store hiccuped");
                }
                return new CommitDelta(1, r.segmentKey(), List.of(new io.github.huyz0.os.biningester.format.RunCommit(
                        r.recordCounts().keySet().iterator().next(), 2, 0)));
            }

            @Override
            public void close() {
            }
        };

        assertThatThrownBy(() -> InboxDrain.drain(store, PREFIX, failsOnFive))
                .as("⚠️ NOT EVERY INTENT APPLIED: the pod that asked stays deferring")
                .isInstanceOf(IOException.class);

        assertThat(seen).as("podx's 6 is NEVER tried after its 5 failed; pody is unaffected")
                .containsExactly(5L, 1L);
        assertThat(store.list(Inbox.prefixFor(PREFIX), null, 100).objects())
                .extracting(o -> o.key().substring(o.key().lastIndexOf('/') + 1))
                .as("podx's two stay; pody's applied one is deleted")
                .hasSize(2);
    }

    @Test
    void aPODsIntentsAreCommittedInOneBatchWhilePODOrderStaysIntact() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Inbox.write(store, PREFIX, flush("podx", "i1", 5));
        Inbox.write(store, PREFIX, flush("podx", "i1", 6));
        Inbox.write(store, PREFIX, flush("pody", "i1", 1));
        List<List<Long>> batches = new java.util.ArrayList<>();
        Sequencer recordsBatches = new Sequencer() {
            @Override
            public CommitDelta commitAll(List<CommitRequest> requests) {
                batches.add(requests.stream().map(CommitRequest::flushSeq).toList());
                CommitRequest first = requests.getFirst();
                return new CommitDelta(1, first.segmentKey(), List.of(
                        new io.github.huyz0.os.biningester.format.RunCommit(
                                first.recordCounts().keySet().iterator().next(), 2, 0)));
            }

            @Override
            public void close() {
            }
        };

        InboxDrain.drain(store, PREFIX, recordsBatches);

        assertThat(batches)
                .as("each pod's ordered intents share one durable delta, so drain cost is "
                        + "bounded by pods rather than intents")
                .containsExactly(List.of(5L, 6L), List.of(1L));
    }
}
