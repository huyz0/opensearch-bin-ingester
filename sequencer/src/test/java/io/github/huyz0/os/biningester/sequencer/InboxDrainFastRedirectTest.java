// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.A;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.RA;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.manager;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.SELF;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A deferred intent with a run of a stream the leader now holds in fast mode
 * (M13.34m; ADR-0081 §8): its producer was answered 202, and every drain
 * refused it -- that pod's inbox stalled for ever. The drain now writes those
 * runs' records through the leader's own fast path first, then commits the
 * rest under the intent's triple, in the pod's order.
 */
@Timeout(30)
class InboxDrainFastRedirectTest {

    private static final RunKey OTHER = new RunKey(new UUID(7, 7), 0);

    /** What the redirect was asked, and the other stream's committed next offset then. */
    private record Asked(long flushSeq, Set<RunKey> runs, long otherNext) {
    }

    private static LocalSequencer fastAtA(MemoryBinStore store) throws Exception {
        LocalSequencer sequencer = LocalSequencer.start(store, PREFIX, manager(store, "poda"), 8,
                BoundedRecoveryFixture.noRenew()).orElseThrow();
        MemoryBinStore rosters = new MemoryBinStore();
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(rosters, "p"),
                new EmptyTermCloser(rosters, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), new SimulatedClock(1_000_000L),
                        new Mono()), SELF, Map::of)
                .open(1, () -> { }).orElseThrow();
        FastLeaderTerm fast = new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(rosters, "p", 1, new Mono(), Duration.ofMillis(250)),
                        nanos -> { }),
                key -> 0L, rosters, "p", new Mono(), Duration.ofMillis(250));
        fast.catalogSays(Map.of(A, 1));
        sequencer.attach(fast);
        return sequencer;
    }

    private static CommitRequest intent(MemoryBinStore store, long seq, Map<RunKey, Integer> runs)
            throws IOException {
        CommitRequest request = new CommitRequest("podb", "i1", seq, "seg/" + seq, runs);
        Inbox.write(store, PREFIX, request);
        return request;
    }

    private static List<Asked> recording(LocalSequencer sequencer, IOException failOn,
            long failSeq) {
        List<Asked> asked = new ArrayList<>();
        InboxDrain.redirect(sequencer, (intent, runs) -> {
            asked.add(new Asked(intent.flushSeq(), Set.copyOf(runs),
                    sequencer.committedNext(OTHER)));
            if (failOn != null && intent.flushSeq() == failSeq) {
                throw failOn;
            }
        });
        return asked;
    }

    @Test
    void aFASTRunIsWrittenThroughTheRedirectTheRestCommittedUnderTheIntentsTriple()
            throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer sequencer = fastAtA(store);
        try {
            List<Asked> asked = recording(sequencer, null, -1);
            CommitRequest mixed = intent(store, 0, Map.of(RA, 3, OTHER, 5));

            assertThat(InboxDrain.drain(store, PREFIX, sequencer)).isEqualTo(1);

            assertThat(asked).containsExactly(new Asked(0, Set.of(RA), 0));
            assertThat(sequencer.committedNext(RA)).as("never on the default path").isZero();
            assertThat(sequencer.committedNext(OTHER)).isEqualTo(5);
            assertThat(sequencer.unapplied(List.of(mixed))).as("its triple applied").isEmpty();
            assertThat(Inbox.pending(store, PREFIX)).as("and the intent gone").isEmpty();
        } finally {
            sequencer.close();
        }
    }

    @Test
    void aPODsIntentsKeepTheirOrderAroundARedirectedOne() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer sequencer = fastAtA(store);
        try {
            List<Asked> asked = recording(sequencer, null, -1);
            intent(store, 0, Map.of(OTHER, 2));
            intent(store, 1, Map.of(RA, 3, OTHER, 5));
            intent(store, 2, Map.of(OTHER, 2));

            assertThat(InboxDrain.drain(store, PREFIX, sequencer)).isEqualTo(3);

            assertThat(asked).as("after flush 0, before flush 1's rest and flush 2")
                    .containsExactly(new Asked(1, Set.of(RA), 2));
            assertThat(sequencer.committedNext(OTHER)).isEqualTo(9);
        } finally {
            sequencer.close();
        }
    }

    @Test
    void aREDIRECTThatFailsLeavesItsIntentAndEveryLaterOneEarlierOnesApplied()
            throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer sequencer = fastAtA(store);
        try {
            recording(sequencer, new IOException("the fast path answered NOT_FAST"), 1);
            intent(store, 0, Map.of(OTHER, 2));
            intent(store, 1, Map.of(RA, 3, OTHER, 5));
            intent(store, 2, Map.of(OTHER, 2));

            assertThatThrownBy(() -> InboxDrain.drain(store, PREFIX, sequencer))
                    .isInstanceOf(IOException.class);

            assertThat(sequencer.committedNext(OTHER)).as("flush 0 only").isEqualTo(2);
            assertThat(Inbox.pending(store, PREFIX)).extracting(p -> p.request().flushSeq())
                    .as("flush 0 applied and deleted; 1 and 2 stay").containsExactly(1L, 2L);
        } finally {
            sequencer.close();
        }
    }

    @Test
    void anINTENTOfOnlyFastRunsCommitsNothingAndIsGone() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer sequencer = fastAtA(store);
        try {
            List<Asked> asked = recording(sequencer, null, -1);
            intent(store, 0, Map.of(RA, 3));
            long before = sequencer.chain().snapshot().deltas().size();

            assertThat(InboxDrain.drain(store, PREFIX, sequencer)).isEqualTo(1);

            assertThat(asked).containsExactly(new Asked(0, Set.of(RA), 0));
            assertThat(sequencer.chain().snapshot().deltas().size()).isEqualTo(before);
            assertThat(Inbox.pending(store, PREFIX)).isEmpty();
        } finally {
            sequencer.close();
        }
    }

    @Test
    void anINTENTTheWindowAppliedIsDeletedItsFastRunNeverWrittenAgain() throws Exception {
        // ⚠️ its review round 1, T1: a drain whose rest committed, then died
        // before its delete -- the next must not write the fast run twice.
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer sequencer = fastAtA(store);
        try {
            List<Asked> asked = recording(sequencer, null, -1);
            intent(store, 0, Map.of(RA, 3, OTHER, 5));
            sequencer.commitAll(List.of(
                    new CommitRequest("podb", "i1", 0, "seg/0", Map.of(OTHER, 5))));

            InboxDrain.drain(store, PREFIX, sequencer);

            assertThat(asked).isEmpty();
            assertThat(sequencer.committedNext(OTHER)).isEqualTo(5);
            assertThat(Inbox.pending(store, PREFIX)).isEmpty();
        } finally {
            sequencer.close();
        }
    }

    @Test
    void anINTENTWithNoFastRunIsCommittedAsBeforeInOneBatchWithItsPodsOthers()
            throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer sequencer = fastAtA(store);
        try {
            List<Asked> asked = recording(sequencer, null, -1);
            intent(store, 0, Map.of(OTHER, 2));
            intent(store, 1, Map.of(OTHER, 2));
            long before = sequencer.chain().snapshot().deltas().size();

            InboxDrain.drain(store, PREFIX, sequencer);

            assertThat(asked).isEmpty();
            assertThat(sequencer.chain().snapshot().deltas().size() - before)
                    .as("one delta for the pod's batch").isEqualTo(1);
        } finally {
            sequencer.close();
        }
    }
}
