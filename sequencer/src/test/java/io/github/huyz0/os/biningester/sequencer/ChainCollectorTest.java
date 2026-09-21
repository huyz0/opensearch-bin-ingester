// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.StoreCounts;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunKey;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

/**
 * Chain GC for the held term, wired from memory (M8.39, FR-9, M7.24).
 *
 * <p>⚠️ **THE COST IS HALF THE ROW.** Every other source of "the newest
 * checkpoint and every checkpoint" is a read, which in the retention loop is a
 * request per pass on an idle leader. So each case counts GETs and LISTs.
 */
class ChainCollectorTest {

    private static final String PREFIX = "chain-gc/";
    private static final RunKey STREAM =
            new RunKey(UUID.fromString("00000000-0000-0000-0000-0000000000aa"), 0);
    private static final CheckpointWriter.Ticker FROZEN = () -> new CountDownLatch(1).await();

    private final MemoryBinStore backing = new MemoryBinStore();
    private final CountingBinStore store = new CountingBinStore(backing);

    private LeaseManager manager(String pod) {
        return new LeaseManager(store, new LeaseConfig(PREFIX, pod, "",
                Duration.ofSeconds(10), Duration.ofSeconds(3)), Clock.systemUTC());
    }

    /** K = 1, so every commit is followed by a checkpoint covering it. */
    private LocalSequencer term(String pod) throws Exception {
        return LocalSequencer.start(store, PREFIX, manager(pod), 8,
                LocalSequencer.sleepFor(Duration.ofSeconds(3)), 1, FROZEN).orElseThrow();
    }

    private static CommitRequest flush(long flushSeq) {
        return new CommitRequest("poda", "i1", flushSeq, "seg/" + flushSeq, Map.of(STREAM, 1));
    }

    private boolean present(long epoch, long sequence) throws Exception {
        return backing.stat(new LogKeys(PREFIX, epoch).keyFor(sequence)).isPresent();
    }

    @Test
    void deltasWhoseSegmentsAreGONEAreCollectedWithNoREAD() throws Exception {
        LocalSequencer term = term("poda");
        try {
            List<CommitDelta> committed = new ArrayList<>();
            for (long f = 0; f < 3; f++) {
                committed.add(term.commit(flush(f)));
            }
            term.chain().forgetCollected(Set.of("seg/0", "seg/1", "seg/2"));

            StoreCounts before = store.counts();
            ChainGc.Result result = new ChainCollector(term, PREFIX).collect(store, 1000);
            StoreCounts after = store.counts();

            assertThat(result.deltasDeleted())
                    .as("the two older deltas: gone segments, below the checkpoint, unpinned")
                    .isEqualTo(2);
            assertThat(result.pinnedByPointer())
                    .as("⚠️ THE NEWEST IS poda's POINTER, so it stays").isEqualTo(1);
            assertThat(present(term.epoch(), committed.get(0).sequence())).isFalse();
            assertThat(present(term.epoch(), committed.get(1).sequence())).isFalse();
            assertThat(present(term.epoch(), committed.get(2).sequence())).isTrue();
            assertThat(result.checkpointsDeleted())
                    .as("three checkpoints written, the newest %d kept",
                            ChainCollector.KEEP_CHECKPOINTS)
                    .isEqualTo(1);
            assertThat(after.gets() - before.gets()).as("⚠️ NO GET").isZero();
            assertThat(after.lists() - before.lists()).as("⚠️ NO LIST").isZero();
            assertThat(term.chain().awaitingChainGc())
                    .as("and what it collected is forgotten; what it kept waits")
                    .hasSize(1);
        } finally {
            term.close();
        }
    }

    @Test
    void thePINNEDDeltaStillANSWERSARetryAcrossATakeover() throws Exception {
        // ⚠️ M7.24's REPLAY HALF: the delta a pod's pointer names is READ to
        // answer that pod's retry. The test M7 wrote asserted only that the
        // object EXISTS; this one asks it to answer.
        LocalSequencer term = term("poda");
        CommitDelta last;
        try {
            term.commit(flush(0));
            term.commit(flush(1));
            last = term.commit(flush(2));
            term.chain().forgetCollected(Set.of("seg/0", "seg/1", "seg/2"));
            new ChainCollector(term, PREFIX).collect(store, 1000);
        } finally {
            term.close();
        }

        LocalSequencer successor = term("podb");
        try {
            CommitDelta answered = successor.commit(flush(2));

            assertThat(answered.allRuns().get(0).firstOffset())
                    .as("⚠️ THE RETRY IS ANSWERED WITH THE OFFSETS IT WAS FIRST GIVEN, "
                            + "read from the delta chain GC kept")
                    .isEqualTo(last.allRuns().get(0).firstOffset());
        } finally {
            successor.close();
        }
    }

    @Test
    void anIDLETermIssuesNOREQUEST() throws Exception {
        LocalSequencer term = term("poda");
        try {
            term.commit(flush(0));
            StoreCounts before = store.counts();

            ChainGc.Result result = new ChainCollector(term, PREFIX).collect(store, 1000);

            assertThat(result.deltasDeleted()).isZero();
            assertThat(store.counts().total() - before.total())
                    .as("⚠️ NOTHING AWAITING AND NO SURPLUS CHECKPOINT: zero requests, "
                            + "which is M8's criterion 4 on an idle leader")
                    .isZero();
        } finally {
            term.close();
        }
    }

    @Test
    void aDeltaNOCheckpointCoversSTAYS() throws Exception {
        // ⚠️ K = 2, so the third commit is NEWER than the newest checkpoint.
        // Collecting it drops its offsets from every recovery (I2), and a case
        // whose every delta sat below a checkpoint could not see that.
        LocalSequencer term = LocalSequencer.start(store, PREFIX, manager("poda"), 8,
                LocalSequencer.sleepFor(Duration.ofSeconds(3)), 2, FROZEN).orElseThrow();
        try {
            term.commit(flush(0));
            term.commit(flush(1));
            CommitDelta uncovered = term.commit(flush(2));
            term.chain().forgetCollected(Set.of("seg/0", "seg/1", "seg/2"));

            new ChainCollector(term, PREFIX).collect(store, 1000);

            assertThat(present(term.epoch(), uncovered.sequence()))
                    .as("⚠️ NO CHECKPOINT COVERS IT, so it stays").isTrue();
            assertThat(term.chain().awaitingChainGc().stream()
                    .map(ChainGc.DeltaAt::sequence))
                    .as("and it waits to be judged again").contains(uncovered.sequence());
        } finally {
            term.close();
        }
    }

    @Test
    void aSECONDPassDoesNotDELETEWhatTheFirstCollected() throws Exception {
        // ⚠️ WHAT WAS COLLECTED IS FORGOTTEN. A writer that kept every
        // checkpoint it ever wrote would re-send a DELETE for each on every
        // pass: a request rate growing with the term's age.
        LocalSequencer term = term("poda");
        try {
            for (long f = 0; f < 3; f++) {
                term.commit(flush(f));
            }
            term.chain().forgetCollected(Set.of("seg/0", "seg/1", "seg/2"));
            ChainCollector chainGc = new ChainCollector(term, PREFIX);
            assertThat(chainGc.collect(store, 1000).checkpointsDeleted())
                    .as("the premise: the first pass collected a checkpoint").isEqualTo(1);

            StoreCounts before = store.counts();
            ChainGc.Result second = chainGc.collect(store, 1000);

            assertThat(second.deltasDeleted() + second.checkpointsDeleted()).isZero();
            assertThat(store.counts().total() - before.total())
                    .as("⚠️ ZERO REQUESTS: nothing collected is sent again").isZero();
        } finally {
            term.close();
        }
    }
}
