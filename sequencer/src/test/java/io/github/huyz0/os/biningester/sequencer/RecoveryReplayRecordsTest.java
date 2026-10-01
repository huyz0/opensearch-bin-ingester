// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A recovery's commits are RECORDED as a delta's, not only folded (M13.25
 * review round 1, T1, T2, T4).
 *
 * <p>⚠️ OFFSETS ALONE CANNOT TELL. A replay or a live commit log that folded
 * a recovery's offsets and dropped its commits would assign every next offset
 * correctly and never deliver the recovered records: chain memory feeds the
 * subscription hub, and the per-stream entry counts feed compaction.
 */
class RecoveryReplayRecordsTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);

    private static Recovery recovery(long sequence, long first) {
        return new Recovery(sequence,
                List.of(new SegmentCommit("seg/recovered-" + sequence,
                        List.of(new RunCommit(A, 2, first)))),
                List.of(new Recovery.VoidRange(A, first + 2, first + 50)));
    }

    private static void put(MemoryBinStore store, long sequence, byte[] bytes) throws Exception {
        store.putIfAbsent(new LogKeys("p", 0).keyFor(sequence), Body.ofBytes(bytes));
    }

    private static List<Long> remembered(CommitLog log) {
        return log.chain().snapshot().deltas().stream().map(CommitDelta::sequence).toList();
    }

    @Test
    void aRECOVEREDLogRemembersTheRecoverysCommitsAsADelta() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        new CommitLog(store, "p", 0).commit("seg-0", Map.of(A, 3));
        put(store, 1, recovery(1, 3).encode());

        CommitLog log = new CommitLog(store, "p", 0);
        log.recover();

        assertThat(remembered(log)).as("the recovery's commits are delivered from memory")
                .contains(1L);
    }

    @Test
    void aLIVELogThatLosesARaceToARecoveryRemembersItsCommits() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "p", 0);
        log.commit("seg-0", Map.of(A, 3));
        put(store, 1, recovery(1, 3).encode());

        log.commit("seg-2", Map.of(A, 1));

        assertThat(remembered(log)).containsExactly(0L, 1L, 2L);
    }

    @Test
    void aREPLAYCountsARecoverysRunsAndListsItsDelta() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        new CommitLog(store, "p", 0).commit("seg-0", Map.of(A, 3));
        put(store, 1, recovery(1, 3).encode());

        ChainReplay.Result result = ChainReplay.replay(store, "p", 0);

        assertThat(result.indexEntries())
                .as("one index entry per run: the delta's and the recovery's")
                .containsEntry(A, 2L);
        assertThat(result.deltas()).extracting(d -> d.delta().sequence()).contains(1L);
    }

    @Test
    void BACKFILLReturnsARecoverysCommits() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        put(store, 0, recovery(0, 0).encode());
        new CommitLog(store, "p", 0).commit("seg-1", Map.of(A, 1));

        assertThat(ChainBackfill.below(store, "p", 0, 2))
                .extracting(at -> at.sequence())
                .as("a recovered segment is a committed one chain GC and the orphan sweep "
                        + "must know about")
                .contains(0L);
    }

    @Test
    void theSIMULATIONCheckerTakesARecoveryAsAContinuationOfItsStream() throws Exception {
        MemoryBinStore valid = new MemoryBinStore();
        new CommitLog(valid, "p", 0).commit("seg-0", Map.of(A, 3));
        put(valid, 1, recovery(1, 3).encode());
        assertThat(Invariants.checkChain(valid, "p", 0))
                .extracting(Invariants.Violation::invariant)
                .as("runs then a void continuing stream A from its high-water mark: neither a "
                        + "rewind nor a gap (the fixture writes no CONTINUE, so `link` is not "
                        + "this case's concern)")
                .doesNotContain("I2", "gap");

        MemoryBinStore voidingCommitted = new MemoryBinStore();
        new CommitLog(voidingCommitted, "p", 0).commit("seg-0", Map.of(A, 3));
        put(voidingCommitted, 1, new Recovery(1, List.of(),
                List.of(new Recovery.VoidRange(A, 2, 40))).encode());
        assertThat(Invariants.checkChain(voidingCommitted, "p", 0))
                .extracting(Invariants.Violation::invariant)
                .as("a void over committed offset 2 is I2: an offset given other content")
                .contains("I2");
    }
}
