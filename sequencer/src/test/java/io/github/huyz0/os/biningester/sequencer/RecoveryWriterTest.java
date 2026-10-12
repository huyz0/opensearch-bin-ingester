// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.Version;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.ChainEntry;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A takeover's decided streams written as ONE recovery entry (M13.33g;
 * ADR-0081 §5 step 6, invariant c): their entries uploaded as segments at the
 * offsets they were assigned, every void beside them, in one chain append.
 */
class RecoveryWriterTest {

    private static final RunKey S = new RunKey(new UUID(1, 1), 0);
    private static final RunKey T = new RunKey(new UUID(2, 2), 0);

    private static FastJournalRecord.Entry at(RunKey key, long first, int count) {
        List<SegmentRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(new SegmentRecord(key.partitionId() + "d" + first + "-" + i,
                    OpType.INDEX, OptionalLong.empty(), new byte[] {1}));
        }
        return new FastJournalRecord.Entry(7, key, first, 2, 0,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), first), records);
    }

    /** Records what each uploaded segment held, naming them seg/0, seg/1... */
    static final class Uploads implements RecoveryWriter.SegmentUpload {
        final List<Map<RunKey, List<SegmentRecord>>> segments = new ArrayList<>();

        @Override
        public String upload(Map<RunKey, List<SegmentRecord>> runs) {
            segments.add(runs);
            return "seg/" + (segments.size() - 1);
        }
    }

    private static CommitLog opened(BinStore store) throws Exception {
        CommitLog log = new CommitLog(store, "p", 1);
        log.recover();
        log.open(0, 0);
        return log;
    }

    private static StreamDecision.Decided decided(List<FastJournalRecord.Entry> commits,
            Recovery.VoidRange... voids) {
        return new StreamDecision.Decided(commits, List.of(voids), 0);
    }

    @Test
    void aSTREAMsContiguousCommitsAndItsVoidLandInOneRecoveryEntry() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = opened(store);
        Uploads uploads = new Uploads();

        Recovery written = new RecoveryWriter(log, uploads).write(List.of(decided(
                List.of(at(S, 0, 2), at(S, 2, 1)), new Recovery.VoidRange(S, 3, 10))));

        assertThat(uploads.segments).hasSize(1);
        assertThat(uploads.segments.get(0).get(S)).hasSize(3);
        assertThat(written.segments()).singleElement().satisfies(seg ->
                assertThat(seg.runs()).containsExactly(new RunCommit(S, 3, 0)));
        assertThat(written.voids()).containsExactly(new Recovery.VoidRange(S, 3, 10));
        assertThat(log.nextOffset(S)).as("the void moves the stream").isEqualTo(10);
        assertThat(log.chain().snapshot().voids()).hasSize(1);
    }

    @Test
    void aSTREAMCommittedAroundAVoidTakesASegmentPerRun() throws Exception {
        CommitLog log = opened(new MemoryBinStore());
        Uploads uploads = new Uploads();

        Recovery written = new RecoveryWriter(log, uploads).write(List.of(decided(
                List.of(at(S, 0, 1), at(S, 5, 1)), new Recovery.VoidRange(S, 1, 5),
                new Recovery.VoidRange(S, 6, 10))));

        assertThat(written.segments()).hasSize(2);
        assertThat(written.segments().get(0).runs()).containsExactly(new RunCommit(S, 1, 0));
        assertThat(written.segments().get(1).runs()).containsExactly(new RunCommit(S, 1, 5));
    }

    @Test
    void twoSTREAMsShareASegment() throws Exception {
        CommitLog log = opened(new MemoryBinStore());
        Uploads uploads = new Uploads();

        Recovery written = new RecoveryWriter(log, uploads).write(List.of(
                decided(List.of(at(S, 0, 1))), decided(List.of(at(T, 0, 2)))));

        assertThat(uploads.segments).hasSize(1);
        assertThat(written.segments().get(0).runs())
                .containsExactlyInAnyOrder(new RunCommit(S, 1, 0), new RunCommit(T, 2, 0));
        assertThat(log.nextOffset(T)).isEqualTo(2);
    }

    @Test
    void voidsALONEAreAnEntryWithNoSegment() throws Exception {
        CommitLog log = opened(new MemoryBinStore());
        Uploads uploads = new Uploads();

        Recovery written = new RecoveryWriter(log, uploads).write(List.of(
                decided(List.of(), new Recovery.VoidRange(S, 0, 10))));

        assertThat(uploads.segments).isEmpty();
        assertThat(written.segments()).isEmpty();
        assertThat(log.nextOffset(S)).isEqualTo(10);
    }

    @Test
    void anOFFSETTheChainAlreadyCommittedIsRefusedBeforeAnything() throws Exception {
        CommitLog log = opened(new MemoryBinStore());
        log.commit("seg/x", Map.of(S, 5));
        Uploads uploads = new Uploads();

        assertThatThrownBy(() -> new RecoveryWriter(log, uploads).write(List.of(
                decided(List.of(), new Recovery.VoidRange(S, 3, 10)))))
                .as("a committed offset is never voided")
                .isInstanceOf(IllegalStateException.class);
        assertThat(uploads.segments).as("nothing uploaded").isEmpty();
        assertThat(log.nextOffset(S)).isEqualTo(5);
    }

    @Test
    void aSLOTTakenByACommitOfTheStreamsOffsetsIsRefused() throws Exception {
        // ⚠️ ANOTHER STREAM'S COMMIT IN THE SLOT IS PASSED (M13.33o review
        // round 2, T3; RecoveryWriterEdgesTest); one that committed an offset
        // the entry voids is refused, wherever the entry would go next.
        MemoryBinStore store = new MemoryBinStore();
        CommitLog mine = opened(store);
        CommitLog theirs = new CommitLog(store, "p", 1);
        theirs.recover();
        theirs.commit("seg/x", Map.of(S, 1));

        assertThatThrownBy(() -> new RecoveryWriter(mine, new Uploads()).write(List.of(
                decided(List.of(), new Recovery.VoidRange(S, 0, 10)))))
                .as("a committed offset is never voided")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void aLOSTResponseOfAnAppendThatLandedIsReconciledAsWritten() throws Exception {
        MemoryBinStore inner = new MemoryBinStore();
        boolean[] armed = {false};
        BinStore losing = new ForwardingBinStore(inner) {
            @Override
            public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
                Optional<Version> written = inner.putIfAbsent(key, body);
                if (armed[0]) {
                    armed[0] = false;
                    throw new IOException("the response was lost");
                }
                return written;
            }
        };
        CommitLog log = opened(losing);
        armed[0] = true;

        Recovery written = new RecoveryWriter(log, new Uploads()).write(List.of(
                decided(List.of(), new Recovery.VoidRange(S, 0, 10))));

        ChainEntry landed = ChainEntry.decode(inner.get(log.keyFor(written.sequence()))
                .readAllBytes());
        assertThat(landed).isEqualTo(written);
        assertThat(log.nextOffset(S)).isEqualTo(10);
    }
}
