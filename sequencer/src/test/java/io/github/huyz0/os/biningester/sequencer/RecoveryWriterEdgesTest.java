// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.Version;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The recovery writer's edges (M13.33g review round 1, P2, T1-T4): a commit
 * below the chain's next offset refused, a lost response that never landed
 * written again and given up after three attempts, a lost response to another
 * entry refused, the entry's runs and voids sorted by RunKey, and a sealed
 * log fenced.
 */
class RecoveryWriterEdgesTest {

    private static final RunKey S = new RunKey(new UUID(1, 1), 0);
    private static final RunKey T = new RunKey(new UUID(2, 2), 0);

    private static FastJournalRecord.Entry at(RunKey key, long first) {
        return new FastJournalRecord.Entry(7, key, first, 2, 0,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), first),
                List.of(new SegmentRecord("d" + first, OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1})));
    }

    private static StreamDecision.Decided decided(List<FastJournalRecord.Entry> commits,
            Recovery.VoidRange... voids) {
        return new StreamDecision.Decided(commits, List.of(voids), 0);
    }

    private static CommitLog opened(BinStore store) throws Exception {
        CommitLog log = new CommitLog(store, "p", 1);
        log.recover();
        log.open(0, 0);
        return log;
    }

    /** Throws on the next {@code lose} conditional PUTs, landing them only if {@code land}. */
    private static final class Losing extends ForwardingBinStore {
        final MemoryBinStore inner;
        int lose;
        boolean land;
        int attempts;

        Losing(MemoryBinStore inner) {
            super(inner);
            this.inner = inner;
        }

        @Override
        public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
            if (lose > 0) {
                lose--;
                attempts++;
                if (land) {
                    inner.putIfAbsent(key, body);
                }
                throw new IOException("the response was lost");
            }
            attempts++;
            return inner.putIfAbsent(key, body);
        }
    }

    @Test
    void aCOMMITBelowTheChainsNextOffsetIsRefusedBeforeAnything() throws Exception {
        CommitLog log = opened(new MemoryBinStore());
        log.commit("seg/x", Map.of(S, 5));
        RecoveryWriterTest.Uploads uploads = new RecoveryWriterTest.Uploads();

        assertThatThrownBy(() -> new RecoveryWriter(log, uploads).write(List.of(
                decided(List.of(at(S, 3))))))
                .as("never committed twice").isInstanceOf(IllegalStateException.class);
        assertThat(uploads.segments).isEmpty();
    }

    @Test
    void aLOSTResponseThatNeverLandedIsWrittenAgain() throws Exception {
        Losing store = new Losing(new MemoryBinStore());
        CommitLog log = opened(store);
        store.lose = 1;
        store.attempts = 0;

        new RecoveryWriter(log, new RecoveryWriterTest.Uploads()).write(List.of(
                decided(List.of(), new Recovery.VoidRange(S, 0, 10))));

        assertThat(store.attempts).isEqualTo(2);
        assertThat(log.nextOffset(S)).isEqualTo(10);
    }

    @Test
    void anAPPENDThatNeverLandsIsGivenUpAfterThreeAttempts() throws Exception {
        Losing store = new Losing(new MemoryBinStore());
        CommitLog log = opened(store);
        store.lose = 99;
        store.attempts = 0;

        assertThatThrownBy(() -> new RecoveryWriter(log, new RecoveryWriterTest.Uploads()).write(
                List.of(decided(List.of(), new Recovery.VoidRange(S, 0, 10)))))
                .isInstanceOf(IOException.class);
        assertThat(store.attempts).isEqualTo(3);
        assertThat(log.nextOffset(S)).isZero();
    }

    @Test
    void aLOSTResponseWithAnotherEntryInTheSlotIsRefused() throws Exception {
        MemoryBinStore inner = new MemoryBinStore();
        opened(inner);
        BinStore racing = new ForwardingBinStore(inner) {
            @Override
            public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
                CommitLog theirs = new CommitLog(inner, "p", 1);
                theirs.recover();
                theirs.commit("seg/x", Map.of(T, 1));
                throw new IOException("the response was lost");
            }
        };
        CommitLog log = new CommitLog(racing, "p", 1);
        log.recover();

        assertThatThrownBy(() -> new RecoveryWriter(log, new RecoveryWriterTest.Uploads()).write(
                List.of(decided(List.of(), new Recovery.VoidRange(S, 0, 10)))))
                .isInstanceOf(IllegalStateException.class);
    }

    /** A store whose first conditional PUT finds a commit of {@code counts} landed first. */
    private static BinStore takenOnce(MemoryBinStore inner, Map<RunKey, Integer> counts) {
        return new ForwardingBinStore(inner) {
            boolean taken;

            @Override
            public Optional<Version> putIfAbsent(String key, Body body) throws IOException {
                if (!taken) {
                    taken = true;
                    CommitLog theirs = new CommitLog(inner, "p", 1);
                    theirs.recover();
                    theirs.commit("seg/x", counts);
                }
                return inner.putIfAbsent(key, body);
            }
        };
    }

    @Test
    void aSLOTTakenByAnotherStreamsCommitIsFoldedInAndPassed() throws Exception {
        MemoryBinStore inner = new MemoryBinStore();
        opened(inner);
        CommitLog log = new CommitLog(takenOnce(inner, Map.of(T, 1)), "p", 1);
        log.recover();

        Recovery written = new RecoveryWriter(log, new RecoveryWriterTest.Uploads())
                .write(List.of(decided(List.of(at(S, 0)))));

        assertThat(written.sequence()).as("the slot after the landed commit").isEqualTo(2);
        assertThat(log.nextOffset(T)).as("the landed commit folded in").isEqualTo(1);
        assertThat(log.nextOffset(S)).isEqualTo(1);
    }

    @Test
    void theENTRYsRunsAndVoidsAreSortedByRunKey() throws Exception {
        CommitLog log = opened(new MemoryBinStore());

        Recovery written = new RecoveryWriter(log, new RecoveryWriterTest.Uploads()).write(
                List.of(decided(List.of(at(T, 0)), new Recovery.VoidRange(T, 1, 5)),
                        decided(List.of(at(S, 0)), new Recovery.VoidRange(S, 1, 5))));

        assertThat(written.segments().get(0).runs()).containsExactly(new RunCommit(S, 1, 0),
                new RunCommit(T, 1, 0));
        assertThat(written.voids()).extracting(Recovery.VoidRange::key).containsExactly(S, T);
    }

    @Test
    void aSEALEDLogIsFencedNotWritten() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = opened(store);
        log.seal(1, 0);

        assertThatThrownBy(() -> new RecoveryWriter(log, new RecoveryWriterTest.Uploads()).write(
                List.of(decided(List.of(), new Recovery.VoidRange(S, 0, 10)))))
                .isInstanceOf(FencedException.class);
    }
}
