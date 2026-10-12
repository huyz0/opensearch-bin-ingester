// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastLeaderTermCommitTest.POD;
import static io.github.huyz0.os.biningester.sequencer.FastLeaderTermCommitTest.header;
import static io.github.huyz0.os.biningester.sequencer.FastLeaderTermCommitTest.join;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A term's upload is paced (M13.31a; ADR-0081 §7): at most one per
 * {@code min_upload_interval} however often it is asked -- four segment PUTs
 * and four deltas a second, the rate scaling with time, never records.
 */
class FastLeaderTermUploadTest {

    private static final RunKey S1 = new RunKey(FastWriteLeaderTest.Q1, 0);

    @Test
    void anUPLOADIsMadeAtMostOncePerInterval() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = FastLeaderTermTakeoverDeskTest.term3(store, false);
        join(term);
        FastJournal journal = FastJournal.recover(new MemoryJournalFile(), 1 << 20);
        CommitDesk desk = term.commitDesk(journal, FastTermStartTest.SELF);
        term.catalogSays(Map.of(FastWriteLeaderTest.Q1, 1));
        term.flushTermRecord();
        TermJoiner.Transport unreachable = (endpoint, frame) -> {
            throw new IOException("no peer here");
        };
        Map<RunKey, Long> next = new HashMap<>(Map.of(S1, 0L));
        FastUploader.Chain chain = new FastUploader.Chain() {
            @Override
            public long next(RunKey stream) {
                return next.get(stream);
            }

            @Override
            public Map<RunKey, Long> commitAt(String key, Map<RunKey, Long> firsts,
                    Map<RunKey, Integer> counts) {
                counts.forEach((s, n) -> next.put(s, firsts.get(s) + n));
                return Map.copyOf(next);
            }
        };
        List<String> uploads = new ArrayList<>();
        assertThat(term.answerCommit(header(3, POD), commit(POD.podId(), 1, S1), desk))
                .isInstanceOf(FastWriteFrame.Assigned.class);
        term.sendExposed(desk, unreachable);
        assertThat(term.upload(desk, journal, chain, runs -> {
            uploads.add("seg");
            return "seg-1";
        }, unreachable)).hasValue(Map.of(S1, 2L));
        term.answerCommit(header(3, POD), commit(POD.podId(), 2, S1), desk);
        term.sendExposed(desk, unreachable);

        assertThat(term.upload(desk, journal, chain, runs -> {
            uploads.add("seg");
            return "seg-2";
        }, unreachable)).as("inside the interval: not due").isEmpty();
        assertThat(uploads).hasSize(1);
    }

    @Test
    void anUPLOADsSequenceIsKeptUntilItsDeltaIsCommitted() throws Exception {
        // ⚠️ its review round 1, P1 and T2: a retry folds into a landed delta.
        FastLeaderTerm term = FastLeaderTermTakeoverDeskTest.term3(new MemoryBinStore(), false);
        Map<RunKey, Long> at100 = Map.of(S1, 100L);

        long first = term.uploadSeq(at100);
        assertThat(term.uploadSeq(at100)).as("retried, not committed").isEqualTo(first);
        term.uploadCommitted(at100);

        assertThat(term.uploadSeq(at100)).as("a new upload after one committed")
                .isNotEqualTo(first);
        assertThat(term.uploadSeq(Map.of(S1, 102L))).as("other offsets, another upload")
                .isNotEqualTo(first);
    }
}
