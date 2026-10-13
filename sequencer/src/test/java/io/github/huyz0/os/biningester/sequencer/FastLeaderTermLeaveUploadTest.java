// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.SELF;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.read;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.Q1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * A leader shutting down uploads and commits every entry it exposed BEFORE it
 * leaves (ADR-0081 §9; M13.35c): its last upload, unpaced, and only then
 * marked departed -- or, still holding one uncommitted, undeparted as a crash.
 */
class FastLeaderTermLeaveUploadTest {

    /** A term whose desk exposed one entry of S1, at {@code wal_quorum = 1}. */
    private record Exposed(FastLeaderTerm term, CommitDesk desk, FastJournal journal) {
    }

    private static Exposed exposed(MemoryBinStore store) throws Exception {
        FastLeaderTerm term = FastLeaderTermCommitTest.term3(store);
        FastLeaderTermCommitTest.join(term);
        term.catalogSays(Map.of(Q1, 1));
        term.flushTermRecord();
        FastJournal journal = FastJournal.recover(new MemoryJournalFile(), 1 << 20);
        CommitDesk desk = term.commitDesk(journal, SELF);
        term.answerCommit(FastLeaderTermCommitTest.header(3, FastLeaderTermCommitTest.POD),
                commit(FastLeaderTermCommitTest.POD.podId(), 1, S1), desk);
        assertThat(desk.exposures()).hasSize(1);
        return new Exposed(term, desk, journal);
    }

    @Test
    void aLEADERUploadsWhatItExposedAndThenLeaves() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Exposed e = exposed(store);

        FastLeaderTerm.Left left = e.term().leave(() -> e.desk().leader().committed(S1, 2),
                Optional.of(e.journal()));

        assertThat(left).isEqualTo(FastLeaderTerm.Left.DEPARTED_AND_CLOSED);
        assertThat(read(store, 3).leaderDeparted()).isTrue();
    }

    @Test
    void aLEADERWhoseLastUploadFailsStaysUndeparted() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Exposed e = exposed(store);

        FastLeaderTerm.Left left = e.term().leave(() -> {
            throw new IOException("the store is down");
        }, Optional.of(e.journal()));

        assertThat(left).isEqualTo(FastLeaderTerm.Left.STAYED);
        assertThat(read(store, 3).leaderDeparted()).isFalse();
    }

    @Test
    void aLEADERWithNothingExposedMakesNoLastUpload() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = FastLeaderTermCommitTest.term3(store);
        FastJournal journal = FastJournal.recover(new MemoryJournalFile(), 1 << 20);
        term.commitDesk(journal, SELF);
        AtomicInteger uploads = new AtomicInteger();

        assertThat(term.leave(uploads::incrementAndGet, Optional.of(journal)))
                .isEqualTo(FastLeaderTerm.Left.DEPARTED_AND_CLOSED);
        assertThat(uploads).hasValue(0);
    }

    @Test
    void anUNPACEDUploadIsMadeInsideTheIntervalAPacedOneWaitsOut() throws Exception {
        Exposed e = exposed(new MemoryBinStore());
        AtomicInteger puts = new AtomicInteger();
        RecoveryWriter.SegmentUpload failing = runs -> {
            puts.incrementAndGet();
            throw new IOException("the segment PUT failed");
        };
        FastUploader.Chain chain = new FastUploader.Chain() {
            @Override
            public long next(RunKey stream) {
                return 0;
            }

            @Override
            public Map<RunKey, Long> commitAt(String segmentKey, Map<RunKey, Long> firsts,
                    Map<RunKey, Integer> counts) throws IOException {
                throw new IOException("never reached");
            }
        };
        TermJoiner.Transport none = (endpoint, frame) -> {
            throw new IOException("nothing is sent");
        };
        try {
            e.term().upload(e.desk(), e.journal(), chain, failing, none);
        } catch (IOException failed) {
            // the entry stays exposed, for the next upload
        }
        assertThat(puts).as("the first upload put its segment").hasValue(1);

        assertThat(e.term().upload(e.desk(), e.journal(), chain, failing, none))
                .as("paced: inside min_upload_interval").isEmpty();
        assertThat(puts).hasValue(1);
        try {
            e.term().upload(e.desk(), e.journal(), chain, failing, none, false);
        } catch (IOException failed) {
            // as above
        }
        assertThat(puts).as("the unpaced one is made").hasValue(2);
    }
}
