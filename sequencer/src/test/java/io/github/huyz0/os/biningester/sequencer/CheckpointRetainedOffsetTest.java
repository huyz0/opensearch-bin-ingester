// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Checkpoint;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;

/**
 * The checkpoint records what GC actually deleted (M7.10, FR-9).
 *
 * <p>⚠️ {@code CheckpointWriter} WROTE A LITERAL 0 UNTIL THIS COMMIT, and its
 * own comment said that was truthful only until M7 gave retention a meaning.
 * Leaving it there leaves every other case in this milestone green while the
 * refusal a consumer below the boundary should meet (M7.16) has no boundary to
 * be below.
 */
class CheckpointRetainedOffsetTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final UUID B = UUID.fromString("00000000-0000-0000-0000-0000000000bb");
    private static final RunKey RA = new RunKey(A, 0);
    private static final RunKey RB = new RunKey(B, 1);
    private static final Duration T = Duration.ofSeconds(30);

    private static CheckpointWriter.Ticker frozen() {
        return () -> new CountDownLatch(1).await();
    }

    private static long commit(CommitLog log, CheckpointWriter writer, Map<RunKey, Integer> what)
            throws IOException {
        List<CommitRequest> requests = List.of(
                new CommitRequest("poda", "i1", 1, "seg/poda/1", what));
        long applied = log.commitAll(requests).sequence();
        writer.observe(requests, applied);
        return applied;
    }

    private static Checkpoint lastWritten(RecordingBinStore store) throws IOException {
        return Checkpoint.decode(store.lastCheckpointBody()
                .orElseThrow(() -> new AssertionError("no checkpoint was ever written")));
    }

    @Test
    void aCheckpointCarriesWhatGCReportedRetained() throws Exception {
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "bins", 1);
        log.open(0, 0);
        try (CheckpointWriter writer = new CheckpointWriter(store, log, "bins", 1, T, frozen())) {
            writer.observeRetained(Map.of(RA, 3L, RB, 4L));
            commit(log, writer, Map.of(RA, 9, RB, 9));
            Checkpoint written = lastWritten(store);
            assertThat(written.streams().get(RA).oldestRetainedOffset())
                    .as("⚠️ THE MUTATION THAT SURVIVES EVERYTHING ELSE IN M7: leave the "
                            + "literal 0 in place and every other case stays green while "
                            + "the boundary a consumer is refused below never moves")
                    .isEqualTo(3);
            assertThat(written.streams().get(RB).oldestRetainedOffset()).isEqualTo(4);
        }
    }

    @Test
    void aStreamGCSaidNOTHINGAboutKeepsZERO() throws Exception {
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "bins", 1);
        log.open(0, 0);
        try (CheckpointWriter writer = new CheckpointWriter(store, log, "bins", 1, T, frozen())) {
            writer.observeRetained(Map.of(RA, 3L));
            commit(log, writer, Map.of(RA, 9, RB, 9));
            assertThat(lastWritten(store).streams().get(RB).oldestRetainedOffset())
                    .as("a stream no pass has reported on retains everything it ever had, "
                            + "which is what 0 means -- inventing a boundary for it would "
                            + "refuse a consumer for data still in the bucket")
                    .isZero();
        }
    }

    @Test
    void aWriterNOBODYToldRetainsEverything() throws Exception {
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "bins", 1);
        log.open(0, 0);
        try (CheckpointWriter writer = new CheckpointWriter(store, log, "bins", 1, T, frozen())) {
            commit(log, writer, Map.of(RA, 9));
            assertThat(lastWritten(store).streams().get(RA).oldestRetainedOffset())
                    .as("before GC runs at all, 0 is the truthful answer -- and it must "
                            + "stay the answer, because a deployment with no GC role must "
                            + "not have its consumers refused")
                    .isZero();
        }
    }

    @Test
    void aBoundaryThatArrivesOUTOFOrderDoesNotWALKBackwards() throws Exception {
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "bins", 1);
        log.open(0, 0);
        try (CheckpointWriter writer = new CheckpointWriter(store, log, "bins", 1, T, frozen())) {
            writer.observeRetained(Map.of(RA, 9L));
            writer.observeRetained(Map.of(RA, 3L));
            commit(log, writer, Map.of(RA, 20));
            assertThat(lastWritten(store).streams().get(RA).oldestRetainedOffset())
                    .as("⚠️ THE CASE THAT SEPARATES `max` FROM LAST-WRITE-WINS, and review "
                            + "found the pair indistinguishable while both reports rose. "
                            + "Two passes can report out of order once a retry is involved, "
                            + "and a boundary that walked BACKWARDS would tell consumers "
                            + "that deleted records are readable again")
                    .isEqualTo(9);
        }
    }

    @Test
    void aHIGHERBoundaryFromALaterPassMovesItForward() throws Exception {
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "bins", 1);
        log.open(0, 0);
        try (CheckpointWriter writer = new CheckpointWriter(store, log, "bins", 1, T, frozen())) {
            writer.observeRetained(Map.of(RA, 3L));
            writer.observeRetained(Map.of(RA, 9L));
            commit(log, writer, Map.of(RA, 20));
            assertThat(lastWritten(store).streams().get(RA).oldestRetainedOffset())
                    .as("retention moves FORWARD -- a writer that kept the first value "
                            + "reports a boundary hours stale and lets a consumer seek "
                            + "into deleted data")
                    .isEqualTo(9);
        }
    }

    @Test
    void aBoundaryPASTTheNextOffsetIsCLAMPEDRatherThanThrown() throws Exception {
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "bins", 1);
        log.open(0, 0);
        try (CheckpointWriter writer = new CheckpointWriter(store, log, "bins", 1, T, frozen())) {
            writer.observeRetained(Map.of(RA, 9_999L));
            commit(log, writer, Map.of(RA, 5));
            assertThat(lastWritten(store).streams().get(RA).oldestRetainedOffset())
                    .as("⚠️ `StreamOffsets` REFUSES oldestRetainedOffset > nextOffset, and "
                            + "this runs ON THE COMMIT THREAD -- a throw here takes the "
                            + "sequencer down over a stale GC report. Clamping to "
                            + "nextOffset says the stream is EMPTY -- the STRONGEST "
                            + "boundary there is, refusing every consumer of it until it "
                            + "commits again -- which is deliberate: a report that arrived "
                            + "after the stream was truncated still says those records "
                            + "are gone")
                    .isEqualTo(5);
        }
    }

    @Test
    void aNEGATIVEBoundaryIsIGNORED() throws Exception {
        RecordingBinStore store = new RecordingBinStore(new MemoryBinStore());
        CommitLog log = new CommitLog(store, "bins", 1);
        log.open(0, 0);
        try (CheckpointWriter writer = new CheckpointWriter(store, log, "bins", 1, T, frozen())) {
            writer.observeRetained(Map.of(RA, -1L));
            commit(log, writer, Map.of(RA, 5));
            assertThat(lastWritten(store).streams().get(RA).oldestRetainedOffset())
                    .as("a negative boundary is a bug upstream, and the commit thread is "
                            + "not where it should be discovered")
                    .isZero();
        }
    }
}
