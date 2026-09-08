// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.format.RunKey;
import binjava.format.SegmentCommit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * ADR-0036: the committed delta carries `(podId, incarnationId, flushSeq)` per
 * `SegmentCommit`, so a successor rebuilds the idempotency window from the
 * uncheckpointed tail rather than losing every commit since the predecessor's
 * last checkpoint.
 *
 * <p>⚠️ `Submission` DROPPED THE POD FIELDS before `CommitLog` saw them, so the
 * attribution could not be written even once the format carried it. That is why
 * this asserts on the delta the sequencer RETURNS, not on a hand-built one.
 */
class CommitLogAttributionTest {

    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000aa");

    private static Map<RunKey, Integer> counts(int n) {
        return Map.of(new RunKey(INDEX, 0), n);
    }

    @Test
    void aCommittedDeltaCarriesTheRequestsTriplePerSegment() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "bins/c", 1);
        {
            CommitDelta delta = log.commitAll(List.of(
                    new CommitRequest("poda", "inc-1", 0, "seg/a", counts(2)),
                    new CommitRequest("poda", "inc-1", 1, "seg/b", counts(3)),
                    new CommitRequest("podb", "inc-2", 0, "seg/c", counts(1))));

            assertThat(delta.segments()).hasSize(3);
            // ⚠️ DIFFERING flushSeq between segments of ONE delta: this is what a
            // once-per-delta field cannot reproduce. Differing podId alone would
            // survive the partial hoist.
            assertThat(delta.segments().get(0).attribution())
                    .isEqualTo(new SegmentCommit.Attribution("poda", "inc-1", 0));
            assertThat(delta.segments().get(1).attribution())
                    .as("the second segment keeps its OWN flushSeq")
                    .isEqualTo(new SegmentCommit.Attribution("poda", "inc-1", 1));
            assertThat(delta.segments().get(2).attribution())
                    .isEqualTo(new SegmentCommit.Attribution("podb", "inc-2", 0));
        }
    }

    /** And it survives the store, which is what a successor actually reads. */
    @Test
    void theAttributionSurvivesTheRoundTripThroughTheStore() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        CommitLog log = new CommitLog(store, "bins/c", 1);
        {
            log.commitAll(List.of(
                    new CommitRequest("poda", "inc-1", 7, "seg/a", counts(2))));
        }
        String key = store.list("bins/c", null, 100).objects().stream()
                .map(s -> s.key()).filter(k -> k.endsWith(".delta")).findFirst().orElseThrow();
        CommitDelta read;
        try (var in = store.get(key)) {
            read = CommitDelta.decode(in.readAllBytes());
        }
        assertThat(read.segments().get(0).attribution())
                .as("read back from the chain, not from memory")
                .isEqualTo(new SegmentCommit.Attribution("poda", "inc-1", 7));
    }
}
