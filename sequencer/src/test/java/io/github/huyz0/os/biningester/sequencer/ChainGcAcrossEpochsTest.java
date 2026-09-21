// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Checkpoint;
import io.github.huyz0.os.biningester.format.Checkpoint.PodState;
import io.github.huyz0.os.biningester.format.Checkpoint.StreamOffsets;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Chain GC judges "below the checkpoint" in (epoch, sequence), not in sequence
 * alone (M8.39).
 *
 * <p>⚠️ **A CHAIN HELD ACROSS A TAKEOVER HOLDS TWO NUMBERING SPACES.** A
 * predecessor's deltas run to high sequences and a young successor's
 * checkpoint sits at a low one, so a sequence-only test kept every one of the
 * predecessor's deltas for ever -- and would collect a NEWER epoch's delta
 * that no checkpoint covers.
 */
class ChainGcAcrossEpochsTest {

    private static final String PREFIX = "bucket";
    private static final RunKey STREAM = new RunKey(UUID.randomUUID(), 0);

    private final MemoryBinStore store = new MemoryBinStore();

    private ChainGc.DeltaAt delta(long epoch, long sequence) throws Exception {
        CommitDelta delta = new CommitDelta(sequence, List.of(new SegmentCommit(
                "bucket/data/seg-" + epoch + "-" + sequence,
                List.of(new RunCommit(STREAM, 1, sequence)),
                new SegmentCommit.Attribution("poda", "i1", sequence))));
        store.put(new LogKeys(PREFIX, epoch).keyFor(sequence), Body.ofBytes(delta.encode()));
        return new ChainGc.DeltaAt(epoch, sequence, delta);
    }

    private static Checkpoint newest(long sequence) {
        return new Checkpoint(sequence, Map.of(STREAM, new StreamOffsets(100, 0)),
                Map.of("poda", PodState.bare(42)));
    }

    @Test
    void aPREDECESSORsDeltaBelowTheSuccessorsCheckpointIsCOLLECTED() throws Exception {
        ChainGc.DeltaAt old = delta(3, 40);

        ChainGc.Result result = new ChainGc(store, PREFIX, 1000, 2).collect(newest(2),
                new ChainGc.CheckpointAt(4, 2), List.of(old), Set.of(), List.of());

        assertThat(result.deltasDeleted())
                .as("⚠️ EPOCH 3 IS BEFORE EPOCH 4 whatever the sequences say")
                .isEqualTo(1);
        assertThat(store.stat(new LogKeys(PREFIX, 3).keyFor(40))).isEmpty();
    }

    @Test
    void aNEWEREpochsDeltaIsNOTCoveredByAnOlderCheckpoint() throws Exception {
        ChainGc.DeltaAt young = delta(4, 1);

        ChainGc.Result result = new ChainGc(store, PREFIX, 1000, 2).collect(newest(9),
                new ChainGc.CheckpointAt(3, 9), List.of(young), Set.of(), List.of());

        assertThat(result.deltasDeleted())
                .as("⚠️ NO CHECKPOINT COVERS IT: collecting it drops its offsets from "
                        + "every recovery (I2)")
                .isZero();
        assertThat(store.stat(new LogKeys(PREFIX, 4).keyFor(1))).isPresent();
    }
}
