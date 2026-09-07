// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.format.Checkpoint.PodState;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * ADR-0036: `pods` becomes `podId -> (incarnationId, lastAppliedFlushSeq,
 * epoch, sequence)` — the watermark AND a pointer to the delta that last
 * applied for that pod.
 *
 * <p>⚠️ WITHOUT THE POINTER, detection is unbounded-depth and ANSWERING is
 * bounded to the uncheckpointed tail, because entries below the newest
 * checkpoint are never GET. That is the ground on which "bound the window to
 * the tail" is rejected, so a slot carrying only a watermark shares the defect
 * it rejects.
 *
 * <p>⚠️ AND THE INCARNATION IS A VALUE BESIDE THE WATERMARK, never a qualifier
 * on the pod id: qualifying the id adds a `pods` entry per restart, so the
 * checkpoint grows with history against ADR-0033's size argument.
 */
class CheckpointPodStateTest {

    private static final UUID INDEX = UUID.fromString("11112222-3333-4444-5555-666677778888");

    private static Map<RunKey, Checkpoint.StreamOffsets> streams() {
        return Map.of(new RunKey(INDEX, 1), new Checkpoint.StreamOffsets(1001, 100));
    }

    @Test
    void aPodSlotCarriesTheIncarnationTheWatermarkAndThePointer() throws Exception {
        Checkpoint original = new Checkpoint(9, streams(),
                Map.of("poda", new PodState("inc-1", 42, 7, 1234)));

        Checkpoint decoded = Checkpoint.decode(original.encode());

        assertThat(decoded.pods()).containsExactly(
                Map.entry("poda", new PodState("inc-1", 42, 7, 1234)));
        assertThat(decoded).isEqualTo(original);
    }

    /** Two pods, so a slot cannot be hoisted to a single fleet-wide value. */
    @Test
    void everyPodKeepsItsOwnSlot() throws Exception {
        Checkpoint original = new Checkpoint(3, streams(), Map.of(
                "poda", new PodState("inc-1", 5, 2, 100),
                "podb", new PodState("inc-2", 9, 3, 200)));

        Map<String, PodState> back = Checkpoint.decode(original.encode()).pods();

        assertThat(back.get("poda")).isEqualTo(new PodState("inc-1", 5, 2, 100));
        assertThat(back.get("podb"))
                .as("the second pod keeps its OWN incarnation, watermark and pointer")
                .isEqualTo(new PodState("inc-2", 9, 3, 200));
    }

    /**
     * ⚠️ THE ENUMERATION ITSELF IS PINNED IN
     * {@link CheckpointDecodeRefusalTest}, by a NEGATIVE version — the only
     * shape that tells an enumerated set from `version <= VERSION_ATTRIBUTED`,
     * since version 9 is refused by both. This test pins only that a v0
     * checkpoint still decodes and an unknown one does not.
     */
    @Test
    void aV0CheckpointStillDecodesAndAnUnknownVersionIsRefused() throws Exception {
        byte[] v0;
        try (var in = CheckpointPodStateTest.class.getResourceAsStream("/golden/checkpoint-v0.bin")) {
            v0 = in.readAllBytes();
        }
        Checkpoint legacy = Checkpoint.decode(v0);
        assertThat(legacy.pods().get("poda").lastAppliedFlushSeq())
                .as("a v0 slot's watermark survives")
                .isEqualTo(42);
        assertThat(legacy.pods().get("poda").incarnationId())
                .as("and carries no incarnation, so its window is empty for that pod")
                .isNull();

        byte[] future = v0.clone();
        future[7] = 9;
        assertThatThrownBy(() -> Checkpoint.decode(future))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unsupported checkpoint version");
    }
}
