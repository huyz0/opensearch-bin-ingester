// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.Checkpoint.PodState;
import io.github.huyz0.os.biningester.format.Checkpoint.StreamOffsets;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The bytes of a watermarked checkpoint, pinned (M7.4, `wire-format-change`).
 *
 * <p>⚠️ THE OLD SHAPES ARE PINNED HERE TOO, by their own goldens in
 * {@link GoldenCheckpointTest}, and this file adds the half that matters for an
 * upgrade: a v0 and a v1 object already in a bucket must still decode, and
 * their streams must come back carrying NO watermark rather than a zero.
 */
class GoldenCheckpointV2Test {

    private static byte[] golden(String name) throws IOException {
        try (var in = GoldenCheckpointV2Test.class.getResourceAsStream("/golden/" + name)) {
            assertThat(in).as("missing golden file %s", name).isNotNull();
            return in.readAllBytes();
        }
    }

    private static UUID idx(int n) {
        return new UUID(0x1111_2222_3333_4444L, n);
    }

    static Checkpoint fixture() {
        Map<RunKey, StreamOffsets> streams = new LinkedHashMap<>();
        streams.put(new RunKey(idx(1), 1), new StreamOffsets(1001, 100, OptionalLong.of(640)));
        streams.put(new RunKey(idx(2), 2), new StreamOffsets(2002, 200));
        streams.put(new RunKey(idx(3), 3), new StreamOffsets(3003, 300, OptionalLong.of(3003)));
        return new Checkpoint(9, streams, Map.of("poda", new PodState("i7", 42, 3, 11)));
    }

    @Test
    void aWATERMARKEDCheckpointEncodesToItsStoredBytes() throws Exception {
        assertThat(fixture().encode())
                .as("every value in the fixture is distinct and one stream carries NO "
                        + "watermark, so a per-stream flag written the wrong way round, or "
                        + "a watermark written where oldestRetainedOffset belongs, changes "
                        + "these bytes")
                .isEqualTo(golden("checkpoint-v2.bin"));
    }

    @Test
    void theSTOREDWatermarkedCheckpointStillMeansWhatItMeant() throws Exception {
        assertThat(Checkpoint.decode(golden("checkpoint-v2.bin"))).isEqualTo(fixture());
    }

    @Test
    void aV0CheckpointComesBackWithNOWatermarkAtAll() throws Exception {
        assertThat(Checkpoint.decode(golden("checkpoint-v0.bin")).streams().values())
                .as("a v0 object is read for the whole retention window after the upgrade, "
                        + "and every one of its streams must come back as 'nobody has "
                        + "reported' rather than as 'consumed up to zero'")
                .allSatisfy(o -> assertThat(o.consumerWatermark()).isEmpty());
    }

    @Test
    void aV1CheckpointComesBackWithNOWatermarkAndKEEPSItsPodPointers() throws Exception {
        Checkpoint v1 = Checkpoint.decode(golden("checkpoint-v1.bin"));
        assertThat(v1.streams().values())
                .allSatisfy(o -> assertThat(o.consumerWatermark()).isEmpty());
        assertThat(v1.pods().values())
                .as("the attribution ADR-0036 put in v1 survives a build that knows v2 -- "
                        + "losing it makes a detected replay unanswerable")
                .anySatisfy(p -> assertThat(p.hasPointer()).isTrue());
    }
}
