// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.Checkpoint.DeltaAt;
import io.github.huyz0.os.biningester.format.Checkpoint.PodState;
import io.github.huyz0.os.biningester.format.Checkpoint.SplitFlush;
import io.github.huyz0.os.biningester.format.Checkpoint.StreamOffsets;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The bytes of a checkpoint carrying split flushes, pinned (ADR-0086,
 * `wire-format-change`): a pointed pod slot lists each recent flush whose
 * segment was committed across more than one delta, with those deltas. ⚠️ A
 * checkpoint with none encodes as v2 did, byte for byte -- the older goldens
 * pin that -- and v0 to v2 objects in a bucket still decode.
 */
class GoldenCheckpointV3Test {

    static byte[] golden(String name) throws IOException {
        try (var in = GoldenCheckpointV3Test.class.getResourceAsStream("/golden/" + name)) {
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
        Map<String, PodState> pods = new LinkedHashMap<>();
        pods.put("podb", new PodState("i8", 77, 4, 19));
        pods.put("poda", new PodState("i7", 42, 3, 11, List.of(
                new SplitFlush(41, "seg/z", List.of(new DeltaAt(3, 9), new DeltaAt(3, 10))),
                new SplitFlush(40, "seg/y", List.of(new DeltaAt(2, 30), new DeltaAt(3, 2),
                        new DeltaAt(3, 5))))));
        pods.put("podc", PodState.bare(5));
        return new Checkpoint(9, streams, pods);
    }

    @Test
    void aCHECKPOINTWithSplitFlushesEncodesToItsStoredBytes() throws Exception {
        assertThat(fixture().encode())
                .as("two split flushes handed in descending order, one of three deltas "
                        + "crossing an epoch, beside a pointed slot with none and a bare one")
                .isEqualTo(golden("checkpoint-v3.bin"));
    }

    @Test
    void theSTOREDSplitCheckpointStillMeansWhatItMeant() throws Exception {
        Checkpoint decoded = Checkpoint.decode(golden("checkpoint-v3.bin"));

        assertThat(decoded.pods().get("poda").splits()).extracting(SplitFlush::flushSeq)
                .as("sorted on the wire").containsExactly(40L, 41L);
        assertThat(decoded.encode()).isEqualTo(fixture().encode());
    }

    @Test
    void aCHECKPOINTWithNoSplitFlushStaysAtItsOlderVersion() throws Exception {
        Checkpoint plain = new Checkpoint(9, Map.of(), Map.of("poda", new PodState("i7", 42, 3,
                11)));

        assertThat(java.nio.ByteBuffer.wrap(plain.encode()).getInt(4))
                .isEqualTo(Checkpoint.VERSION_ATTRIBUTED);
    }

    @Test
    void aSPLITCountPastTheBytesLeftIsRefused() throws Exception {
        byte[] bytes = golden("checkpoint-v3.bin");
        assertThat(bytes[0x44]).as("the premise: poda's split count").isEqualTo((byte) 2);
        bytes[0x44] = 0x7f;

        assertThatThrownBy(() -> Checkpoint.decode(bytes)).isInstanceOf(IOException.class)
                .hasMessageContaining("127 split flushes");
    }

    @Test
    void aSPLITKeyLengthPastTheBytesLeftIsRefused() throws Exception {
        byte[] bytes = golden("checkpoint-v3.bin");
        assertThat(bytes[0x46]).as("the premise: seg/y's key length").isEqualTo((byte) 5);
        bytes[0x46] = 0x7f;

        assertThatThrownBy(() -> Checkpoint.decode(bytes)).isInstanceOf(IOException.class)
                .hasMessageContaining("127-byte split segment key");
    }

    @Test
    void aSPLITDeltaCountPastTheBytesLeftIsRefused() throws Exception {
        byte[] bytes = golden("checkpoint-v3.bin");
        assertThat(bytes[0x4c]).as("the premise: seg/y's delta count").isEqualTo((byte) 3);
        bytes[0x4c] = 0x7f;

        assertThatThrownBy(() -> Checkpoint.decode(bytes)).isInstanceOf(IOException.class)
                .hasMessageContaining("127 deltas of a split flush");
    }

    @Test
    void aSPLITFlushListedTwiceIsRefused() {
        Checkpoint.SplitFlush once = new SplitFlush(1, "seg/a",
                List.of(new DeltaAt(1, 1), new DeltaAt(1, 2)));
        Checkpoint.SplitFlush again = new SplitFlush(1, "seg/a",
                List.of(new DeltaAt(1, 3), new DeltaAt(1, 4)));

        assertThatThrownBy(() -> new PodState("i1", 5, 1, 1, List.of(once, again)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("twice");
    }

    @Test
    void aSPLITFlushOfOneDeltaOrOnABareSlotIsRefused() {
        assertThatThrownBy(() -> new SplitFlush(1, "seg/a", List.of(new DeltaAt(1, 1))))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SplitFlush(1, "seg/a",
                List.of(new DeltaAt(1, 2), new DeltaAt(1, 1))))
                .as("descending").isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PodState(null, 5, -1, -1, List.of(new SplitFlush(1,
                "seg/a", List.of(new DeltaAt(1, 1), new DeltaAt(1, 2))))))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
