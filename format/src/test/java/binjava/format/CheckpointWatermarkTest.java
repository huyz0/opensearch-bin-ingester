// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.format.Checkpoint.PodState;
import binjava.format.Checkpoint.StreamOffsets;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The consumer watermark in the checkpoint (M7.4, FR-9, research 09 §8).
 *
 * <p>⚠️ IT GOES IN THE OBJECT THAT ALREADY EXISTS — no new object, no new CAS,
 * no new request (cost rule R6). What it costs is one varint per stream on an
 * object already bounded by ADR-0033.
 *
 * <p>⚠️ AND V0 AND V1 MUST KEEP DECODING. A checkpoint already in a bucket is
 * read for the whole retention window, and after a rolling deploy the two
 * versions coexist; the version is chosen by CONTENT, so a checkpoint carrying
 * no watermark encodes byte-for-byte as it did before this commit.
 */
class CheckpointWatermarkTest {

    private static UUID idx(int n) {
        return new UUID(0x1111_2222_3333_4444L, n);
    }

    private static Map<RunKey, StreamOffsets> streams(StreamOffsets... offsets) {
        Map<RunKey, StreamOffsets> out = new LinkedHashMap<>();
        for (int i = 0; i < offsets.length; i++) {
            out.put(new RunKey(idx(i + 1), i + 1), offsets[i]);
        }
        return out;
    }

    @Test
    void aWATERMARKRoundTrips() throws Exception {
        Checkpoint c = new Checkpoint(9,
                streams(new StreamOffsets(1001, 100, OptionalLong.of(640)),
                        new StreamOffsets(2002, 200, OptionalLong.of(1500))),
                Map.of("poda", PodState.bare(42)));
        assertThat(Checkpoint.decode(c.encode())).isEqualTo(c);
    }

    @Test
    void aStreamWithNOWatermarkCarriesNONERatherThanZero() throws Exception {
        Checkpoint c = new Checkpoint(9, streams(new StreamOffsets(1001, 100)),
                Map.of("poda", PodState.bare(42)));
        StreamOffsets back = Checkpoint.decode(c.encode()).streams().values().iterator().next();
        assertThat(back.consumerWatermark())
                .as("⚠️ NOBODY HAS REPORTED IS NOT THE SAME FACT AS NOBODY HAS READ. A "
                        + "zero written for an absent watermark is indistinguishable from "
                        + "a real zero -- which keeps everything, harmlessly -- but the "
                        + "same slot read as 'consumed up to 0' by a later build is the "
                        + "one that deletes")
                .isEmpty();
    }

    @Test
    void aCheckpointWithNOWatermarkEncodesEXACTLYAsItDidBefore() {
        Checkpoint plain = new Checkpoint(9, streams(new StreamOffsets(1001, 100)),
                Map.of("poda", PodState.bare(42)));
        byte[] bytes = plain.encode();
        assertThat(ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt(4))
                .as("the version is chosen by CONTENT: a build that wrote v2 for every "
                        + "checkpoint would make every object it wrote unreadable to the "
                        + "peers still running the previous release")
                .isEqualTo(Checkpoint.VERSION);
    }

    @Test
    void aCheckpointWithATTRIBUTEDPodsAndNOWatermarkIsSTILLV1() {
        Checkpoint attributed = new Checkpoint(9, streams(new StreamOffsets(1001, 100)),
                Map.of("poda", new PodState("i7", 42, 3, 11)));
        assertThat(ByteBuffer.wrap(attributed.encode()).order(ByteOrder.BIG_ENDIAN).getInt(4))
                .isEqualTo(Checkpoint.VERSION_ATTRIBUTED);
    }

    @Test
    void aCheckpointWithAWatermarkIsV2AndCarriesTheAttributionToo() throws Exception {
        Checkpoint both = new Checkpoint(9,
                streams(new StreamOffsets(1001, 100, OptionalLong.of(640))),
                Map.of("poda", new PodState("i7", 42, 3, 11)));
        assertThat(ByteBuffer.wrap(both.encode()).order(ByteOrder.BIG_ENDIAN).getInt(4))
                .isEqualTo(Checkpoint.VERSION_WATERMARKED);
        assertThat(Checkpoint.decode(both.encode()).pods().get("poda"))
                .as("v2 is a SUPERSET of v1: a build that dropped the pod pointer while "
                        + "adding the watermark would make every detected replay "
                        + "unanswerable, which is ADR-0036's criterion 6")
                .isEqualTo(new PodState("i7", 42, 3, 11));
    }

    @Test
    void MIXEDStreamsKeepTheirOWNWatermarkOrLackOfOne() throws Exception {
        Checkpoint mixed = new Checkpoint(9,
                streams(new StreamOffsets(1001, 100, OptionalLong.of(640)),
                        new StreamOffsets(2002, 200),
                        new StreamOffsets(3003, 300, OptionalLong.of(3000))),
                Map.of("poda", PodState.bare(42)));
        Map<RunKey, StreamOffsets> back = Checkpoint.decode(mixed.encode()).streams();
        assertThat(back.get(new RunKey(idx(1), 1)).consumerWatermark()).hasValue(640);
        assertThat(back.get(new RunKey(idx(2), 2)).consumerWatermark())
                .as("MIXED IS THE NORMAL STATE: a stream whose consumers have reported "
                        + "sits beside one whose have not, and a version-wide flag would "
                        + "force the writer to invent a watermark for the second")
                .isEmpty();
        assertThat(back.get(new RunKey(idx(3), 3)).consumerWatermark()).hasValue(3000);
    }

    @Test
    void aWatermarkPASTTheNextOffsetIsREFUSED() {
        assertThatThrownBy(() -> new StreamOffsets(1000, 100, OptionalLong.of(1001)))
                .as("a copy cannot have consumed past the offsets that exist, and a "
                        + "watermark that had would delete every segment of the stream on "
                        + "the next pass")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("consumerWatermark");
    }

    @Test
    void aWatermarkEXACTLYAtTheNextOffsetIsFINE() {
        assertThat(new StreamOffsets(1000, 100, OptionalLong.of(1000)).consumerWatermark())
                .as("consumedUpTo is EXCLUSIVE, so a copy that has read everything "
                        + "committed reports exactly nextOffset -- refusing it would "
                        + "refuse the fully-caught-up consumer, which is the normal state")
                .hasValue(1000);
    }

    @Test
    void aWatermarkBELOWTheOldestRetainedOffsetIsALLOWED() {
        assertThat(new StreamOffsets(1000, 500, OptionalLong.of(20)).consumerWatermark())
                .as("⚠️ THAT STATE IS DATA LOSS AND MUST BE REPRESENTABLE. A copy behind "
                        + "the oldest retained offset has had its records deleted -- the "
                        + "maxRetention ceiling, which alarms. A format that refused it "
                        + "would make the incident unrecordable and the alarm unprovable")
                .hasValue(20);
    }

    @Test
    void aNEGATIVEWatermarkIsREFUSED() {
        assertThatThrownBy(() -> new StreamOffsets(1000, 100, OptionalLong.of(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aNULLWatermarkIsREFUSED() {
        assertThatThrownBy(() -> new StreamOffsets(1000, 100, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void aStreamWatermarkFlagThatIsNEITHERZeroNorOneIsREFUSED() throws Exception {
        Checkpoint c = new Checkpoint(9,
                streams(new StreamOffsets(1001, 100, OptionalLong.of(640))),
                Map.of("poda", PodState.bare(42)));
        byte[] bytes = c.encode();
        // The layout, counted rather than searched for: 8 header + 1 sequence
        // + 1 stream count + 16 uuid + 1 partition + 2 nextOffset (1001) + 1
        // oldestRetainedOffset (100) puts the watermark's presence flag at 30.
        int flag = 30;
        assertThat(bytes[flag]).as("the fixture's watermark presence flag").isEqualTo((byte) 1);
        bytes[flag] = 2;
        assertThatThrownBy(() -> Checkpoint.decode(bytes))
                .as("⚠️ A PRESENCE FLAG IS ENUMERATED, AND `!= 0` IS NOT THE SAME TEST. A "
                        + "reader that took any non-zero byte as 'present' decodes a torn "
                        + "object as valid, with a watermark stolen from the next wire "
                        + "field -- the defect this codec's pod slot shipped with once, "
                        + "and the message must name WHICH field")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("stream watermark");
    }

    @Test
    void aVersionABOVETheKnownOnesIsREFUSED() {
        Checkpoint c = new Checkpoint(9, streams(new StreamOffsets(1001, 100)),
                Map.of("poda", PodState.bare(42)));
        byte[] bytes = c.encode();
        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)
                .putInt(4, Checkpoint.VERSION_WATERMARKED + 1);
        assertThatThrownBy(() -> Checkpoint.decode(bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(String.valueOf(Checkpoint.VERSION_WATERMARKED + 1));
    }

}
