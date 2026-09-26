// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** The pushed delta: the chain's own bytes, wrapped and pinned (M10.17, ADR-0075). */
class DeltaPushFrameTest {

    private static final UUID LOGS = UUID.fromString("00000000-0000-4000-8000-000000000001");

    private static byte[] golden(String name) throws IOException {
        try (var in = DeltaPushFrameTest.class.getResourceAsStream("/golden/" + name)) {
            assertThat(in).as("missing golden file %s", name).isNotNull();
            return in.readAllBytes();
        }
    }

    /** Every number distinct, so a swapped field cannot hide behind an equal one. */
    private static DeltaPushFrame fixture() {
        return new DeltaPushFrame(9, new CommitDelta(41, "bins/cluster-a/data/seg-a.bseg",
                List.of(new RunCommit(new RunKey(LOGS, 3), 12, 500L))));
    }

    @Test
    void aPushEncodesToItsStoredBytes() throws Exception {
        assertThat(fixture().encode()).isEqualTo(golden("delta-push-v1.bin"));
    }

    @Test
    void theStoredPushStillDecodesToWhatItMeant() throws Exception {
        DeltaPushFrame decoded = DeltaPushFrame.decode(golden("delta-push-v1.bin"));

        assertThat(decoded.epoch()).isEqualTo(9);
        assertThat(decoded.delta().sequence()).isEqualTo(41);
        assertThat(decoded.delta().encode()).as("the chain's own delta bytes, unchanged")
                .isEqualTo(fixture().delta().encode());
    }

    @Test
    void malformedPushesAreRefused() throws Exception {
        byte[] good = fixture().encode();

        byte[] badMagic = good.clone();
        badMagic[3] = 0;
        assertThatThrownBy(() -> DeltaPushFrame.decode(badMagic)).isInstanceOf(IOException.class);

        byte[] badVersion = good.clone();
        badVersion[7] = 2;
        assertThatThrownBy(() -> DeltaPushFrame.decode(badVersion))
                .isInstanceOf(IOException.class).hasMessageContaining("version");

        byte[] trailing = java.util.Arrays.copyOf(good, good.length + 1);
        assertThatThrownBy(() -> DeltaPushFrame.decode(trailing))
                .isInstanceOf(IOException.class).hasMessageContaining("trailing");

        byte[] zeroEpoch = good.clone();
        for (int i = 8; i < 16; i++) {
            zeroEpoch[i] = 0;
        }
        assertThatThrownBy(() -> DeltaPushFrame.decode(zeroEpoch)).isInstanceOf(IOException.class);

        // A length with the top bit set reads negative; the cast must never see it.
        byte[] body = java.util.Arrays.copyOfRange(good, 17, good.length);
        java.io.ByteArrayOutputStream negative = new java.io.ByteArrayOutputStream();
        negative.writeBytes(java.util.Arrays.copyOf(good, 16));
        SegmentWriter.putUvarint(negative, (1L << 63) | body.length);
        negative.writeBytes(body);
        assertThatThrownBy(() -> DeltaPushFrame.decode(negative.toByteArray()))
                .isInstanceOf(IOException.class).hasMessageContaining("body length");

        byte[] truncated = java.util.Arrays.copyOf(good, good.length - 1);
        assertThatThrownBy(() -> DeltaPushFrame.decode(truncated)).isInstanceOf(IOException.class);
    }

    @Test
    void aDamagedDeltaInsideAPushIsRefusedAsMalformedInput() throws Exception {
        // Byte 75 is the run's record count; the golden layout pins it.
        byte[] noRecords = golden("delta-push-v1.bin");
        assertThat(noRecords[75]).as("the premise: the fixture's record count").isEqualTo((byte) 12);
        noRecords[75] = 0;
        assertThatThrownBy(() -> DeltaPushFrame.decode(noRecords))
                .as("network input fails as IOException, never as an unchecked throw")
                .isInstanceOf(IOException.class);
    }

    /** A delta of {@code runs} one-record runs, each in its own stream. */
    private static CommitDelta deltaOfRuns(int runs) {
        List<RunCommit> all = new java.util.ArrayList<>(runs);
        for (int i = 0; i < runs; i++) {
            all.add(new RunCommit(new RunKey(LOGS, i), 1, i));
        }
        return new CommitDelta(0, "bins/cluster-a/data/seg-a.bseg", all);
    }

    @Test
    void theCapIsTheDeltaReplyCapAndIsEnforcedBothWays() throws Exception {
        assertThat(DeltaPushFrame.MAX_DELTA_BYTES)
                .as("a delta batches many pods' commits; the forwarded reply already allows 8 MiB")
                .isEqualTo(8 << 20);

        DeltaPushFrame large = new DeltaPushFrame(1, deltaOfRuns(60_000));
        assertThat(large.delta().encode().length).as("the premise: above 1 MiB")
                .isGreaterThan(1 << 20);
        assertThat(DeltaPushFrame.decode(large.encode()).delta().encode())
                .isEqualTo(large.delta().encode());

        DeltaPushFrame huge = new DeltaPushFrame(1, deltaOfRuns(500_000));
        assertThat(huge.delta().encode().length).as("the premise: above the cap")
                .isGreaterThan(DeltaPushFrame.MAX_DELTA_BYTES);
        assertThatThrownBy(huge::encode).isInstanceOf(IllegalStateException.class);

        // A length prefix one over the cap, with that many bytes behind it.
        java.io.ByteArrayOutputStream over = new java.io.ByteArrayOutputStream();
        over.writeBytes(java.util.Arrays.copyOf(fixture().encode(), 16));
        SegmentWriter.putUvarint(over, DeltaPushFrame.MAX_DELTA_BYTES + 1L);
        over.writeBytes(new byte[DeltaPushFrame.MAX_DELTA_BYTES + 1]);
        assertThatThrownBy(() -> DeltaPushFrame.decode(over.toByteArray()))
                .isInstanceOf(IOException.class).hasMessageContaining("body length");

        // Exactly at the cap the length is allowed; the zero bytes then fail as a delta.
        java.io.ByteArrayOutputStream at = new java.io.ByteArrayOutputStream();
        at.writeBytes(java.util.Arrays.copyOf(fixture().encode(), 16));
        SegmentWriter.putUvarint(at, DeltaPushFrame.MAX_DELTA_BYTES);
        at.writeBytes(new byte[DeltaPushFrame.MAX_DELTA_BYTES]);
        assertThatThrownBy(() -> DeltaPushFrame.decode(at.toByteArray()))
                .isInstanceOf(IOException.class)
                .satisfies(e -> assertThat(e.getMessage()).doesNotContain("body length"));
    }

    @Test
    void anEpochBelowOneIsRefusedOnConstruction() {
        assertThatThrownBy(() -> new DeltaPushFrame(0, fixture().delta()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(new DeltaPushFrame(1, fixture().delta()).epoch())
                .as("the first epoch is a real one").isEqualTo(1);
    }
}
