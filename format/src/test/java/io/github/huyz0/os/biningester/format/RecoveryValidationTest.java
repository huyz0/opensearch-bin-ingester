// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * What a recovery entry refuses, at construction and on the wire (M13.25
 * review round 1, P1-P3, T3, T5), and a seeded round trip (P5).
 *
 * <p>⚠️ A REFUSAL ON THE WIRE IS AN IOException. A shape the bytes can carry
 * but the record refuses, left to throw unchecked, escapes every reader's
 * {@code throws IOException} -- {@code CommitLog.recover}'s included.
 */
class RecoveryValidationTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 3);
    private static final RunKey B = new RunKey(new UUID(2, 2), 0);

    private static void uvarint(ByteArrayOutputStream out, long n) {
        while ((n & ~0x7FL) != 0) {
            out.write((int) ((n & 0x7F) | 0x80));
            n >>>= 7;
        }
        out.write((int) n);
    }

    /** A kinded recovery entry's header and kind. */
    private static ByteArrayOutputStream header(long sequence) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.writeBytes(ByteBuffer.allocate(8).putInt(ChainEntry.MAGIC)
                .putInt(ChainEntry.VERSION_KINDED).array());
        uvarint(out, ChainEntry.KIND_RECOVERY);
        uvarint(out, sequence);
        return out;
    }

    private static void segment(ByteArrayOutputStream out, String key, RunKey run, long first) {
        byte[] k = key.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        uvarint(out, k.length);
        out.writeBytes(k);
        uvarint(out, 1);
        out.writeBytes(ByteBuffer.allocate(16).putLong(run.indexId().getMostSignificantBits())
                .putLong(run.indexId().getLeastSignificantBits()).array());
        uvarint(out, run.partitionId());
        uvarint(out, 1);
        uvarint(out, first);
    }

    @Test
    void aSEGMENTNamedTwiceIsRefusedOnTheWireNotLaterInAReader() {
        ByteArrayOutputStream out = header(1);
        uvarint(out, 2);
        segment(out, "seg/x", A, 0);
        segment(out, "seg/x", B, 0);
        uvarint(out, 0);

        assertThatThrownBy(() -> ChainEntry.decode(out.toByteArray()))
                .as("a delta refuses a segment named twice; a recovery holding one decoded and "
                        + "then threw unchecked from delta() in every reader")
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> new Recovery(1, List.of(
                        new SegmentCommit("seg/x", List.of(new RunCommit(A, 1, 0))),
                        new SegmentCommit("seg/x", List.of(new RunCommit(B, 1, 0)))), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anATTRIBUTEDSegmentIsRefusedRatherThanSilentlyStripped() {
        assertThatThrownBy(() -> new Recovery(1, List.of(new SegmentCommit("seg/x",
                        List.of(new RunCommit(A, 1, 0)),
                        new SegmentCommit.Attribution("pod", "inc", 1))), List.of()))
                .as("this kind encodes no attribution; one would not survive the round trip")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aVOIDOverlappingItsOwnRunIsRefusedAndAnAdjacentOneIsNot() {
        SegmentCommit run = new SegmentCommit("seg/x", List.of(new RunCommit(A, 5, 10)));

        assertThatThrownBy(() -> new Recovery(1, List.of(run),
                        List.of(new Recovery.VoidRange(A, 14, 20))))
                .as("offset 14 both committed and voided by one entry")
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(new Recovery(1, List.of(run), List.of(new Recovery.VoidRange(A, 15, 20),
                        new Recovery.VoidRange(B, 0, 1))).voids())
                .as("a void starting where the run ends is a continuation, not an overlap")
                .hasSize(2);
        assertThat(new Recovery(1, List.of(), List.of(new Recovery.VoidRange(A, 0, 5),
                        new Recovery.VoidRange(A, 5, 9))).voids())
                .as("two adjacent voids of one stream are disjoint")
                .hasSize(2);
    }

    @Test
    void aVOIDCountPastTheBytesIsRefusedBeforeItSizesAList() {
        ByteArrayOutputStream out = header(1);
        uvarint(out, 0);
        uvarint(out, Integer.MAX_VALUE);

        assertThatThrownBy(() -> ChainEntry.decode(out.toByteArray()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("voids");
    }

    @Test
    void aVOIDPartitionPastAnIntIsRefused() {
        ByteArrayOutputStream out = header(1);
        uvarint(out, 0);
        uvarint(out, 1);
        out.writeBytes(new byte[16]);
        uvarint(out, 1L << 33);
        uvarint(out, 0);
        uvarint(out, 1);

        assertThatThrownBy(() -> ChainEntry.decode(out.toByteArray()))
                .as("a partition the int cast would narrow to a valid one")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("partition");
    }

    @Test
    void SEEDEDRecoveriesRoundTrip() throws Exception {
        Random random = new Random(1325);
        for (int i = 0; i < 200; i++) {
            List<SegmentCommit> segments = new ArrayList<>();
            int segs = random.nextInt(3);
            for (int s = 0; s < segs; s++) {
                segments.add(new SegmentCommit("seg/" + i + "/" + s,
                        List.of(new RunCommit(new RunKey(new UUID(9, s), s), 1 + random.nextInt(5),
                                random.nextInt(1000)))));
            }
            List<Recovery.VoidRange> voids = new ArrayList<>();
            int count = segs == 0 ? 1 + random.nextInt(3) : random.nextInt(3);
            long from = 1_000_000;
            for (int v = 0; v < count; v++) {
                long length = 1 + random.nextInt(65_536);
                voids.add(new Recovery.VoidRange(A, from, from + length));
                from += length + random.nextInt(3);
            }
            Recovery recovery = new Recovery(random.nextInt(1 << 20), segments, voids);

            assertThat(ChainEntry.decode(recovery.encode())).as("seed case %d", i)
                    .isEqualTo(recovery);
        }
    }
}
