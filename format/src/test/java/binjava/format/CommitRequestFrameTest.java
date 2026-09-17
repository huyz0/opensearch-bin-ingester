// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** One pod's commit on the wire (M8.33, FR-12). */
class CommitRequestFrameTest {

    private static final UUID LOGS = UUID.fromString("00000000-0000-4000-8000-000000000001");
    private static final UUID METRICS = UUID.fromString("00000000-0000-4000-8000-000000000002");

    private static CommitRequestFrame frame() {
        Map<RunKey, Integer> counts = new LinkedHashMap<>();
        counts.put(new RunKey(LOGS, 3), 100);
        counts.put(new RunKey(METRICS, 0), 7);
        return new CommitRequestFrame("pod1", "inc-a", 42, "bins/c/data/seg-1", counts);
    }

    @Test
    void aREQUESTRoundTrips() throws Exception {
        CommitRequestFrame decoded = CommitRequestFrame.decode(frame().encode());
        assertThat(decoded).isEqualTo(frame());
        assertThat(decoded.recordCounts())
                .containsEntry(new RunKey(LOGS, 3), 100)
                .containsEntry(new RunKey(METRICS, 0), 7);
    }

    @Test
    void theSAMERequestEncodesToTheSAMEBytesWhateverTheMapsOrder() {
        Map<RunKey, Integer> reversed = new LinkedHashMap<>();
        reversed.put(new RunKey(METRICS, 0), 7);
        reversed.put(new RunKey(LOGS, 3), 100);
        CommitRequestFrame other =
                new CommitRequestFrame("pod1", "inc-a", 42, "bins/c/data/seg-1", reversed);

        assertThat(other.encode())
                .as("⚠️ `Map.copyOf` GIVES NO ITERATION ORDER, so an unsorted encode makes "
                        + "one commit two sequences of bytes -- which breaks a golden file "
                        + "and, worse, makes a retry's bytes differ from the first attempt's "
                        + "for anything that ever compares them")
                .isEqualTo(frame().encode());
    }

    @Test
    void aFRAMEWithNORecordsIsREFUSED() {
        assertThatThrownBy(() -> new CommitRequestFrame("pod1", "inc-a", 1, "seg", Map.of()))
                .as("⚠️ A COMMIT OF NOTHING would take a zero-length range from every stream "
                        + "it named and still consume a flush sequence, so the leaseholder "
                        + "records a flush the sender never made")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("commits nothing");
    }

    @Test
    void aZEROOrNEGATIVERecordCountIsREFUSEDRatherThanDropped() {
        assertThatThrownBy(() -> new CommitRequestFrame("pod1", "inc-a", 1, "seg",
                Map.of(new RunKey(LOGS, 0), 0)))
                .as("⚠️ A STREAM NAMED WITH NO RECORDS IS A CALLER THAT BELIEVES IT WROTE "
                        + "SOMETHING. Dropping it silently is how the sender and the "
                        + "leaseholder come to disagree about which streams a segment holds")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
        assertThatThrownBy(() -> new CommitRequestFrame("pod1", "inc-a", 1, "seg",
                Map.of(new RunKey(LOGS, 0), -3)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aPODIdWithTheSEGMENTKeySeparatorIsREFUSED() {
        // ⚠️ THE GRAMMAR `CommitRequest` ENFORCES, HERE TOO, because this type
        // is what arrives from ANOTHER process: without it a peer's `pod-1`
        // decodes cleanly and then throws an unchecked IllegalArgumentException
        // out of the conversion, in the loop serving that peer. And it is not
        // cosmetic -- a segment key is `<seq>-<podId>-<flushSeq>-...`, so a `-`
        // in a podId makes the key ambiguous to parse.
        Map<RunKey, Integer> counts = Map.of(new RunKey(LOGS, 0), 1);
        assertThatThrownBy(() -> new CommitRequestFrame("pod-1", "inc-a", 1, "seg", counts))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("may not contain");
        assertThatThrownBy(() -> new CommitRequestFrame("pod/1", "inc-a", 1, "seg", counts))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("may not contain");
    }

    @Test
    void aDECODEDFrameWithABadPodIdIsAnIOExceptionRatherThanAnIAE() throws Exception {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        out.writeBytes(new byte[] {0x42, 0x50, 0x43, 0x52, 0, 0, 0, 1});
        putString(out, "pod-1");
        putString(out, "inc-a");
        SegmentWriter.putUvarint(out, 1);
        putString(out, "seg");
        SegmentWriter.putUvarint(out, 1);
        putUuid(out, LOGS);
        SegmentWriter.putUvarint(out, 0);
        SegmentWriter.putUvarint(out, 5);
        assertThatThrownBy(() -> CommitRequestFrame.decode(out.toByteArray()))
                .as("over a wire it must arrive as an IOException, or the loop serving the "
                        + "peer dies and the fleet stops committing")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not valid");
    }

    @Test
    void aBLANKIdentityIsREFUSED() {
        Map<RunKey, Integer> counts = Map.of(new RunKey(LOGS, 0), 1);
        assertThatThrownBy(() -> new CommitRequestFrame(" ", "inc-a", 1, "seg", counts))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("podId");
        assertThatThrownBy(() -> new CommitRequestFrame("pod1", "", 1, "seg", counts))
                .as("⚠️ ADR-0036: a blank incarnation collapses every incarnation of a pod "
                        + "into one identity, and a restart becomes indistinguishable from "
                        + "a replay")
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("incarnationId");
        assertThatThrownBy(() -> new CommitRequestFrame("pod1", "inc-a", 1, "  ", counts))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("segmentKey");
    }

    @Test
    void aNEGATIVEFlushSeqIsREFUSED() {
        assertThatThrownBy(() -> new CommitRequestFrame("pod1", "inc-a", -1, "seg",
                Map.of(new RunKey(LOGS, 0), 1)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("flushSeq");
    }

    @Test
    void aNULLInAnyFieldIsREFUSED() {
        Map<RunKey, Integer> counts = Map.of(new RunKey(LOGS, 0), 1);
        assertThatThrownBy(() -> new CommitRequestFrame(null, "inc-a", 1, "seg", counts))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new CommitRequestFrame("pod1", null, 1, "seg", counts))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new CommitRequestFrame("pod1", "inc-a", 1, null, counts))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new CommitRequestFrame("pod1", "inc-a", 1, "seg", null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void theCOUNTSAreCOPIEDNotALIASED() {
        Map<RunKey, Integer> mutable = new LinkedHashMap<>();
        mutable.put(new RunKey(LOGS, 0), 5);
        CommitRequestFrame held = new CommitRequestFrame("pod1", "inc-a", 1, "seg", mutable);
        mutable.put(new RunKey(METRICS, 0), 99);

        assertThat(held.recordCounts())
                .as("⚠️ THE CALLER'S MAP IS THE ACCUMULATOR'S, and it keeps filling while "
                        + "the request is in flight")
                .hasSize(1);
    }

    @Test
    void aWRONGMagicIsREFUSED() {
        byte[] bytes = frame().encode();
        bytes[0] ^= 0xFF;
        assertThatThrownBy(() -> CommitRequestFrame.decode(bytes))
                .isInstanceOf(IOException.class).hasMessageContaining("magic");
    }

    @Test
    void aFORWARDVersionIsREFUSEDAndSoIsOneBELOW() throws Exception {
        byte[] forward = frame().encode();
        forward[7] = 9;
        assertThatThrownBy(() -> CommitRequestFrame.decode(forward))
                .isInstanceOf(IOException.class).hasMessageContaining("version 9");

        byte[] zeroed = frame().encode();
        zeroed[7] = 0;
        assertThatThrownBy(() -> CommitRequestFrame.decode(zeroed))
                .as("⚠️ `!=` AND NOT `>`: a version BELOW the known one is what zeroed torn "
                        + "bytes carry, and `> VERSION_1` would parse them as v1")
                .isInstanceOf(IOException.class).hasMessageContaining("version 0");
    }

    @Test
    void aTRUNCATEDFrameIsREFUSED() {
        byte[] whole = frame().encode();
        for (int cut : new int[] {4, 9, 20, whole.length - 1}) {
            byte[] torn = java.util.Arrays.copyOf(whole, cut);
            assertThatThrownBy(() -> CommitRequestFrame.decode(torn))
                    .as("cut at %d", cut)
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void aFrameWithTRAILINGBytesIsREFUSED() {
        byte[] whole = frame().encode();
        byte[] extra = java.util.Arrays.copyOf(whole, whole.length + 1);
        assertThatThrownBy(() -> CommitRequestFrame.decode(extra))
                .as("⚠️ TWO FRAMES RUN TOGETHER, a different shape, or a torn stream -- and "
                        + "the one thing they are not is this frame")
                .isInstanceOf(IOException.class).hasMessageContaining("trailing");
    }

    @Test
    void aSTREAMCountBiggerThanTheFrameIsREFUSEDWhereItIsREAD() throws Exception {
        // ⚠️ AN EARLIER NAME AND COMMENT CLAIMED THIS PREVENTED AN ALLOCATION,
        // which review measured false -- the map is built with no capacity. The
        // guard is still worth having, and what it buys is that a torn frame is
        // refused HERE, with a message an operator can act on, rather than
        // twenty streams later as a truncation -- and that a count of 2^31 does
        // not spend 2^31 iterations failing.
        byte[] whole = frame().encode();
        int countAt = indexOfStreamCount(whole);
        byte[] lying = java.util.Arrays.copyOf(whole, countAt + 1);
        lying[countAt] = 0x7F;
        assertThatThrownBy(() -> CommitRequestFrame.decode(lying))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("streams");
    }

    @Test
    void aDUPLICATEStreamInOneRequestIsREFUSED() throws Exception {
        // ⚠️ UNDECIDABLE, and either answer is wrong: the larger count
        // over-assigns offsets for records nobody wrote, the smaller strands
        // the rest. Built by hand because the record cannot express it.
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        out.writeBytes(new byte[] {0x42, 0x50, 0x43, 0x52, 0, 0, 0, 1});
        putString(out, "pod1");
        putString(out, "inc-a");
        SegmentWriter.putUvarint(out, 1);
        putString(out, "seg");
        SegmentWriter.putUvarint(out, 2);
        for (int i = 0; i < 2; i++) {
            putUuid(out, LOGS);
            SegmentWriter.putUvarint(out, 0);
            SegmentWriter.putUvarint(out, 5);
        }
        assertThatThrownBy(() -> CommitRequestFrame.decode(out.toByteArray()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("two record counts");
    }

    @Test
    void aPARTITIONTooLargeForAnINTIsREFUSEDRatherThanNARROWED() throws Exception {
        // ⚠️ THE CASE REVIEW MEASURED MISSING. With the bound in `toInt`
        // removed, a varint partition of 0x80000000 narrows to a NEGATIVE int
        // and `RunKey`'s own guard threw an IllegalArgumentException straight
        // out of `decode` -- past its `throws IOException`, which is the
        // unchecked throw that kills the loop serving the peer and stops the
        // fleet committing. The whole suite stayed green.
        assertThatThrownBy(() -> CommitRequestFrame.decode(oneStream(0x80000000L, 5)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("does not fit an int");
    }

    @Test
    void aRECORDCountTooLargeForAnINTIsREFUSEDToo() throws Exception {
        assertThatThrownBy(() -> CommitRequestFrame.decode(oneStream(0, 0x100000000L)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("does not fit an int");
    }

    @Test
    void aSTRINGLongerThanTheFrameIsREFUSED() throws Exception {
        // ⚠️ `Cursor.bytes` CATCHES EVERY TRUNCATION THIS SUITE PRODUCES, so
        // the length bound in `getString` was unexercised -- review measured
        // deleting it green. A length that overruns while the bytes AFTER it
        // exist (the rest of the frame) is what reaches it.
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        out.writeBytes(new byte[] {0x42, 0x50, 0x43, 0x52, 0, 0, 0, 1});
        SegmentWriter.putUvarint(out, 4000);
        out.writeBytes("pod1".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        putString(out, "inc-a");
        SegmentWriter.putUvarint(out, 1);
        putString(out, "seg");
        SegmentWriter.putUvarint(out, 1);
        putUuid(out, LOGS);
        SegmentWriter.putUvarint(out, 0);
        SegmentWriter.putUvarint(out, 5);
        assertThatThrownBy(() -> CommitRequestFrame.decode(out.toByteArray()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("4000-byte string");
    }

    /** A hand-built one-stream frame with the given partition and count. */
    private static byte[] oneStream(long partition, long records) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        out.writeBytes(new byte[] {0x42, 0x50, 0x43, 0x52, 0, 0, 0, 1});
        putString(out, "pod1");
        putString(out, "inc-a");
        SegmentWriter.putUvarint(out, 1);
        putString(out, "seg");
        SegmentWriter.putUvarint(out, 1);
        putUuid(out, LOGS);
        SegmentWriter.putUvarint(out, partition);
        SegmentWriter.putUvarint(out, records);
        return out.toByteArray();
    }

    private static void putUuid(java.io.ByteArrayOutputStream out, UUID id) {
        java.nio.ByteBuffer b = java.nio.ByteBuffer.allocate(16)
                .order(java.nio.ByteOrder.BIG_ENDIAN);
        b.putLong(id.getMostSignificantBits());
        b.putLong(id.getLeastSignificantBits());
        out.writeBytes(b.array());
    }

    @Test
    void aDECODEDInvalidFrameThrowsIOExceptionRatherThanIAE() throws Exception {
        // ⚠️ AN UNCHECKED THROW OUT OF A DECODE KILLS THE LOOP SERVING THE
        // PEER, and on this path that stops the fleet committing. A zero
        // record count is refused by the record; over the wire it must arrive
        // as an IOException.
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        out.writeBytes(new byte[] {0x42, 0x50, 0x43, 0x52, 0, 0, 0, 1});
        putString(out, "pod1");
        putString(out, "inc-a");
        SegmentWriter.putUvarint(out, 1);
        putString(out, "seg");
        SegmentWriter.putUvarint(out, 1);
        putUuid(out, LOGS);
        SegmentWriter.putUvarint(out, 0);
        SegmentWriter.putUvarint(out, 0);
        assertThatThrownBy(() -> CommitRequestFrame.decode(out.toByteArray()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("not valid");
    }

    private static void putString(java.io.ByteArrayOutputStream out, String value) {
        byte[] utf8 = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        SegmentWriter.putUvarint(out, utf8.length);
        out.writeBytes(utf8);
    }

    /** Where the stream count's varint starts in a frame built by {@link #frame()}. */
    private static int indexOfStreamCount(byte[] whole) {
        int at = 8;
        at += skipString(whole, at);
        at += skipString(whole, at);
        at += 1;
        at += skipString(whole, at);
        return at;
    }

    private static int skipString(byte[] bytes, int at) {
        int length = bytes[at] & 0x7F;
        return 1 + length;
    }
}
