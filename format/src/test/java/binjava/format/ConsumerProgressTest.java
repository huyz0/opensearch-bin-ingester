// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * What the plugin reports about where its shard copies have got to (M7.1, FR-9,
 * ADR-0005).
 *
 * <p>⚠️ THIS FRAME DECIDES WHAT GC DELETES, so every refusal here is a refusal
 * to delete on a shape nobody sent. A frame that decoded "best effort" — taking
 * the prefix it understood — would hand the watermark table a position for a
 * copy whose identity came from a torn byte, and the consequence is a deleted
 * segment rather than an exception.
 */
class ConsumerProgressTest {

    private static final String LOGS = "nVzgup36TLqWp7VBBREj1w";
    private static final String METRICS = "8Gk1lQ2HRs-TvA4pZ0bXyQ";

    private static ConsumerProgress threeEntries() {
        return new ConsumerProgress(List.of(
                new ConsumerProgress.Entry(LOGS, 0, "alloc-a", 41822L),
                new ConsumerProgress.Entry(LOGS, 3, "alloc-b", 17L),
                new ConsumerProgress.Entry(METRICS, 7, "alloc-c", 900_001L)));
    }

    @Test
    void aBATCHAcrossTWOIndicesRoundTrips() throws Exception {
        assertThat(ConsumerProgress.decode(threeEntries().encode()))
                .as("the node reports every partition it hosts in ONE frame, and each "
                        + "entry keeps its own copy and its own position")
                .isEqualTo(threeEntries());
    }

    @Test
    void aCopyThatHasReadNOTHINGReportsZERO() throws Exception {
        ConsumerProgress fresh = new ConsumerProgress(
                List.of(new ConsumerProgress.Entry(LOGS, 0, "alloc-a", 0L)));
        assertThat(ConsumerProgress.decode(fresh.encode()).entries().get(0).consumedUpTo())
                .as("zero is a real position -- a copy that has indexed nothing has "
                        + "consumed up to offset 0, and refusing it would make a booting "
                        + "shard unreportable and its stream's data deletable")
                .isZero();
    }

    @Test
    void aNEGATIVEPositionIsREFUSED() {
        assertThatThrownBy(() -> new ConsumerProgress.Entry(LOGS, 0, "alloc-a", -1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("consumedUpTo");
    }

    @Test
    void aNEGATIVEPartitionIsREFUSED() {
        assertThatThrownBy(() -> new ConsumerProgress.Entry(LOGS, -1, "alloc-a", 5L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("partition");
    }

    @Test
    void aBLANKIndexUuidOrShardCopyIsREFUSED() {
        assertThatThrownBy(() -> new ConsumerProgress.Entry("  ", 0, "alloc-a", 5L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("indexUuid");
        assertThatThrownBy(() -> new ConsumerProgress.Entry(LOGS, 0, "", 5L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("shardCopy");
    }

    @Test
    void anEMPTYBatchIsREFUSED() {
        assertThatThrownBy(() -> new ConsumerProgress(List.of()))
                .as("a frame carrying no entry reports nothing, and silence already has a "
                        + "meaning here -- research 09 §6.3, a copy that stops reporting "
                        + "FREEZES. An empty frame that counted as a report would keep a "
                        + "dead copy fresh forever, which is the one thing freshness is for")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theSAMECopyTWICEInOneFrameIsREFUSED() {
        assertThatThrownBy(() -> new ConsumerProgress(List.of(
                new ConsumerProgress.Entry(LOGS, 0, "alloc-a", 41822L),
                new ConsumerProgress.Entry(LOGS, 0, "alloc-a", 7L))))
                .as("two positions for one copy in one frame: whichever the table took "
                        + "would be arbitrary, and taking the LATER one deletes the data "
                        + "the EARLIER one still needs")
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("alloc-a");
    }

    @Test
    void theSAMECopyOnTWODifferentPartitionsIsFINE() throws Exception {
        ConsumerProgress twoStreams = new ConsumerProgress(List.of(
                new ConsumerProgress.Entry(LOGS, 0, "alloc-a", 41822L),
                new ConsumerProgress.Entry(LOGS, 1, "alloc-a", 7L)));
        assertThat(ConsumerProgress.decode(twoStreams.encode()).entries())
                .as("identity is (index, partition, copy) and NOT the copy alone -- "
                        + "refusing this would refuse a legitimate report and freeze one "
                        + "of the two streams' watermarks")
                .hasSize(2);
    }

    @Test
    void TWOCopiesOfONEStreamAreBOTHCarried() throws Exception {
        ConsumerProgress replicated = new ConsumerProgress(List.of(
                new ConsumerProgress.Entry(LOGS, 4, "alloc-primary", 41822L),
                new ConsumerProgress.Entry(LOGS, 4, "alloc-replica", 90L)));
        assertThat(ConsumerProgress.decode(replicated.encode()).entries())
                .as("THE LAGGING REPLICA, which is the whole reason shardCopy is in the "
                        + "frame: with all_active = true every copy consumes the partition "
                        + "independently, so one stream legitimately carries two positions "
                        + "in one frame. An identity that dropped the copy would refuse "
                        + "this as a duplicate, and the replica's data would be deleted "
                        + "while it was still reading it")
                .containsExactly(
                        new ConsumerProgress.Entry(LOGS, 4, "alloc-primary", 41822L),
                        new ConsumerProgress.Entry(LOGS, 4, "alloc-replica", 90L));
    }

    @Test
    void theENTRYListIsCOPIEDNotAliased() {
        java.util.List<ConsumerProgress.Entry> mutable = new java.util.ArrayList<>(
                List.of(new ConsumerProgress.Entry(LOGS, 0, "alloc-a", 5L)));
        ConsumerProgress frame = new ConsumerProgress(mutable);
        mutable.clear();
        assertThat(frame.entries()).hasSize(1);
    }

    @Test
    void aTRUNCATEDFrameIsREFUSED() throws Exception {
        byte[] whole = threeEntries().encode();
        for (int cut : new int[] {4, 9, 20, whole.length - 1}) {
            byte[] torn = Arrays.copyOf(whole, cut);
            assertThatThrownBy(() -> ConsumerProgress.decode(torn))
                    .as("cut at %d of %d", cut, whole.length)
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void aFORWARDVersionIsREFUSEDAndTheMessageNamesIt() throws Exception {
        byte[] bytes = threeEntries().encode();
        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).putInt(4, ConsumerProgress.VERSION_1 + 1);
        assertThatThrownBy(() -> ConsumerProgress.decode(bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(String.valueOf(ConsumerProgress.VERSION_1 + 1));
    }

    @Test
    void aVersionBELOWTheKnownOneIsREFUSEDToo() throws Exception {
        byte[] bytes = threeEntries().encode();
        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).putInt(4, 0);
        assertThatThrownBy(() -> ConsumerProgress.decode(bytes))
                .as("version 0 is what zeroed torn bytes carry, and `version > VERSION_1` "
                        + "parses it as v1 -- accepting a shape nobody wrote on the frame "
                        + "that decides what is deleted. An unknown version STOPS, in both "
                        + "directions")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version 0");
    }

    @Test
    void anAMBIGUOUSSplitOfTheIdentityIsNOTADuplicate() throws Exception {
        ConsumerProgress frame = new ConsumerProgress(List.of(
                new ConsumerProgress.Entry("a/1", 2, "alloc-c", 5L),
                new ConsumerProgress.Entry("a", 1, "2/alloc-c", 9L)));
        assertThat(ConsumerProgress.decode(frame.encode()).entries())
                .as("a joined-string identity renders these two identically and refuses a "
                        + "legitimate frame -- fail-closed, but a frame dropped for a "
                        + "reason no message can explain")
                .hasSize(2);
    }

    @Test
    void aWRONGMagicIsREFUSED() throws Exception {
        byte[] bytes = threeEntries().encode();
        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).putInt(0, 0xDEADBEEF);
        assertThatThrownBy(() -> ConsumerProgress.decode(bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("magic");
    }

    @Test
    void anENTRYCountBiggerThanTheFrameIsREFUSEDBeforeItAllocates() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteBuffer head = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        head.putInt(ConsumerProgress.MAGIC);
        head.putInt(ConsumerProgress.VERSION_1);
        out.writeBytes(head.array());
        SegmentWriter.putUvarint(out, Integer.MAX_VALUE);
        byte[] lying = out.toByteArray();
        assertThatThrownBy(() -> ConsumerProgress.decode(lying))
                .as("a torn frame claiming 2^31-1 entries must not size a list before it "
                        + "has read one: an OutOfMemoryError travels past the declared "
                        + "IOException and takes the reporting channel down with it")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("2147483647");
    }

    @Test
    void aFrameWithTRAILINGBytesIsREFUSED() throws Exception {
        byte[] whole = threeEntries().encode();
        byte[] extra = Arrays.copyOf(whole, whole.length + 3);
        assertThatThrownBy(() -> ConsumerProgress.decode(extra))
                .as("more bytes after the last entry is a different shape, two frames run "
                        + "together, or a torn stream -- and the one thing it is not is "
                        + "this frame")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("trailing");
    }

    @Test
    void aPARTITIONTooLargeForAnINTIsREFUSED() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteBuffer head = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        head.putInt(ConsumerProgress.MAGIC);
        head.putInt(ConsumerProgress.VERSION_1);
        out.writeBytes(head.array());
        SegmentWriter.putUvarint(out, 1);
        byte[] uuid = LOGS.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        SegmentWriter.putUvarint(out, uuid.length);
        out.writeBytes(uuid);
        SegmentWriter.putUvarint(out, Long.MAX_VALUE);
        byte[] copy = "alloc-a".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        SegmentWriter.putUvarint(out, copy.length);
        out.writeBytes(copy);
        SegmentWriter.putUvarint(out, 5);
        byte[] oversized = out.toByteArray();
        assertThatThrownBy(() -> ConsumerProgress.decode(oversized))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("partition");
    }

    @Test
    void aDECODEDInvalidFrameThrowsIOExceptionRatherThanIAE() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteBuffer head = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        head.putInt(ConsumerProgress.MAGIC);
        head.putInt(ConsumerProgress.VERSION_1);
        out.writeBytes(head.array());
        SegmentWriter.putUvarint(out, 2);
        for (int i = 0; i < 2; i++) {
            byte[] uuid = LOGS.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            SegmentWriter.putUvarint(out, uuid.length);
            out.writeBytes(uuid);
            SegmentWriter.putUvarint(out, 0);
            byte[] copy = "alloc-a".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            SegmentWriter.putUvarint(out, copy.length);
            out.writeBytes(copy);
            SegmentWriter.putUvarint(out, 5);
        }
        byte[] duplicated = out.toByteArray();
        assertThatThrownBy(() -> ConsumerProgress.decode(duplicated))
                .as("this arrived over a wire: an unchecked throw out of a decode is what "
                        + "kills the loop reading the channel")
                .isInstanceOf(IOException.class);
    }

    @Test
    void aSTRINGLengthTooLargeForTheFrameIsREFUSED() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteBuffer head = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        head.putInt(ConsumerProgress.MAGIC);
        head.putInt(ConsumerProgress.VERSION_1);
        out.writeBytes(head.array());
        SegmentWriter.putUvarint(out, 1);
        SegmentWriter.putUvarint(out, 4096);
        out.writeBytes(new byte[] {1, 2, 3});
        byte[] lying = out.toByteArray();
        assertThatThrownBy(() -> ConsumerProgress.decode(lying))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("4096");
    }
}
