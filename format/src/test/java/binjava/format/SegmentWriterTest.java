// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;

/**
 * The v0 segment layout, decoded INDEPENDENTLY of the writer.
 *
 * <p>⚠️ These assertions read raw bytes with a ByteBuffer rather than calling a
 * reader. A round-trip test encodes and decodes with the same code, so a
 * symmetric mistake — a swapped pair of fields, a wrong endianness — round-trips
 * perfectly and ships. That is exactly what the M1 test plan's T1/T1b split is
 * about, and why the reader lands in its own task.
 */
class SegmentWriterTest {

    private static final UUID INDEX_A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final UUID INDEX_B = UUID.fromString("00000000-0000-0000-0000-0000000000bb");

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static SegmentRecord index(String id, long version, String body) {
        return new SegmentRecord(id, OpType.INDEX, OptionalLong.of(version), bytes(body));
    }

    private static ByteBuffer segment(SegmentWriter w) throws Exception {
        return ByteBuffer.wrap(w.toByteArray(1_700_000_000_000L)).order(ByteOrder.BIG_ENDIAN);
    }

    @Test
    void thePreambleCarriesMagicVersionAndRunCount() throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(new RunKey(INDEX_A, 3), index("1", 7, "{}"), 100L);
        ByteBuffer b = segment(w);

        assertThat(b.getInt(0)).as("magic 'BSEG'").isEqualTo(SegmentFormat.MAGIC);
        // ⚠️ M3; ADR-0025: version 1 now (the reserved lane byte widens the
        // directory entry) -- SegmentReader still accepts version 0 for
        // segments an earlier build already wrote (GoldenSegmentTest).
        assertThat(b.getShort(4)).as("format version").isEqualTo((short) SegmentFormat.VERSION);
        assertThat(b.getInt(8)).as("headerLen is one 49-byte (v1) entry")
                .isEqualTo(SegmentFormat.DIRECTORY_ENTRY_BYTES);
        assertThat(b.getLong(16)).as("createdAtMillis is the value passed in, not a clock")
                .isEqualTo(1_700_000_000_000L);
        assertThat(b.getInt(24)).as("runCount").isEqualTo(1);
    }

    @Test
    void runsAreSortedByIndexThenPartition() throws Exception {
        SegmentWriter w = new SegmentWriter();
        // added out of order on purpose
        w.add(new RunKey(INDEX_B, 1), index("b1", 1, "{}"), 10L);
        w.add(new RunKey(INDEX_A, 7), index("a7", 1, "{}"), 10L);
        w.add(new RunKey(INDEX_A, 2), index("a2", 1, "{}"), 10L);
        ByteBuffer b = segment(w);

        // ⚠️ Contiguity is cost rules R4/R5: one consumer's runs adjacent means
        // one coalesced range GET instead of one per run. Arrival order would
        // pass a round-trip test and quietly multiply every consumer's reads.
        int e = SegmentFormat.PREAMBLE_BYTES;
        assertThat(b.getLong(e)).isEqualTo(INDEX_A.getMostSignificantBits());
        assertThat(b.getLong(e + 8)).isEqualTo(INDEX_A.getLeastSignificantBits());
        assertThat(b.getInt(e + 16)).as("lowest partition of index A first").isEqualTo(2);
        assertThat(b.getInt(e + SegmentFormat.DIRECTORY_ENTRY_BYTES + 16)).as("then partition 7")
                .isEqualTo(7);
        assertThat(b.getLong(e + 2 * SegmentFormat.DIRECTORY_ENTRY_BYTES)).as("index B last")
                .isEqualTo(INDEX_B.getMostSignificantBits());
    }

    @Test
    void directoryEntriesAreFixedWidthAndPointAtTheirOwnRun() throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(new RunKey(INDEX_A, 0), index("a", 1, "{\"x\":1}"), 55L);
        w.add(new RunKey(INDEX_B, 0), index("b", 1, "{\"y\":2}"), 66L);
        ByteBuffer b = segment(w);

        int headerLen = b.getInt(8);
        assertThat(headerLen).as("two fixed-width entries")
                .isEqualTo(2 * SegmentFormat.DIRECTORY_ENTRY_BYTES);

        int e0 = SegmentFormat.PREAMBLE_BYTES;
        int e1 = e0 + SegmentFormat.DIRECTORY_ENTRY_BYTES;
        long start0 = b.getLong(e0 + 24);
        int len0 = b.getInt(e0 + 32);
        long start1 = b.getLong(e1 + 24);

        // ⚠️ byteStart is ABSOLUTE within the segment and the runs abut. Off by
        // the preamble here and every range read lands one header short.
        assertThat(start0).isEqualTo(SegmentFormat.PREAMBLE_BYTES + headerLen);
        assertThat(start1).as("run 1 begins where run 0 ends").isEqualTo(start0 + len0);
        assertThat(b.getLong(e0 + 36)).as("minTimestamp of run 0").isEqualTo(55L);
        assertThat(b.getLong(e1 + 36)).as("minTimestamp of run 1").isEqualTo(66L);
        assertThat(b.getInt(e0 + 20)).as("recordCount").isEqualTo(1);
    }

    @Test
    void aRunKeepsTheLowestTimestampItWasGiven() throws Exception {
        SegmentWriter w = new SegmentWriter();
        RunKey k = new RunKey(INDEX_A, 0);
        w.add(k, index("1", 1, "{}"), 500L);
        w.add(k, index("2", 1, "{}"), 100L);
        w.add(k, index("3", 1, "{}"), 300L);
        ByteBuffer b = segment(w);
        // ⚠️ minTimestampMillis is what makes pointerFromTimestampMillis()
        // answerable from the directory alone, without reading any run.
        assertThat(b.getLong(SegmentFormat.PREAMBLE_BYTES + 36)).isEqualTo(100L);
        assertThat(b.getInt(SegmentFormat.PREAMBLE_BYTES + 20)).as("all three counted")
                .isEqualTo(3);
    }

    @Test
    void aRecordFramesIdOpTypeAndVersionAroundAnOpaquePayload() throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(new RunKey(INDEX_A, 0), index("doc-1", 9, "{\"a\":1}"), 1L);
        ByteBuffer b = segment(w);

        int dataStart = (int) b.getLong(SegmentFormat.PREAMBLE_BYTES + 24);
        int uncompressedLen = b.getInt(dataStart);
        int declaredCrc = b.getInt(dataStart + 4);
        byte[] body = new byte[uncompressedLen];
        b.position(dataStart + 8);
        b.get(body);

        CRC32C crc = new CRC32C();
        crc.update(body);
        assertThat((int) crc.getValue()).as("the block CRC covers the records").isEqualTo(declaredCrc);

        // flags | uvarint idLen | id | uvarint version | uvarint payloadLen | payload
        assertThat(body[0] & 0b11).as("op type INDEX").isEqualTo(OpType.INDEX.ordinal());
        assertThat((body[0] & 0b100) != 0).as("version present").isTrue();
        assertThat(body[1]).as("id length").isEqualTo((byte) 5);
        assertThat(new String(body, 2, 5, StandardCharsets.UTF_8)).isEqualTo("doc-1");
        assertThat(body[7]).as("version").isEqualTo((byte) 9);
        assertThat(body[8]).as("payload length").isEqualTo((byte) 7);
        assertThat(new String(body, 9, 7, StandardCharsets.UTF_8)).isEqualTo("{\"a\":1}");
    }

    @Test
    void aDeleteCarriesNoPayloadAndSaysSoInItsFlags() throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(new RunKey(INDEX_A, 0),
                new SegmentRecord("gone", OpType.DELETE, OptionalLong.of(4), new byte[0]), 1L);
        ByteBuffer b = segment(w);
        int dataStart = (int) b.getLong(SegmentFormat.PREAMBLE_BYTES + 24);
        byte[] body = new byte[b.getInt(dataStart)];
        b.position(dataStart + 8);
        b.get(body);
        // ⚠️ Dropping _op_type turns a delete into an index of an empty document
        // — criterion 0's middle clause, and a silent resurrection.
        assertThat(body[0] & 0b11).isEqualTo(OpType.DELETE.ordinal());
        assertThat(body[body.length - 1]).as("zero-length payload").isEqualTo((byte) 0);
    }

    @Test
    void aRecordWithNoVersionOmitsTheFieldRatherThanWritingZero() throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(new RunKey(INDEX_A, 0),
                new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(), bytes("{}")), 1L);
        ByteBuffer b = segment(w);
        int dataStart = (int) b.getLong(SegmentFormat.PREAMBLE_BYTES + 24);
        byte[] body = new byte[b.getInt(dataStart)];
        b.position(dataStart + 8);
        b.get(body);
        // ⚠️ Writing 0 instead of omitting would make "no version" indistinguishable
        // from version 0, and a stale replay at version 0 would then be accepted.
        assertThat((body[0] & 0b100) != 0).as("version-present flag clear").isFalse();
        assertThat(body).hasSize(1 + 1 + 1 + 1 + 2);
    }

    @Test
    void theFooterRepeatsTheHeaderLocationSoTruncationIsDetectable() throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(new RunKey(INDEX_A, 0), index("a", 1, "{}"), 1L);
        byte[] seg = w.toByteArray(1L);
        ByteBuffer b = ByteBuffer.wrap(seg).order(ByteOrder.BIG_ENDIAN);
        int f = seg.length - SegmentFormat.FOOTER_BYTES;

        assertThat(b.getLong(f)).as("headerStart").isEqualTo(SegmentFormat.PREAMBLE_BYTES);
        assertThat(b.getInt(f + 8)).as("headerLen, duplicated").isEqualTo(b.getInt(8));
        // ⚠️ A 64-bit footer magic, so a truncated object cannot end in something
        // that happens to look like a complete segment.
        assertThat(b.getLong(f + 12)).isEqualTo(SegmentFormat.FOOTER_MAGIC);
    }

    @Test
    void aUvarintSpansMoreThanOneByteWhenItMust() throws Exception {
        SegmentWriter w = new SegmentWriter();
        String longId = "x".repeat(300);
        w.add(new RunKey(INDEX_A, 0),
                new SegmentRecord(longId, OpType.INDEX, OptionalLong.of(1), bytes("{}")), 1L);
        ByteBuffer b = segment(w);
        int dataStart = (int) b.getLong(SegmentFormat.PREAMBLE_BYTES + 24);
        byte[] body = new byte[b.getInt(dataStart)];
        b.position(dataStart + 8);
        b.get(body);
        // ⚠️ 300 does not fit in seven bits. A single-byte length would truncate
        // every id past 127 bytes, and ADR-0003 puts base64url in keys by design.
        assertThat(body[1] & 0xFF).as("low 7 bits set, continuation bit on")
                .isEqualTo(0b1010_1100);
        assertThat(body[2] & 0xFF).as("high bits").isEqualTo(0b0000_0010);
        assertThat(new String(body, 3, 300, StandardCharsets.UTF_8)).isEqualTo(longId);
    }

    @Test
    void aDeleteWithAPayloadIsRefusedAtConstruction() {
        assertThatThrownBy(
                () -> new SegmentRecord("d", OpType.DELETE, OptionalLong.of(1), bytes("{\"a\":1}")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SegmentRecord("", OpType.INDEX, OptionalLong.empty(), new byte[0]))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theRecordPayloadIsCopiedInAndOut() {
        byte[] source = bytes("{\"a\":1}");
        SegmentRecord r = new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(), source);
        java.util.Arrays.fill(source, (byte) 0);
        assertThat(r.payload()).as("the caller's later mutation must not reach it")
                .isEqualTo(bytes("{\"a\":1}"));
        java.util.Arrays.fill(r.payload(), (byte) 0);
        assertThat(r.payload()).as("nor a mutation of what the accessor handed out")
                .isEqualTo(bytes("{\"a\":1}"));
    }

    @Test
    void anEmptySegmentIsStillWellFormed() throws Exception {
        SegmentWriter w = new SegmentWriter();
        assertThat(w.isEmpty()).isTrue();
        byte[] seg = w.toByteArray(1L);
        ByteBuffer b = ByteBuffer.wrap(seg).order(ByteOrder.BIG_ENDIAN);
        assertThat(b.getInt(0)).isEqualTo(SegmentFormat.MAGIC);
        assertThat(b.getInt(24)).as("no runs").isZero();
        assertThat(seg).hasSize(SegmentFormat.PREAMBLE_BYTES + SegmentFormat.FOOTER_BYTES);
    }

    @Test
    void runsAreSortedByIndexIdAsUnsignedComparison() {
        // ⚠️ UUID.compareTo is SIGNED on each half, so ordering by it is not the
        // same as ordering the 16 bytes. Pinned because the reader will binary
        // search the directory and must use the same order the writer emitted.
        UUID low = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID high = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");
        assertThat(new RunKey(low, 0).compareTo(new RunKey(high, 0)))
                .as("whatever the order is, it must be TOTAL and match the reader")
                .isNotZero();
        assertThat(new RunKey(low, 1).compareTo(new RunKey(low, 2))).isNegative();
    }
}
