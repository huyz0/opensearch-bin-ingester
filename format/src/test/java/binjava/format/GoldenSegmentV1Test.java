// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;

/**
 * A {@code VERSION} (v1) segment, written by an EARLIER v1 build, must still
 * parse (M3; ADR-0025).
 *
 * <p>⚠️ {@code GoldenSegmentTest}'s own {@code golden/segment-v0.bin} is
 * deliberately UNTOUCHED by this task — the wire-format-change skill's own
 * checklist wants golden-file tests for BOTH the old and the new shape, not
 * the old one rewritten to look like the new one. This file is the NEW
 * shape's half of that pair: {@code golden/segment-v1.bin}, with the
 * reserved {@code lane} byte every directory entry now carries.
 *
 * <p>⚠️ Do NOT regenerate this file to make the test pass — same discipline
 * as {@code GoldenSegmentTest}'s own warning. It was written once (a small
 * fixture, generated via {@code SegmentWriter} and pasted in, mirroring
 * M2.4's own golden-file workflow) and committed; if this test fails, either
 * the v1 format changed again — non-negotiable 8 applies, a THIRD golden
 * file and another version bump — or the reader broke.
 */
class GoldenSegmentV1Test {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final UUID B = UUID.fromString("00000000-0000-0000-0000-0000000000bb");

    private static byte[] golden() throws Exception {
        try (var in = GoldenSegmentV1Test.class.getResourceAsStream("/golden/segment-v1.bin")) {
            assertThat(in).as("golden/segment-v1.bin must be on the test classpath").isNotNull();
            return in.readAllBytes();
        }
    }

    /** ⚠️ The SAME v0 fixture {@code GoldenSegmentTest} reads, untouched. */
    private static byte[] goldenV0() throws Exception {
        try (var in = GoldenSegmentV1Test.class.getResourceAsStream("/golden/segment-v0.bin")) {
            assertThat(in).as("golden/segment-v0.bin must be on the test classpath").isNotNull();
            return in.readAllBytes();
        }
    }

    @Test
    void theCommittedGoldenV1SegmentStillParsesAtTheRightVersion() throws Exception {
        byte[] bytes = golden();
        int version = java.nio.ByteBuffer.wrap(bytes)
                .order(java.nio.ByteOrder.BIG_ENDIAN).getShort(4);
        assertThat(version).as("this fixture is genuinely v1, not accidentally v0")
                .isEqualTo(SegmentFormat.VERSION);

        SegmentReader r = SegmentReader.open(bytes);
        assertThat(r.createdAtMillis()).isEqualTo(1_700_000_000_000L);
        assertThat(r.directory()).hasSize(2);
        assertThat(r.directory().get(0).key()).isEqualTo(new RunKey(A, 0));
        assertThat(r.directory().get(0).recordCount()).isEqualTo(2);
        assertThat(r.directory().get(0).minTimestampMillis()).isEqualTo(1000L);
        assertThat(r.directory().get(1).key()).isEqualTo(new RunKey(B, 0));
    }

    @Test
    void theReservedLaneByteRoundTripsAsZeroForEveryRunInTheGoldenV1Segment() throws Exception {
        // ⚠️ The property this whole task exists to prove: the NEW byte a v0
        // reader never had to think about is present, in the right place,
        // and decodes to exactly what the writer put there -- 0, since no
        // caller has a real lane value to write until M10.
        SegmentReader r = SegmentReader.open(golden());
        for (RunEntry e : r.directory()) {
            assertThat(e.lane()).as("reserved, unwired until M10").isEqualTo((byte) 0);
        }
    }

    @Test
    void aFreshlyWrittenSegmentRoundTripsTheReservedLaneByteThroughTheRealWriterAndReader()
            throws Exception {
        // ⚠️ round-1 review (M3.0): the golden-fixture tests above pin
        // COMMITTED bytes, which cannot catch a change that only affects
        // freshly-encoded segments (or a field-reordering that happens to
        // still match the frozen fixture byte-for-byte). This exercises the
        // ACTUAL production SegmentWriter and SegmentReader together, not a
        // pre-baked byte array.
        SegmentWriter w = new SegmentWriter();
        w.add(new RunKey(A, 0), new SegmentRecord("x", OpType.INDEX,
                java.util.OptionalLong.of(1), new byte[] {1}), 1L);
        byte[] segment = w.toByteArray(1L);

        SegmentReader r = SegmentReader.open(segment);
        assertThat(r.directory()).singleElement()
                .satisfies(e -> assertThat(e.lane()).isEqualTo((byte) 0));
    }

    @Test
    void theLaneByteIsReadFromItsOwnOffsetNotAliasedToCodecFlags() throws Exception {
        // ⚠️ test-reviewer round 1 (M3.0): every other test here has BOTH
        // codecAndFlags and lane at 0, so a reader aliasing lane's offset to
        // codecAndFlags's (e+44 instead of e+48) would still pass everything
        // -- both fields read as 0 either way. This test makes them DIFFER:
        // codecAndFlags is patched to a non-zero sentinel (SegmentWriter has
        // no public way to do this, so the patch is applied directly to a
        // real, otherwise-valid segment's bytes, with the header CRC
        // recomputed to match) while lane stays untouched at 0. If the
        // reader read lane from codecAndFlags's offset, it would now report
        // the sentinel instead of 0.
        SegmentWriter w = new SegmentWriter();
        w.add(new RunKey(A, 0), new SegmentRecord("x", OpType.INDEX, OptionalLong.of(1),
                new byte[] {1}), 1L);
        byte[] segment = w.toByteArray(1L);

        int e = SegmentFormat.PREAMBLE_BYTES;
        int codecAndFlagsOffset = e + 44;
        // ⚠️ round-2 review (M3.0): ALL FOUR bytes of this int must be
        // non-zero, not just the first. A reader aliased to codecAndFlags's
        // offset by an off-by-one (e.g. reading its LAST byte, immediately
        // adjacent to the true lane offset, rather than its first) would
        // still read 0 if only the leading byte were non-zero -- a sentinel
        // with a single non-zero byte proved the e+44 case but not e+45,
        // e+46 or e+47.
        int sentinel = 0x2A2A2A2A;
        ByteBuffer buf = ByteBuffer.wrap(segment).order(ByteOrder.BIG_ENDIAN);
        buf.putInt(codecAndFlagsOffset, sentinel);
        recomputeHeaderCrc(buf, segment);

        SegmentReader r = SegmentReader.open(segment);
        RunEntry entry = r.directory().get(0);
        assertThat(entry.codecAndFlags()).as("the patch landed where intended")
                .isEqualTo(sentinel);
        assertThat(entry.lane()).as("must read the LANE byte, not codecAndFlags's")
                .isEqualTo((byte) 0);
    }

    @Test
    void aVersion0SegmentSynthesisesLaneAsZeroNotGarbage() throws Exception {
        // ⚠️ test-reviewer round 1 (M3.0): GoldenSegmentTest (untouched,
        // pre-existing) never calls .lane() on the v0 fixture it opens, and
        // every lane assertion elsewhere in THIS file only ever reads a v1
        // (VERSION-tagged) segment -- so the reader's synthesised-0 path for
        // a version-0 entry, which has no lane byte in its bytes at all, was
        // never independently exercised. This is that fixture, that path.
        SegmentReader r = SegmentReader.open(goldenV0());
        assertThat(r.directory()).isNotEmpty();
        for (RunEntry entry : r.directory()) {
            assertThat(entry.lane()).as("synthesised, not read from bytes that don't exist")
                    .isEqualTo((byte) 0);
        }
    }

    @Test
    void aVersion0SegmentSynthesisesZeroEvenWhenTheByteAfterTheEntryIsGenuinelyNonZero()
            throws Exception {
        // ⚠️ round-2 review (M3.0): the test above does NOT actually kill a
        // reader that drops the version guard and reads raw bytes at e+48
        // regardless -- golden/segment-v0.bin happens to have 0x00 at every
        // entry's "would-be" lane offset (the next entry's UUID, or the
        // DATA section's length prefix, both coincidentally zero at this
        // fixture's small scale). This is a SYNTHETIC v0 segment (hand-built,
        // not going through SegmentWriter, which can no longer emit v0 at
        // all) with a SECOND index whose UUID's leading byte is deliberately
        // non-zero, landing exactly at the first entry's own e+48 -- so a
        // reader that read raw bytes there instead of synthesising 0 would
        // report THIS value, not 0.
        UUID nonZeroLead = UUID.fromString("12000000-0000-0000-0000-000000000001");
        assertThat(nonZeroLead.getMostSignificantBits() >>> 56).as("leading byte is non-zero")
                .isNotEqualTo(0L);

        byte[] segment = buildSyntheticV0Segment(A, nonZeroLead);
        SegmentReader r = SegmentReader.open(segment);
        assertThat(r.directory()).hasSize(2);
        assertThat(r.directory().get(1).key().indexId()).isEqualTo(nonZeroLead);
        assertThat(r.directory().get(0).lane())
                .as("synthesised 0, not the next entry's genuinely non-zero leading byte")
                .isEqualTo((byte) 0);
    }

    /**
     * A hand-built, {@link SegmentFormat#VERSION_0}-shaped segment (48-byte
     * entries, no lane byte) with two single-record runs -- {@code
     * SegmentWriter} can no longer emit this shape at all (M3; ADR-0025), so
     * the one test above that needs a REAL v0 segment with a controlled
     * byte value builds it directly, mirroring {@code SegmentWriter}'s own
     * layout exactly but at {@link SegmentFormat#VERSION_0}'s narrower width.
     */
    private static byte[] buildSyntheticV0Segment(UUID first, UUID second) throws IOException {
        byte[] block0 = {0, 0, 0, 1, 0, 0, 0, 0, 9}; // [len=1][crc=0][content=9] -- crc unchecked by directory()
        byte[] block1 = {0, 0, 0, 1, 0, 0, 0, 0, 8};
        int headerLen = 2 * SegmentFormat.DIRECTORY_ENTRY_BYTES_V0;
        long byteStart0 = SegmentFormat.PREAMBLE_BYTES + headerLen;
        long byteStart1 = byteStart0 + block0.length;

        ByteBuffer dir = ByteBuffer.allocate(headerLen).order(ByteOrder.BIG_ENDIAN);
        for (var run : List.of(
                new Object[] {first, byteStart0, block0.length},
                new Object[] {second, byteStart1, block1.length})) {
            UUID id = (UUID) run[0];
            dir.putLong(id.getMostSignificantBits());
            dir.putLong(id.getLeastSignificantBits());
            dir.putInt(0); // partition
            dir.putInt(1); // recordCount
            dir.putLong((long) run[1]);
            dir.putInt((int) run[2]);
            dir.putLong(0L); // minTimestampMillis
            dir.putInt(SegmentFormat.CODEC_NONE);
        }
        byte[] directory = dir.array();

        CRC32C headerCrc = new CRC32C();
        headerCrc.update(directory);
        ByteBuffer preamble = ByteBuffer.allocate(SegmentFormat.PREAMBLE_BYTES).order(ByteOrder.BIG_ENDIAN);
        preamble.putInt(SegmentFormat.MAGIC);
        preamble.putShort((short) SegmentFormat.VERSION_0);
        preamble.putShort((short) 0);
        preamble.putInt(headerLen);
        preamble.putInt((int) headerCrc.getValue());
        preamble.putLong(1L);
        preamble.putInt(2);
        preamble.putInt(0);

        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        out.write(preamble.array());
        out.write(directory);
        out.write(block0);
        out.write(block1);

        ByteBuffer footer = ByteBuffer.allocate(SegmentFormat.FOOTER_BYTES).order(ByteOrder.BIG_ENDIAN);
        footer.putLong(SegmentFormat.PREAMBLE_BYTES);
        footer.putInt(headerLen);
        footer.putLong(SegmentFormat.FOOTER_MAGIC);
        CRC32C footerCrc = new CRC32C();
        footerCrc.update(footer.array(), 0, 20);
        footer.putInt((int) footerCrc.getValue());
        out.write(footer.array());
        return out.toByteArray();
    }

    private static void recomputeHeaderCrc(ByteBuffer buf, byte[] segment) {
        int headerLen = buf.getInt(8);
        CRC32C crc = new CRC32C();
        crc.update(segment, SegmentFormat.PREAMBLE_BYTES, headerLen);
        buf.putInt(12, (int) crc.getValue());
    }

    @Test
    void everyRecordInTheGoldenV1SegmentDecodesToWhatItWasWrittenAs() throws Exception {
        SegmentReader r = SegmentReader.open(golden());

        List<SegmentRecord> a0 = r.read(r.find(new RunKey(A, 0)).orElseThrow());
        assertThat(a0).hasSize(2);
        assertThat(a0.get(0).id()).isEqualTo("doc-1");
        assertThat(a0.get(0).opType()).isEqualTo(OpType.INDEX);
        assertThat(a0.get(0).version()).hasValue(1);
        assertThat(new String(a0.get(0).payload(), StandardCharsets.UTF_8)).isEqualTo("{\"n\":1}");
        assertThat(a0.get(1).opType()).isEqualTo(OpType.DELETE);
        assertThat(a0.get(1).payload()).isEmpty();

        List<SegmentRecord> b0 = r.read(r.find(new RunKey(B, 0)).orElseThrow());
        assertThat(b0).singleElement().satisfies(rec -> {
            assertThat(rec.id()).isEqualTo("doc-3");
            assertThat(rec.opType()).isEqualTo(OpType.CREATE);
            assertThat(rec.payload()).hasSize(200);
        });
    }

    @Test
    void aSegmentClaimingAnUnknownVersionIsRefused() throws Exception {
        byte[] bytes = golden().clone();
        // ⚠️ Byte 4-5 is the u16 version field (big-endian) -- bump it to a
        // version neither VERSION nor VERSION_0 recognises.
        bytes[5] = (byte) (SegmentFormat.VERSION + 1);
        assertThatThrownBy(() -> SegmentReader.open(bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unsupported segment version");
    }

    @Test
    void directoryEntryBytesForRefusesAnyVersionOtherThanTheTwoKnownOnes() {
        assertThatThrownBy(() -> SegmentFormat.directoryEntryBytesFor(2))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unsupported segment version");
        assertThatThrownBy(() -> SegmentFormat.directoryEntryBytesFor(-1))
                .isInstanceOf(IOException.class);
    }

    @Test
    void directoryEntryBytesForMapsEachKnownVersionToItsOwnWidthNotTheOther() throws Exception {
        // ⚠️ round-2 review (M3.0): the refusal test above never actually
        // asserts on the two ACCEPTED versions directly -- a mutation
        // swapping which width each maps to (VERSION -> V0's width and vice
        // versa) is still caught, but only indirectly through a
        // headerLen-disagreement IOException several tests away, not here,
        // on the unit under test.
        assertThat(SegmentFormat.directoryEntryBytesFor(SegmentFormat.VERSION))
                .isEqualTo(SegmentFormat.DIRECTORY_ENTRY_BYTES);
        assertThat(SegmentFormat.directoryEntryBytesFor(SegmentFormat.VERSION_0))
                .isEqualTo(SegmentFormat.DIRECTORY_ENTRY_BYTES_V0);
    }
}
