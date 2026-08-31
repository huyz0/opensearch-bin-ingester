// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** The v0 reader: round trip, directory lookup, and what it must refuse. */
class SegmentReaderTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final UUID B = UUID.fromString("00000000-0000-0000-0000-0000000000bb");
    private static final UUID C = UUID.fromString("00000000-0000-0000-0000-0000000000cc");

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private static SegmentRecord index(String id, long v, String body) {
        return new SegmentRecord(id, OpType.INDEX, OptionalLong.of(v), bytes(body));
    }

    @Test
    void everyRunRoundTripsWithItsRecordsIntact() throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(new RunKey(A, 0), index("a1", 1, "{\"n\":1}"), 10L);
        w.add(new RunKey(A, 0), new SegmentRecord("a2", OpType.DELETE, OptionalLong.of(2), new byte[0]), 20L);
        w.add(new RunKey(A, 0), new SegmentRecord("a3", OpType.INDEX, OptionalLong.empty(), bytes("{}")), 30L);
        w.add(new RunKey(B, 5), index("b1", 7, "{\"n\":2}"), 40L);

        SegmentReader r = SegmentReader.open(w.toByteArray(99L));
        assertThat(r.createdAtMillis()).isEqualTo(99L);
        assertThat(r.directory()).hasSize(2);

        List<SegmentRecord> runA = r.read(r.find(new RunKey(A, 0)).orElseThrow());
        assertThat(runA).hasSize(3);
        assertThat(runA.get(0).id()).isEqualTo("a1");
        assertThat(runA.get(0).version()).hasValue(1);
        assertThat(runA.get(0).payload()).isEqualTo(bytes("{\"n\":1}"));
        // ⚠️ Order within a run IS the format: records are replayed in it.
        assertThat(runA.get(1).opType()).isEqualTo(OpType.DELETE);
        assertThat(runA.get(1).payload()).isEmpty();
        assertThat(runA.get(2).version()).isEmpty();

        List<SegmentRecord> runB = r.read(r.find(new RunKey(B, 5)).orElseThrow());
        assertThat(runB).singleElement().satisfies(rec -> {
            assertThat(rec.id()).isEqualTo("b1");
            assertThat(rec.version()).hasValue(7);
        });
    }

    @Test
    void theDirectoryFindsTheFirstTheLastAndReportsAnAbsentStream() throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(new RunKey(A, 0), index("a", 1, "{}"), 1L);
        w.add(new RunKey(B, 0), index("b", 1, "{}"), 1L);
        w.add(new RunKey(C, 0), index("c", 1, "{}"), 1L);
        SegmentReader r = SegmentReader.open(w.toByteArray(1L));

        // ⚠️ FIRST and LAST explicitly. An off-by-one in the binary search bound
        // passes every fixture that happens to look only in the middle, and the
        // M1 test plan names that omission for exactly this case.
        assertThat(r.find(new RunKey(A, 0))).isPresent();
        assertThat(r.find(new RunKey(C, 0))).isPresent();
        assertThat(r.find(new RunKey(B, 0))).isPresent();
        // absent past both ends and in between
        assertThat(r.find(new RunKey(UUID.fromString("00000000-0000-0000-0000-000000000000"), 0)))
                .as("before the first").isEmpty();
        assertThat(r.find(new RunKey(UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"), 0)))
                .as("after the last").isEmpty();
        assertThat(r.find(new RunKey(A, 99))).as("same index, absent partition").isEmpty();
    }

    @Test
    void aSingleRunSegmentIsFoundAtBothBoundsOfTheSearch() throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(new RunKey(A, 0), index("a", 1, "{}"), 1L);
        SegmentReader r = SegmentReader.open(w.toByteArray(1L));
        // ⚠️ One entry is where `lo <= hi` versus `lo < hi` diverges.
        assertThat(r.find(new RunKey(A, 0))).isPresent();
        assertThat(r.find(new RunKey(B, 0))).isEmpty();
    }

    @Test
    void anEmptySegmentHasNoRunsAndFindsNothing() throws Exception {
        SegmentReader r = SegmentReader.open(new SegmentWriter().toByteArray(1L));
        assertThat(r.directory()).isEmpty();
        assertThat(r.find(new RunKey(A, 0))).isEmpty();
    }

    @Test
    void aTruncatedSegmentIsRefusedRatherThanPartlyRead() throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(new RunKey(A, 0), index("a", 1, "{\"n\":1}"), 1L);
        byte[] whole = w.toByteArray(1L);
        byte[] cut = java.util.Arrays.copyOf(whole, whole.length - 4);
        // ⚠️ A truncated object still has a VALID PREAMBLE. Only the footer says
        // the writer got to the end, which is why it repeats the header location
        // behind its own magic.
        assertThatThrownBy(() -> SegmentReader.open(cut))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("truncated");
    }

    @Test
    void aCorruptDirectoryIsRefused() throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(new RunKey(A, 0), index("a", 1, "{}"), 1L);
        byte[] seg = w.toByteArray(1L);
        seg[SegmentFormat.PREAMBLE_BYTES + 20] ^= 0x7F;   // flip a bit in recordCount
        assertThatThrownBy(() -> SegmentReader.open(seg))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("checksum");
    }

    @Test
    void aCorruptRunBodyIsRefused() throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(new RunKey(A, 0), index("a", 1, "{\"n\":1}"), 1L);
        byte[] seg = w.toByteArray(1L);
        int dataStart = SegmentFormat.PREAMBLE_BYTES + SegmentFormat.DIRECTORY_ENTRY_BYTES;
        seg[dataStart + 12] ^= 0x40;
        SegmentReader r = SegmentReader.open(seg);
        assertThatThrownBy(() -> r.read(r.directory().get(0)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("checksum");
    }

    @Test
    void aBadMagicOrVersionIsRefused() throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(new RunKey(A, 0), index("a", 1, "{}"), 1L);
        byte[] wrongMagic = w.toByteArray(1L);
        wrongMagic[0] ^= (byte) 0xFF;
        assertThatThrownBy(() -> SegmentReader.open(wrongMagic))
                .isInstanceOf(IOException.class).hasMessageContaining("magic");

        byte[] wrongVersion = w.toByteArray(1L);
        wrongVersion[5] = 9;
        // ⚠️ REFUSE, never best-effort: a later version may reuse a field, so
        // "read what I recognise" silently misinterprets it.
        assertThatThrownBy(() -> SegmentReader.open(wrongVersion))
                .isInstanceOf(IOException.class).hasMessageContaining("version");
    }

    @Test
    void aRunPointingOutsideTheSegmentIsRefusedBeforeAnythingIsAllocated() throws Exception {
        SegmentWriter w = new SegmentWriter();
        w.add(new RunKey(A, 0), index("a", 1, "{}"), 1L);
        byte[] seg = w.toByteArray(1L);
        int e = SegmentFormat.PREAMBLE_BYTES;
        java.nio.ByteBuffer.wrap(seg).order(java.nio.ByteOrder.BIG_ENDIAN)
                .putInt(e + 32, Integer.MAX_VALUE);   // byteLen
        // the directory CRC now fails first, which is itself the right refusal
        assertThatThrownBy(() -> SegmentReader.open(seg)).isInstanceOf(IOException.class);
    }

    @Test
    void aSegmentShorterThanItsOwnFramingIsRefused() {
        assertThatThrownBy(() -> SegmentReader.open(new byte[8]))
                .isInstanceOf(IOException.class).hasMessageContaining("shorter");
    }
}
