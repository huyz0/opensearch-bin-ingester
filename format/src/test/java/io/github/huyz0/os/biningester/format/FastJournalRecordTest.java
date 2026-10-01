// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;

/**
 * The fast journal's records (M13.24, ADR-0082 §4).
 *
 * <p>⚠️ THE GOLDEN FILES CAME FROM AN INDEPENDENT ENCODER of the ADR's layout,
 * whose CRC-32C was checked against the standard check value first, so a
 * disagreement here is with the decision record.
 *
 * <p>⚠️ RECOVERY STOPS; IT NEVER SKIPS. A record that fails its length, its
 * checksum or its parse ends the read there, and everything after it is
 * reported as not valid -- the caller truncates the file to
 * {@code validLength} before it appends again (ADR-0082 §4), because an entry
 * appended after a tear would be lost to the next recovery.
 */
class FastJournalRecordTest {

    private static final RunKey KEY = new RunKey(
            new UUID(0x0123456789ABCDEFL, -0x0123456789ABCDF0L), 3);
    private static final FastJournalRecord.IdempotencyKey IDEM = new FastJournalRecord.IdempotencyKey(
            "pod-a", new UUID(0x1111111111111111L, 0x2222222222222222L), 99);

    private static FastJournalRecord.Entry entry() {
        return new FastJournalRecord.Entry(7, KEY, 4242, 2, 5, IDEM, List.of(
                new SegmentRecord("doc-1", OpType.INDEX, OptionalLong.empty(),
                        "{\"a\":1}".getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                new SegmentRecord("doc-2", OpType.DELETE, OptionalLong.of(17), new byte[0])));
    }

    private static byte[] golden(String name) throws IOException {
        try (var in = FastJournalRecordTest.class.getResourceAsStream("/golden/" + name)) {
            assertThat(in).as("missing golden file %s", name).isNotNull();
            return in.readAllBytes();
        }
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] p : parts) {
            out.writeBytes(p);
        }
        return out.toByteArray();
    }

    @Test
    void eachKINDEncodesToItsStoredBytes() throws Exception {
        assertThat(entry().encode()).isEqualTo(golden("fast-journal-entry-v1.bin"));
        assertThat(new FastJournalRecord.Release(KEY, 4244).encode())
                .isEqualTo(golden("fast-journal-release-v1.bin"));
        assertThat(new FastJournalRecord.Drop(KEY, 7, 5, 4243).encode())
                .isEqualTo(golden("fast-journal-drop-v1.bin"));
    }

    @Test
    void theSTOREDRecordsDecodeInOrderAndReEncodeToThemselves() throws Exception {
        byte[] file = concat(golden("fast-journal-entry-v1.bin"),
                golden("fast-journal-release-v1.bin"), golden("fast-journal-drop-v1.bin"));

        FastJournalRecord.Recovered read = FastJournalRecord.decodeAll(file);

        assertThat(read.validLength()).isEqualTo(file.length);
        assertThat(read.records()).hasSize(3);
        FastJournalRecord.Entry e = (FastJournalRecord.Entry) read.records().get(0);
        assertThat(e.epoch()).isEqualTo(7);
        assertThat(e.key()).isEqualTo(KEY);
        assertThat(e.firstOffset()).isEqualTo(4242);
        assertThat(e.walQuorum()).isEqualTo(2);
        assertThat(e.assignedAfter()).isEqualTo(5);
        assertThat(e.idempotencyKey()).isEqualTo(IDEM);
        assertThat(e.records()).extracting(SegmentRecord::id).containsExactly("doc-1", "doc-2");
        assertThat(e.records().get(1).version()).hasValue(17);
        assertThat(read.records().get(1)).isEqualTo(new FastJournalRecord.Release(KEY, 4244));
        assertThat(read.records().get(2)).isEqualTo(new FastJournalRecord.Drop(KEY, 7, 5, 4243));
        assertThat(concat(read.records().stream().map(FastJournalRecord::encode)
                .toArray(byte[][]::new))).isEqualTo(file);
    }

    @Test
    void aTORNTailStopsTheReadAtTheLastWholeRecord() throws Exception {
        byte[] whole = concat(golden("fast-journal-entry-v1.bin"),
                golden("fast-journal-release-v1.bin"));
        byte[] drop = golden("fast-journal-drop-v1.bin");

        for (int cut = 1; cut < drop.length; cut++) {
            FastJournalRecord.Recovered read = FastJournalRecord.decodeAll(
                    concat(whole, Arrays.copyOf(drop, cut)));

            assertThat(read.records()).as("torn after %d bytes", cut).hasSize(2);
            assertThat(read.validLength()).as("torn after %d bytes", cut).isEqualTo(whole.length);
        }
    }

    @Test
    void aCHECKSUMMismatchStopsTheReadThereAndNothingAfterItIsTaken() throws Exception {
        byte[] entry = golden("fast-journal-entry-v1.bin");
        byte[] release = golden("fast-journal-release-v1.bin");
        byte[] file = concat(entry, release, golden("fast-journal-drop-v1.bin"));
        file[entry.length + release.length - 1] ^= 1;

        FastJournalRecord.Recovered read = FastJournalRecord.decodeAll(file);

        assertThat(read.records())
                .as("the release is damaged; the whole drop after it is NOT read -- a reader "
                        + "that skipped would hold entries past a hole")
                .hasSize(1);
        assertThat(read.validLength()).isEqualTo(entry.length);
    }

    @Test
    void aBADMagicVersionOrLengthStopsTheRead() throws Exception {
        byte[] entry = golden("fast-journal-entry-v1.bin");
        int[][] damage = {{0, 0x00}, {4, 2}, {5, 0x7F}};
        for (int[] d : damage) {
            byte[] file = concat(entry, golden("fast-journal-release-v1.bin"));
            file[d[0]] = (byte) d[1];

            assertThat(FastJournalRecord.decodeAll(file).records())
                    .as("byte %d set to %d", d[0], d[1]).isEmpty();
            assertThat(FastJournalRecord.decodeAll(file).validLength()).isZero();
        }
    }

    @Test
    void aRecordWithAGOODChecksumButAnUnknownKindStopsTheRead() throws Exception {
        byte[] rest = {9, 0, 0, 0};
        CRC32C crc = new CRC32C();
        crc.update(rest);
        byte[] forged = ByteBuffer.allocate(FastJournalRecord.HEADER_BYTES + rest.length)
                .putInt(FastJournalRecord.MAGIC).put((byte) 1).putInt(rest.length)
                .putInt((int) crc.getValue()).put(rest).array();
        byte[] entry = golden("fast-journal-entry-v1.bin");

        FastJournalRecord.Recovered read = FastJournalRecord.decodeAll(concat(entry, forged, entry));

        assertThat(read.records()).as("a kind this build does not know ends the read").hasSize(1);
        assertThat(read.validLength()).isEqualTo(entry.length);
    }

    @Test
    void anEntryOutsideItsFIELDRangesIsRefused() {
        List<SegmentRecord> one = entry().records();
        assertThatThrownBy(() -> new FastJournalRecord.Entry(7, KEY, 0, 4, 0, IDEM, one))
                .as("walQuorum 4").isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FastJournalRecord.Entry(7, KEY, 0, 2, 1L << 32, IDEM, one))
                .as("assignedAfter past u32").isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FastJournalRecord.Entry(7, KEY, -1, 2, 0, IDEM, one))
                .as("a negative offset").isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FastJournalRecord.Entry(7, KEY, 0, 2, 0, IDEM, List.of()))
                .as("an entry with no records holds nothing").isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FastJournalRecord.Drop(KEY, 7, -1, 0))
                .as("a drop's assignedAfter is a u32 too").isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anEntrySPANSItsRecordCount() {
        FastJournalRecord.Entry e = entry();

        assertThat(e.endOffset()).as("firstOffset plus the record count").isEqualTo(4244);
    }
}
