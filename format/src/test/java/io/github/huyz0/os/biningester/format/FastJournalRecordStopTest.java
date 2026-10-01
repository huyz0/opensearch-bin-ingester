// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;

/**
 * A tear and an unreadable record are different stops (M13.24 review round 1,
 * P1 and T5).
 *
 * <p>⚠️ ONLY A TEAR MAY BE CUT. A record whose checksum verifies was fsynced by
 * some build, so its copies may have been answered; reading it as a tear
 * would let recovery truncate it -- after a rollback, every answered copy on
 * the pod erased while its UID lives, invisibly to the quorum-loss predicate.
 */
class FastJournalRecordStopTest {

    private static byte[] golden(String name) throws IOException {
        try (var in = FastJournalRecordStopTest.class.getResourceAsStream("/golden/" + name)) {
            assertThat(in).isNotNull();
            return in.readAllBytes();
        }
    }

    private static byte[] forge(int version, byte[] rest) {
        CRC32C crc = new CRC32C();
        crc.update(rest);
        return ByteBuffer.allocate(FastJournalRecord.HEADER_BYTES + rest.length)
                .putInt(FastJournalRecord.MAGIC).put((byte) version).putInt(rest.length)
                .putInt((int) crc.getValue()).put(rest).array();
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] p : parts) {
            out.writeBytes(p);
        }
        return out.toByteArray();
    }

    private static byte[] restOf(byte[] record) {
        return Arrays.copyOfRange(record, FastJournalRecord.HEADER_BYTES, record.length);
    }

    @Test
    void aTEARIsNotUnreadable() throws Exception {
        byte[] entry = golden("fast-journal-entry-v1.bin");
        byte[] release = golden("fast-journal-release-v1.bin");

        FastJournalRecord.Recovered read = FastJournalRecord.decodeAll(
                concat(entry, Arrays.copyOf(release, release.length - 1)));

        assertThat(read.unreadable()).as("a length past the end is a tear: cut it").isFalse();
        assertThat(read.validLength()).isEqualTo(entry.length);
    }

    @Test
    void aCHECKSUMFailureIsATearNotUnreadable() throws Exception {
        byte[] entry = golden("fast-journal-entry-v1.bin");
        byte[] release = golden("fast-journal-release-v1.bin");
        release[release.length - 1] ^= 1;

        assertThat(FastJournalRecord.decodeAll(concat(entry, release)).unreadable()).isFalse();
    }

    @Test
    void aWHOLERecordOfAnUnknownVersionIsUnreadable() throws Exception {
        byte[] entry = golden("fast-journal-entry-v1.bin");
        byte[] future = forge(2, restOf(golden("fast-journal-release-v1.bin")));

        FastJournalRecord.Recovered read = FastJournalRecord.decodeAll(concat(entry, future));

        assertThat(read.unreadable())
                .as("its checksum verifies: a later build wrote it, and it may have been answered")
                .isTrue();
        assertThat(read.validLength()).isEqualTo(entry.length);
    }

    @Test
    void aWHOLERecordOfAnUnknownKindIsUnreadable() throws Exception {
        assertThat(FastJournalRecord.decodeAll(forge(1, new byte[] {9, 0, 0})).unreadable())
                .isTrue();
    }

    @Test
    void aWHOLEReleaseOrDropWithTrailingBytesIsUnreadable() throws Exception {
        for (String name : new String[] {"fast-journal-release-v1.bin", "fast-journal-drop-v1.bin"}) {
            byte[] rest = restOf(golden(name));
            byte[] longer = Arrays.copyOf(rest, rest.length + 1);

            FastJournalRecord.Recovered read = FastJournalRecord.decodeAll(forge(1, longer));

            assertThat(read.records()).as(name).isEmpty();
            assertThat(read.unreadable())
                    .as("%s with a byte after its last field is a shape this build does not "
                            + "know, not a release or drop to apply", name)
                    .isTrue();
        }
    }
}
