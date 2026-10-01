// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;

/**
 * A whole record whose fields the entry refuses is unreadable, not a crash of
 * the read (M13.24 review round 3, T3).
 */
class FastJournalRecordInvalidFieldsTest {

    @Test
    void aWHOLEEntryWithAQuorumOfNineIsUnreadable() {
        byte[] whole = new FastJournalRecord.Entry(7, new RunKey(new UUID(1, 2), 3), 40, 2, 0,
                new FastJournalRecord.IdempotencyKey("pod-a", new UUID(9, 9), 1),
                List.of(new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(), new byte[] {1})))
                .encode();
        byte[] rest = Arrays.copyOfRange(whole, FastJournalRecord.HEADER_BYTES, whole.length);
        // walQuorum follows kind (1), epoch (8), RunKey (20), firstOffset (8), count (4)
        rest[41] = 9;
        CRC32C crc = new CRC32C();
        crc.update(rest);
        byte[] forged = ByteBuffer.allocate(whole.length).putInt(FastJournalRecord.MAGIC)
                .put((byte) 1).putInt(rest.length).putInt((int) crc.getValue()).put(rest).array();

        FastJournalRecord.Recovered read = FastJournalRecord.decodeAll(forged);

        assertThat(read.unreadable())
                .as("the entry refuses quorum 9 with an IllegalArgumentException; the read must "
                        + "report it unreadable, not throw out of recovery")
                .isTrue();
    }
}
