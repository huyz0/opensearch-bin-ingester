// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The entry's quorum bound at decode (M13.24 review round 2, T7).
 */
class FastJournalRecordBoundsTest {

    private static FastJournalRecord.Entry entry(int quorum) {
        return new FastJournalRecord.Entry(7, new RunKey(new UUID(1, 2), 3), 40, quorum, 0,
                new FastJournalRecord.IdempotencyKey("pod-a", new UUID(9, 9), 1),
                List.of(new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(), new byte[] {1})));
    }

    @Test
    void anEntryAtQUORUMThreeRoundTrips() {
        FastJournalRecord.Entry e = entry(3);

        FastJournalRecord.Recovered read = FastJournalRecord.decodeAll(e.encode());

        assertThat(read.unreadable()).isFalse();
        assertThat(((FastJournalRecord.Entry) read.records().get(0)).walQuorum()).isEqualTo(3);
    }
}
