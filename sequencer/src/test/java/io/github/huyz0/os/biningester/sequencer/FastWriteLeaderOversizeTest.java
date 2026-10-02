// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A run of more than {@code B} records is the writer's error, refused, never
 * held to wait for ever (M13.27c; M13.27's review round 2, P5).
 */
class FastWriteLeaderOversizeTest {

    @Test
    void aRUNOfMoreThanBRecordsIsRefusedNotHeld() throws Exception {
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(1 << 20, 2);
        SegmentRecord r = new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(),
                new byte[] {1});
        FastWriteFrame.Commit three = new FastWriteFrame.Commit(
                new FastJournalRecord.IdempotencyKey("pod", new UUID(9, 9), 1),
                List.of(new FastWriteFrame.CommitRun(FastWriteLeaderTest.S1, List.of(r, r, r))));

        assertThatThrownBy(() -> l.leader().commit(FastWriteLeaderTest.B, three,
                FastWriteLeaderTest.roster(), FastWriteLeaderTest.ALL))
                .as("three records, B = 2: it could never fit")
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(l.cursor().cursor(FastWriteLeaderTest.S1)).isEqualTo(100);
    }
}
