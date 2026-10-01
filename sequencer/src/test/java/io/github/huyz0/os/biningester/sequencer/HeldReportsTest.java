// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A holder's HELD report and what the leader's HELD_STATUS lets it drop
 * (ADR-0081 §5.7, §9; M13.26f).
 */
class HeldReportsTest {

    static final RunKey A = new RunKey(new UUID(1, 1), 0);
    static final RunKey B = new RunKey(new UUID(1, 2), 0);

    static FastJournalRecord.Entry entry(RunKey key, long epoch, long assignedAfter, long first,
            int count) {
        List<SegmentRecord> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(new SegmentRecord("d" + first + "-" + i, OpType.INDEX,
                    OptionalLong.empty(), new byte[] {1}));
        }
        return new FastJournalRecord.Entry(epoch, key, first, 2, assignedAfter,
                new FastJournalRecord.IdempotencyKey("pod", new UUID(9, 9), first), records);
    }

    static FastJournal journal(FastJournalRecord.Entry... entries) throws Exception {
        FastJournal journal = FastJournal.recover(new MemoryJournalFile(), 1 << 20);
        for (FastJournalRecord.Entry e : entries) {
            journal.append(e);
        }
        journal.sync();
        return journal;
    }

    @Test
    void theREPORTGroupsEntriesByStreamAndTermAndSpansTheirOffsets() throws Exception {
        FastJournal journal = journal(entry(A, 7, 0, 10, 3), entry(B, 7, 0, 0, 1),
                entry(A, 7, 0, 13, 2), entry(A, 7, 2, 15, 1));

        FastFrame.Held held = HeldReports.of(journal.held());

        assertThat(held).isEqualTo(new FastFrame.Held(List.of(
                new FastFrame.HeldStream(A, List.of(new FastFrame.HeldGroup(7, 0, 10, 14),
                        new FastFrame.HeldGroup(7, 2, 15, 15))),
                new FastFrame.HeldStream(B, List.of(new FastFrame.HeldGroup(7, 0, 0, 0))))));
    }

    @Test
    void anANSWERReleasesTheCommittedAndDropsWhatIsSettledKeepingWhatIsPending()
            throws Exception {
        FastJournal journal = journal(entry(A, 6, 0, 0, 2), entry(A, 7, 0, 2, 2),
                entry(A, 7, 1, 4, 2), entry(A, 8, 0, 6, 2), entry(A, 8, 0, 8, 2),
                entry(B, 8, 0, 0, 2));
        FastFrame.HeldStatus status = new FastFrame.HeldStatus(List.of(
                new FastFrame.StreamStatus(A, 6, List.of(
                        new FastFrame.GroupStatus(6, 0, Long.MAX_VALUE,
                                FastFrame.Status.CLOSED_TERM),
                        new FastFrame.GroupStatus(7, 0, 2,
                                FastFrame.Status.SUPERSEDED),
                        new FastFrame.GroupStatus(7, 1, Long.MAX_VALUE,
                                FastFrame.Status.COMMITTED),
                        new FastFrame.GroupStatus(8, 0, 8, FastFrame.Status.PENDING))),
                new FastFrame.StreamStatus(B, 0, List.of(new FastFrame.GroupStatus(8, 0,
                        Long.MAX_VALUE, FastFrame.Status.PENDING)))));

        boolean pending = HeldReports.apply(journal, status);

        assertThat(pending).isTrue();
        assertThat(journal.held()).extracting(e -> e.key() + "@" + e.firstOffset())
                .as("A's term-8 group kept below the decision superseding it from 8; B kept")
                .containsExactlyInAnyOrder(A + "@6", B + "@0");
    }

    @Test
    void anANSWERWithNothingPendingSaysSo() throws Exception {
        FastJournal journal = journal(entry(A, 7, 0, 0, 2));

        boolean pending = HeldReports.apply(journal, new FastFrame.HeldStatus(List.of(
                new FastFrame.StreamStatus(A, 2, List.of(new FastFrame.GroupStatus(7, 0,
                        Long.MAX_VALUE, FastFrame.Status.COMMITTED))))));

        assertThat(pending).isFalse();
        assertThat(journal.held()).isEmpty();
    }
}
