// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.ALL;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.WA;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.roster;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A paced upload's triggers (ADR-0081 §7; M13.31c): the term's first at
 * once, then the tightest {@code flush_timer} since the last -- or sooner for
 * a stream at {@code B / 2} uncommitted offsets, the journal past half its
 * cap, or a departing pod's ask.
 */
class CommitDeskUploadTriggerTest {

    private static final Duration FIVE_SECONDS = Duration.ofSeconds(5);
    private static final long SECOND = 1_000_000_000L;

    private static CommitDesk desk(FastWriteLeader leader) {
        return new CommitDesk(leader, stream -> 0L, Duration.ofSeconds(1));
    }

    @Test
    void theFIRSTUploadIsDueAtOnceAndTheNextAfterTheFlushTimer() throws Exception {
        CommitDesk d = desk(FastWriteLeaderTest.leader(1 << 20, 1_000).leader());
        assertThat(d.uploadDue(0, FIVE_SECONDS)).as("the term's first").isTrue();

        d.uploaded(SECOND);

        assertThat(d.uploadDue(SECOND + 5 * SECOND - 1, FIVE_SECONDS)).isFalse();
        assertThat(d.uploadDue(SECOND + 5 * SECOND, FIVE_SECONDS)).isTrue();
    }

    @Test
    void aSTREAMAtHalfItsBoundIsDueBeforeTheFlushTimer() throws Exception {
        FastWriteLeader tight = FastWriteLeaderTest.leader(1 << 20, 4).leader();
        FastWriteLeader loose = FastWriteLeaderTest.leader(1 << 20, 1_000).leader();
        for (FastWriteLeader l : List.of(tight, loose)) {
            l.commit(WA, commit(1, S1), roster(), ALL);
            l.flushGroup();
        }
        CommitDesk t = desk(tight);
        CommitDesk l = desk(loose);
        t.uploaded(SECOND);
        l.uploaded(SECOND);

        assertThat(t.uploadDue(2 * SECOND, FIVE_SECONDS)).as("2 of B = 4 uncommitted").isTrue();
        assertThat(l.uploadDue(2 * SECOND, FIVE_SECONDS)).as("2 of B = 1,000").isFalse();
    }

    @Test
    void theJOURNALPastHalfTheLeadersShareIsDueBeforeTheFlushTimer() throws Exception {
        FastWriteLeaderTest.Leader l = FastWriteLeaderTest.leader(4_096, 1_000);
        CommitDesk d = desk(l.leader());
        d.uploaded(SECOND);
        assertThat(d.uploadDue(2 * SECOND, FIVE_SECONDS)).as("the premise").isFalse();

        l.journal().append(new FastJournalRecord.Entry(6, S1, 0, 1, 0,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), 0),
                List.of(new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(),
                        new byte[2_500]))));

        assertThat(l.journal().pastHalfLeaderShare()).as("the premise").isTrue();
        assertThat(d.uploadDue(2 * SECOND, FIVE_SECONDS)).isTrue();
    }

    @Test
    void anASKIsDueAtOnceAndClearedByTheUpload() throws Exception {
        CommitDesk d = desk(FastWriteLeaderTest.leader(1 << 20, 1_000).leader());
        d.uploaded(SECOND);
        d.askUpload();

        assertThat(d.uploadDue(SECOND + 1, FIVE_SECONDS)).isTrue();
        d.uploaded(SECOND + 2);
        assertThat(d.uploadDue(SECOND + 3, FIVE_SECONDS)).isFalse();
    }

    @Test
    void theJOURNALsTriggerIsHalfTheLeadersShareAQuarterOfTheCap() throws Exception {
        // ⚠️ review round 1, P1/T1: the leader's own entries stop at half the
        // cap, so half the WHOLE cap would never fire for them.
        FastJournalRecord.Entry entry = new FastJournalRecord.Entry(6, S1, 0, 1, 0,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), 0),
                List.of(new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(),
                        new byte[64])));
        FastJournal probe = FastJournal.recover(
                new io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile(), 1 << 20);
        probe.append(entry);
        long held = probe.heldBytes();
        FastJournal atQuarter = FastJournal.recover(
                new io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile(), 4 * held);
        FastJournal pastQuarter = FastJournal.recover(
                new io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile(),
                4 * held - 4);

        atQuarter.append(entry);
        pastQuarter.append(entry);

        assertThat(atQuarter.pastHalfLeaderShare()).as("exactly a quarter").isFalse();
        assertThat(pastQuarter.pastHalfLeaderShare()).as("a byte past it").isTrue();
        assertThat(pastQuarter.fitsLeaderShare(0)).as("and the leader's own share not full")
                .isTrue();
    }
}
