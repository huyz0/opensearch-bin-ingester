// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastTakeoverFrame;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A holder answers a RELEASE (M13.28b; ADR-0081 §5 step 7): from the leader
 * of a term it joined, each named stream's entries wholly below its offset
 * are released -- they are committed -- and the RELEASE answered by an empty
 * HELD_STATUS; from anyone else, refused and nothing released.
 */
class ReleaseAnswerTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);
    private static final RunKey B = new RunKey(new UUID(2, 2), 0);

    private static FastJournalRecord.Entry entry(RunKey key, long first, int count) {
        List<SegmentRecord> records = new java.util.ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(new SegmentRecord("d" + first + "-" + i, OpType.INDEX,
                    OptionalLong.empty(), new byte[] {1}));
        }
        return new FastJournalRecord.Entry(3, key, first, 2, 0,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), first), records);
    }

    private static FastJournal holding() throws Exception {
        FastJournal journal = FastJournal.recover(new MemoryJournalFile(), 1 << 20);
        for (FastJournalRecord.Entry e : List.of(entry(A, 0, 2), entry(A, 5, 1),
                entry(B, 0, 1))) {
            assertThat(journal.append(e)).isTrue();
        }
        journal.sync();
        return journal;
    }

    private static JoinedTerms joined() {
        JoinedTerms terms = new JoinedTerms();
        terms.joined(3, 0, "uid-l");
        return terms;
    }

    private static FastFrame.Header from(String sender) {
        return new FastFrame.Header(FastTakeoverFrame.KIND_RELEASE, 3, sender, "uid-h");
    }

    @Test
    void aRELEASEFromItsTermsLeaderReleasesEachStreamBelowItsOffset() throws Exception {
        FastJournal journal = holding();

        FastFrame.Body answer = new ReleaseAnswer(journal, joined()).answer(from("uid-l"),
                new FastTakeoverFrame.Release(List.of(new FastTakeoverFrame.ReleaseAt(A, 5),
                        new FastTakeoverFrame.ReleaseAt(B, 1))));

        assertThat(answer).isEqualTo(new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
        assertThat(journal.held()).extracting(e -> e.key() + "@" + e.firstOffset())
                .containsExactly(A + "@5");
    }

    @Test
    void aRELEASEFromAnyoneElseIsRefusedAndReleasesNothing() throws Exception {
        FastJournal journal = holding();

        FastFrame.Body answer = new ReleaseAnswer(journal, joined()).answer(from("uid-x"),
                new FastTakeoverFrame.Release(List.of(new FastTakeoverFrame.ReleaseAt(A, 9))));

        assertThat(((FastFrame.Refused) answer).reason())
                .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        assertThat(journal.held()).hasSize(3);
    }

    @Test
    void anENTRYStraddlingTheOffsetIsKept() throws Exception {
        FastJournal journal = holding();

        new ReleaseAnswer(journal, joined()).answer(from("uid-l"),
                new FastTakeoverFrame.Release(List.of(new FastTakeoverFrame.ReleaseAt(A, 1))));

        assertThat(journal.held()).extracting(e -> e.key() + "@" + e.firstOffset())
                .as("A@0 runs to 2: not wholly below 1").contains(A + "@0");
    }
}
