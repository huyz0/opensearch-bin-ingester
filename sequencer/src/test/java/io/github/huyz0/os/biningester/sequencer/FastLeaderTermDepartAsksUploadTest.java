// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastLeaderTermDepartTest.POD;
import static io.github.huyz0.os.biningester.sequencer.FastLeaderTermDepartTest.header;
import static io.github.huyz0.os.biningester.sequencer.FastLeaderTermDepartTest.term3;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile;
import io.github.huyz0.os.biningester.format.FastFrame;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A departing pod's DEPART phase 1 asks the leader for an upload at once
 * (ADR-0081 §7, §9; M13.31c), so its pending copies are committed inside its
 * grace rather than at the next {@code flush_timer}.
 */
class FastLeaderTermDepartAsksUploadTest {

    @Test
    void aPHASE1DepartMakesAnUploadDueAtOnce() throws Exception {
        FastLeaderTerm term = term3(new MemoryBinStore(), Map.of());
        CommitDesk desk = term.commitDesk(
                FastJournal.recover(new MemoryJournalFile(), 1 << 20), FastTermStartTest.SELF);
        desk.uploaded(1_000);
        assertThat(desk.uploadDue(2_000, Duration.ofHours(1))).as("the premise").isFalse();

        term.answerDepart(header(FastFrame.KIND_DEPART, 3),
                new FastFrame.Depart(POD, 1, FastFrame.Held.NONE));

        assertThat(desk.uploadDue(2_000, Duration.ofHours(1))).isTrue();
    }
}
