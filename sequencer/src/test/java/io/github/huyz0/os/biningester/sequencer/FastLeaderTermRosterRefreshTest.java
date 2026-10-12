// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A term's own roster writes are seen at once, not a refresh interval later
 * (M13.33m; M13.27q review round 1, P1): a join, a departure or a decision
 * written within {@code min_upload_interval} of a read changes what the term
 * answers and how it numbers its next decision.
 *
 * <p>⚠️ **THE CACHE OUTLIVED THE TERM's OWN WRITES**: a pod joined within the
 * interval had its COMMIT refused as unrostered, a departed one was still
 * assigned, and a decision written by the takeover was numbered over.
 */
class FastLeaderTermRosterRefreshTest {

    private static void read(FastLeaderTerm term) throws Exception {
        term.answerHeld(new FastFrame.Header(FastFrame.KIND_HELD, 3,
                FastLeaderTermCommitTest.POD.podUid(), "uid-self"),
                new FastFrame.HeldReport(FastFrame.Held.NONE));
    }

    private static FastFrame.Body commitFromPod(FastLeaderTerm term) throws Exception {
        return term.answerCommit(FastLeaderTermCommitTest.header(3, FastLeaderTermCommitTest.POD),
                commit(FastLeaderTermCommitTest.POD.podId(), 1, S1),
                FastLeaderTermCommitTest.recorded(FastWriteLeaderTest.leader(1 << 20, 1_000)));
    }

    @Test
    void aJOINWithinTheIntervalIsSeenAtOnce() throws Exception {
        FastLeaderTerm term = FastLeaderTermCommitTest.term3(new MemoryBinStore());
        read(term);

        FastLeaderTermCommitTest.join(term);

        assertThat(commitFromPod(term)).as("the pod is rostered now")
                .isInstanceOf(FastWriteFrame.Assigned.class);
    }

    @Test
    void aDEPARTUREWithinTheIntervalIsSeenAtOnce() throws Exception {
        FastLeaderTerm term = FastLeaderTermCommitTest.term3(new MemoryBinStore());
        FastLeaderTermCommitTest.join(term);
        read(term);

        term.answerDepart(new FastFrame.Header(FastFrame.KIND_DEPART, 3,
                FastLeaderTermCommitTest.POD.podUid(), "uid-self"),
                new FastFrame.Depart(FastLeaderTermCommitTest.POD, 2, FastFrame.Held.NONE));

        assertThat(((FastFrame.Refused) commitFromPod(term)).reason())
                .as("a departed pod is assigned nothing").isEqualTo(FastFrame.Reason.NOT_ROSTERED);
    }

    @Test
    void aDECISIONTheTakeoverWroteIsNumberedOverNever() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FastLeaderTerm term = FastLeaderTermCommitTest.term3(store);
        read(term);
        assertThat(term.nextDecision()).isZero();

        new RosterDecisions(store, "p", 3).record(Map.of(S1, 5L));
        term.rosterWritten();

        assertThat(term.nextDecision()).as("decision 0 is taken").isEqualTo(1);
    }
}
