// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.ALL;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S1;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S2;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.WA;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.roster;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A term's publication at its leader (ADR-0081 §6; M13.30d): each exposed
 * entry owed once to each pod interested in its stream, in offset order per
 * stream, a stream removed from a pod's interest owed nothing, and every
 * exposure step of the term's desk told to it.
 */
class PublishDeskTest {

    private static final Roster.Incarnation A = FastTermStartTest.incarnation("a");
    private static final Roster.Incarnation B = FastTermStartTest.incarnation("b");

    private static FastJournalRecord.Entry entry(RunKey stream, long offset) {
        return new FastJournalRecord.Entry(3, stream, offset, 1, 0,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), offset),
                List.of(new SegmentRecord("d" + offset, OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1})));
    }

    @Test
    void anEXPOSEDEntryIsOwedOnceToEachInterestedPodInOffsetOrderPerStream() {
        PublishDesk desk = new PublishDesk();
        desk.interest(A, List.of(S1), List.of());
        desk.interest(B, List.of(S2), List.of());

        desk.exposed(List.of(entry(S1, 5), entry(S2, 0), entry(S1, 3)));
        var owed = desk.take();

        assertThat(owed.get(A)).extracting(FastJournalRecord.Entry::firstOffset)
                .containsExactly(3L, 5L);
        assertThat(owed.get(B)).extracting(FastJournalRecord.Entry::firstOffset)
                .containsExactly(0L);
        assertThat(desk.take()).as("taken once").isEmpty();
    }

    @Test
    void aSTREAMRemovedFromAPodsInterestIsOwedNothing() {
        PublishDesk desk = new PublishDesk();
        desk.interest(A, List.of(S1, S2), List.of());
        desk.interest(A, List.of(), List.of(S1));

        desk.exposed(List.of(entry(S1, 5), entry(S2, 0)));

        assertThat(desk.take().get(A)).extracting(FastJournalRecord.Entry::key)
                .containsExactly(S2);
    }

    @Test
    void theDESKTellsEachExposureStepOnce() throws Exception {
        FastWriteLeader l = FastWriteLeaderTest.leader(1 << 20, 1_000).leader();
        List<List<FastJournalRecord.Entry>> steps = new ArrayList<>();
        l.publishTo(steps::add);
        l.commit(WA, commit(1, S1), roster(), ALL);
        l.flushGroup();

        l.expose();
        l.expose();

        assertThat(steps).as("one step, its batch's entry").singleElement()
                .satisfies(step -> assertThat(step).extracting(FastJournalRecord.Entry::key)
                        .containsExactly(S1));
    }

    @Test
    void aCOMMITDesksExposureReachesItsPublications() throws Exception {
        FastWriteLeader l = FastWriteLeaderTest.leader(1 << 20, 1_000).leader();
        CommitDesk desk = new CommitDesk(l, stream -> 0L, Duration.ofSeconds(1));
        desk.publications().interest(A, List.of(S1), List.of());
        l.commit(WA, commit(1, S1), roster(), ALL);
        l.flushGroup();

        desk.exposures();

        assertThat(desk.publications().take().get(A)).hasSize(1);
    }
}
