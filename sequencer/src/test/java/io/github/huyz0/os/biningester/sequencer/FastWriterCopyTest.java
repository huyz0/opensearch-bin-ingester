// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The writer's own copy and its CONFIRM (M13.29a review round 1, T1-T3): the
 * journaled entry is exactly the batch's run at its assigned offset, a
 * diskless writer confirms it not journaled, and a CONFIRM refused, fenced
 * or answered by another kind never counts as written.
 */
class FastWriterCopyTest {

    private static final RunKey S = FastWriterTest.S;
    private static final RunKey T = new RunKey(new UUID(2, 2), 0);

    /** Answers a COMMIT with {@code assigned} and a CONFIRM with {@code confirmed}. */
    private static final class Leader implements TermJoiner.Transport {
        final List<FastFrame.Body> sent = new ArrayList<>();
        FastWriteFrame.Assigned assigned;
        FastFrame.Body confirmed = new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE);
        long confirmEpoch = 7;
        ExposedInbox inbox;

        @Override
        public byte[] exchange(String endpoint, byte[] frame) throws IOException {
            FastFrame.Body body = FastFrame.decode(frame).body();
            sent.add(body);
            if (body instanceof FastWriteFrame.Confirm) {
                inbox.deliver(7, new FastWriteFrame.Exposed(assigned.assignedAfter(),
                        assigned.runs().stream().map(r -> new FastWriteFrame.RunAt(r.stream(),
                                r.firstOffset())).toList()));
                return FastFrame.encode(confirmEpoch, "uid-l", "uid-w", confirmed);
            }
            return FastFrame.encode(7, "uid-l", "uid-w", assigned);
        }
    }

    private static SegmentRecord record(String id) {
        return new SegmentRecord(id, OpType.INDEX, OptionalLong.empty(), new byte[] {1});
    }

    private static FastWriteFrame.Commit twoRuns() {
        return new FastWriteFrame.Commit(new FastJournalRecord.IdempotencyKey("w",
                new UUID(9, 9), 5), List.of(
                        new FastWriteFrame.CommitRun(S, List.of(record("s1"), record("s2"))),
                        new FastWriteFrame.CommitRun(T, List.of(record("t1")))));
    }

    private static FastWriteFrame.Assigned bothRequired() {
        return new FastWriteFrame.Assigned(3, List.of(
                new FastWriteFrame.AssignedRun(S, 100, 2, true),
                new FastWriteFrame.AssignedRun(T, 40, 3, true)));
    }

    private static FastWriter writer(Leader leader, Optional<FastJournal> journal)
            throws Exception {
        leader.inbox = new ExposedInbox();
        return new FastWriter(FastWriterTest.SELF, leader, FastWriterTest.fenceAt(7), journal,
                leader.inbox, Duration.ofMillis(200));
    }

    @Test
    void theJOURNALEDCopyIsEachRunAtItsOwnOffsetQuorumAndRecords() throws Exception {
        FastJournal journal = FastJournal.recover(
                new io.github.huyz0.os.biningester.binstore.backend.MemoryJournalFile(), 1 << 20);
        Leader leader = new Leader();
        leader.assigned = bothRequired();

        writer(leader, Optional.of(journal)).write(7, "uid-l", "http://l:1", twoRuns());

        assertThat(journal.held()).hasSize(2).allSatisfy(e -> {
            assertThat(e.epoch()).isEqualTo(7);
            assertThat(e.assignedAfter()).isEqualTo(3);
            assertThat(e.idempotencyKey()).isEqualTo(twoRuns().key());
        });
        FastJournalRecord.Entry s = journal.held().stream().filter(e -> e.key().equals(S))
                .findFirst().orElseThrow();
        FastJournalRecord.Entry t = journal.held().stream().filter(e -> e.key().equals(T))
                .findFirst().orElseThrow();
        assertThat(s.firstOffset()).isEqualTo(100);
        assertThat(s.walQuorum()).isEqualTo(2);
        assertThat(s.records()).extracting(SegmentRecord::id).containsExactly("s1", "s2");
        assertThat(t.firstOffset()).isEqualTo(40);
        assertThat(t.walQuorum()).isEqualTo(3);
        assertThat(t.records()).extracting(SegmentRecord::id).containsExactly("t1");
    }

    @Test
    void aDISKLESSWriterConfirmsItsCopyNotJournaled() throws Exception {
        Leader leader = new Leader();
        leader.assigned = bothRequired();

        writer(leader, Optional.empty()).write(7, "uid-l", "http://l:1", twoRuns());

        assertThat(((FastWriteFrame.Confirm) leader.sent.get(1)).copyJournaled()).isFalse();
    }

    @Test
    void aREFUSEDConfirmIsNotWritten() throws Exception {
        Leader leader = new Leader();
        leader.assigned = bothRequired();
        leader.confirmed = new FastFrame.Refused(FastFrame.Reason.NOT_ROSTERED,
                Optional.empty(), "no");

        assertThat(writer(leader, Optional.empty()).write(7, "uid-l", "http://l:1", twoRuns()))
                .as("though its EXPOSED arrived").isInstanceOf(FastWriter.Rejoin.class);
    }

    @Test
    void aCONFIRMAnsweredByANewerTermIsFenced() throws Exception {
        Leader leader = new Leader();
        leader.assigned = bothRequired();
        leader.confirmEpoch = 8;

        assertThat(writer(leader, Optional.empty()).write(7, "uid-l", "http://l:1", twoRuns()))
                .isInstanceOf(FastWriter.Fenced.class);
    }

    @Test
    void aCONFIRMAnsweredByAnotherKindIsAnError() throws Exception {
        // ⚠️ M13.29a review round 1, P3: it counted as confirmed.
        Leader leader = new Leader();
        leader.assigned = bothRequired();
        leader.confirmed = bothRequired();

        assertThatThrownBy(() -> writer(leader, Optional.empty()).write(7, "uid-l",
                "http://l:1", twoRuns())).isInstanceOf(IOException.class);
    }

    @Test
    void aBOUNDPastAnIntSplitsNothingAndNoBoundIsRefused() {
        // ⚠️ M13.29a review round 1, P2: cast to an int, 2^32 never advanced.
        List<FastWriteFrame.CommitRun> runs = twoRuns().runs();

        assertThat(FastWriter.splitAtBound(runs, 1L << 32)).singleElement()
                .isEqualTo(runs);
        assertThatThrownBy(() -> FastWriter.splitAtBound(runs, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
