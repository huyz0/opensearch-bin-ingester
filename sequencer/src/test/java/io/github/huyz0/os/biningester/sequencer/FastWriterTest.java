// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

/**
 * The writer's side of a fast batch (M13.29a; ADR-0081 §2): COMMIT to the
 * term's leader, journal its own copy with the assigned offsets where one is
 * required, CONFIRM it, and answer only once the batch is EXPOSED.
 *
 * <p>⚠️ **NOTHING WROTE A FAST BATCH**: every frame of the write had a sender
 * on the leader and a holder, and none on the writer.
 */
class FastWriterTest {

    static final Roster.Incarnation SELF = new Roster.Incarnation("w", "uid-w", "az-b", "");
    static final RunKey S = new RunKey(new UUID(1, 1), 0);
    private static final String LEASE = "p/ctl/lease/0.json";

    /** Answers each frame by kind; records what was sent. */
    private static final class Leader implements TermJoiner.Transport {
        final List<FastFrame.Body> sent = new ArrayList<>();
        Function<FastFrame.Body, FastFrame.Body> answer;
        long answerEpoch = 7;
        String answerSender = "uid-l";
        Runnable onConfirm = () -> { };

        @Override
        public byte[] exchange(String endpoint, byte[] frame) throws IOException {
            FastFrame.Body body = FastFrame.decode(frame).body();
            sent.add(body);
            if (body instanceof FastWriteFrame.Confirm) {
                onConfirm.run();
                return FastFrame.encode(answerEpoch, answerSender, SELF.podUid(),
                        new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
            }
            return FastFrame.encode(answerEpoch, answerSender, SELF.podUid(), answer.apply(body));
        }
    }

    static EpochFence fenceAt(long epoch) throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        store.put(LEASE, Body.ofBytes(new Lease(epoch, "leader", "", 1_000).encode()));
        return EpochFence.start(store, LEASE, Optional.empty());
    }

    static FastWriteFrame.Commit commit(String pod) {
        return new FastWriteFrame.Commit(new FastJournalRecord.IdempotencyKey(pod,
                new UUID(9, 9), 1), List.of(new FastWriteFrame.CommitRun(S, List.of(
                        new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(),
                                new byte[] {1})))));
    }

    private static FastWriteFrame.Assigned assigned(boolean copyRequired) {
        return new FastWriteFrame.Assigned(3, List.of(
                new FastWriteFrame.AssignedRun(S, 100, 2, copyRequired)));
    }

    private static final FastWriteFrame.Exposed EXPOSED =
            new FastWriteFrame.Exposed(3, List.of(new FastWriteFrame.RunAt(S, 100)));

    private record Fixture(FastWriter writer, Leader leader, ExposedInbox inbox,
            FastJournal journal, FastWriteLeaderGroupTest.CountingFile file) {
    }

    private static Fixture fixture(long cap) throws Exception {
        FastWriteLeaderGroupTest.CountingFile file = new FastWriteLeaderGroupTest.CountingFile();
        FastJournal journal = FastJournal.recover(file, cap);
        Leader leader = new Leader();
        ExposedInbox inbox = new ExposedInbox();
        FastWriter writer = new FastWriter(SELF, leader, fenceAt(7), Optional.of(journal), inbox,
                Duration.ofMillis(200));
        return new Fixture(writer, leader, inbox, journal, file);
    }

    @Test
    void aBATCHWithNoCopyRequiredIsWrittenOnceExposed() throws Exception {
        Fixture f = fixture(1 << 20);
        f.leader().answer = commit -> assigned(false);
        f.inbox().deliver(7, EXPOSED);

        FastWriter.Outcome outcome = f.writer().write(7, "uid-l", "http://l:1", commit("w"));

        assertThat(outcome).isEqualTo(new FastWriter.Written(assigned(false)));
        assertThat(f.leader().sent).as("no copy, so no CONFIRM").hasSize(1);
        assertThat(f.journal().held()).isEmpty();
    }

    @Test
    void aREQUIREDCopyIsJournaledWithItsOffsetsAndSyncedBeforeItsConfirm() throws Exception {
        Fixture f = fixture(1 << 20);
        f.leader().answer = commit -> assigned(true);
        int[] forcesAtConfirm = {-1};
        f.leader().onConfirm = () -> {
            forcesAtConfirm[0] = f.file().forces;
            f.inbox().deliver(7, EXPOSED);
        };

        FastWriter.Outcome outcome = f.writer().write(7, "uid-l", "http://l:1", commit("w"));

        assertThat(outcome).isInstanceOf(FastWriter.Written.class);
        assertThat(f.journal().held()).singleElement().satisfies(e -> {
            assertThat(e.firstOffset()).isEqualTo(100);
            assertThat(e.epoch()).isEqualTo(7);
            assertThat(e.assignedAfter()).isEqualTo(3);
        });
        assertThat(forcesAtConfirm[0]).as("fsynced before the CONFIRM left").isPositive();
        assertThat(f.leader().sent.get(1)).isEqualTo(new FastWriteFrame.Confirm(
                commit("w").key(), 3, List.of(new FastWriteFrame.RunAt(S, 100)), true));
    }

    @Test
    void aJOURNALAtItsCapConfirmsTheCopyNotJournaled() throws Exception {
        Fixture f = fixture(1);
        f.leader().answer = commit -> assigned(true);
        f.leader().onConfirm = () -> f.inbox().deliver(7, EXPOSED);

        f.writer().write(7, "uid-l", "http://l:1", commit("w"));

        assertThat(((FastWriteFrame.Confirm) f.leader().sent.get(1)).copyJournaled()).isFalse();
        assertThat(f.journal().held()).isEmpty();
    }

    @Test
    void noEXPOSEDWithinTheWaitIsARetry() throws Exception {
        Fixture f = fixture(1 << 20);
        f.leader().answer = commit -> assigned(false);

        assertThat(f.writer().write(7, "uid-l", "http://l:1", commit("w")))
                .isInstanceOf(FastWriter.Retry.class);
    }

    @Test
    void eachREFUSALIsMappedToWhatTheWriterDoesNext() throws Exception {
        Fixture f = fixture(1 << 20);
        for (var c : List.of(
                new Object[] {FastFrame.Reason.BACKPRESSURE, FastWriter.Retry.class},
                new Object[] {FastFrame.Reason.DISCARDED, FastWriter.Retry.class},
                new Object[] {FastFrame.Reason.NOT_FAST, FastWriter.NotFast.class},
                new Object[] {FastFrame.Reason.NOT_ROSTERED, FastWriter.Rejoin.class},
                new Object[] {FastFrame.Reason.DEPARTING, FastWriter.Rejoin.class},
                new Object[] {FastFrame.Reason.LOWER_EPOCH, FastWriter.Fenced.class})) {
            FastFrame.Reason reason = (FastFrame.Reason) c[0];
            f.leader().answer = commit -> new FastFrame.Refused(reason,
                    reason == FastFrame.Reason.DISCARDED ? Optional.of(commit("w").key())
                            : Optional.empty(), "no");

            assertThat(f.writer().write(7, "uid-l", "http://l:1", commit("w")))
                    .as(reason.name()).isInstanceOf((Class<?>) c[1]);
        }
    }

    @Test
    void aCOMMITUnderAnotherPodsKeyIsNeverSent() throws Exception {
        // ⚠️ M13.52g, its review P2: the leader refuses it NOT_ROSTERED though
        // the sender IS rostered, so a writer that sent it would re-join and
        // resend the same COMMIT for ever.
        Fixture f = fixture(1 << 20);

        assertThatThrownBy(() -> f.writer().write(7, "uid-l", "http://l:1", commit("other")))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(f.leader().sent).isEmpty();
    }

    @Test
    void aTERMBelowTheFenceIsFencedWithoutSending() throws Exception {
        Fixture f = fixture(1 << 20);

        assertThat(f.writer().write(6, "uid-l", "http://l:1", commit("w")))
                .isInstanceOf(FastWriter.Fenced.class);
        assertThat(f.leader().sent).isEmpty();
    }

    @Test
    void anANSWERFromAnotherPodOrTermIsRefused() throws Exception {
        Fixture f = fixture(1 << 20);
        f.leader().answer = commit -> assigned(false);
        f.leader().answerSender = "uid-x";
        assertThatThrownBy(() -> f.writer().write(7, "uid-l", "http://l:1", commit("w")))
                .isInstanceOf(IOException.class);

        f.leader().answerSender = "uid-l";
        f.leader().answerEpoch = 8;
        assertThat(f.writer().write(7, "uid-l", "http://l:1", commit("w")))
                .as("a newer term answered: this one is deposed")
                .isInstanceOf(FastWriter.Fenced.class);
    }

    @Test
    void aRUNOfMoreThanBRecordsIsSplitIntoRunsOfAtMostB() {
        RunKey t = new RunKey(new UUID(2, 2), 0);
        SegmentRecord r = new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(),
                new byte[] {1});
        List<List<FastWriteFrame.CommitRun>> batches = FastWriter.splitAtBound(List.of(
                new FastWriteFrame.CommitRun(S, List.of(r, r, r, r, r)),
                new FastWriteFrame.CommitRun(t, List.of(r))), 2);

        assertThat(batches).hasSize(3);
        assertThat(batches.get(0)).extracting(run -> run.records().size()).containsExactly(2, 1);
        assertThat(batches.get(1)).extracting(run -> run.records().size()).containsExactly(2);
        assertThat(batches.get(2)).extracting(run -> run.records().size()).containsExactly(1);
        assertThat(batches.get(2).get(0).stream()).isEqualTo(S);
    }
}
