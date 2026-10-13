// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastPublishFrame;
import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.sequencer.CommitDesk;
import io.github.huyz0.os.biningester.sequencer.EmptyTermCloser;
import io.github.huyz0.os.biningester.sequencer.FastFrameRouter;
import io.github.huyz0.os.biningester.sequencer.FastLeaderTerm;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFence;
import io.github.huyz0.os.biningester.sequencer.FastTermOpening;
import io.github.huyz0.os.biningester.sequencer.FastTermStart;
import io.github.huyz0.os.biningester.sequencer.JoinDesk;
import io.github.huyz0.os.biningester.sequencer.LeaseConfig;
import io.github.huyz0.os.biningester.sequencer.LeaseManager;
import io.github.huyz0.os.biningester.sequencer.LocalSequencer;
import io.github.huyz0.os.biningester.sequencer.MonotonicClock;
import io.github.huyz0.os.biningester.sequencer.RosterJoins;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The loss of a holder of an unreleased entry, found by the leader's drain
 * (ADR-0081 §7; M13.31f): over a real term at {@code wal_quorum = 2}, a pod
 * holding a copy the chain has not committed and that the EndpointSlice drops
 * makes the upload due at once -- once per loss, never for a pod still listed,
 * and with no view at all nobody is lost.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FastHolderLossTest {

    private static final UUID INDEX = new UUID(1, 1);
    private static final RunKey S = new RunKey(INDEX, 0);
    private static final Clock WALL = Clock.fixed(Instant.ofEpochMilli(1_000_000L),
            ZoneOffset.UTC);
    private static final MonotonicClock MONO = () -> 0L;
    private static final Roster.Incarnation SELF =
            new Roster.Incarnation("self", "uid-self", "az-a", "self:1");
    private static final Roster.Incarnation B =
            new Roster.Incarnation("b", "uid-b", "az-b", "b:1");

    private record Leading(LocalSequencer term, FastLeaderTerm fast, FastDisk disk,
            CommitDesk desk) {
    }

    /** A serving fast term led by SELF, with B joined to it. */
    private static Leading leading(MemoryBinStore store) throws Exception {
        LeaseConfig leases = new LeaseConfig("p", "self", "", Duration.ofSeconds(10),
                Duration.ofSeconds(3));
        LocalSequencer term = LocalSequencer.start(store, "p",
                new LeaseManager(store, leases, WALL), 8).orElseThrow();
        FastDisk disk = FastDisk.open(store, leases.leaseKey(),
                Optional.of(new FastJournalConfig("/var/fast", 1L << 20)),
                new FastDiskTest.Files());
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(store, "p"),
                new EmptyTermCloser(store, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), WALL, MONO), SELF, Map::of)
                .open(term.epoch(), term::close).orElseThrow();
        FastLeaderTerm fast = new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(store, "p", term.epoch(), MONO,
                        Duration.ofMillis(250)), nanos -> { }),
                term::committedNext, store, "p", MONO, Duration.ofMillis(250));
        term.attach(fast);
        fast.catalogSays(Map.of(INDEX, 2));
        fast.flushTermRecord();
        // ⚠️ A SECOND POD (review round 1, T1): the leader's own endpoint and
        // uid would not tell a recipient from the sender.
        assertThat(fast.answerJoin(new FastFrame.Header(FastFrame.KIND_JOIN, term.epoch(),
                B.podUid(), SELF.podUid()), new FastFrame.Join(B, FastFrame.Held.NONE)))
                .isInstanceOf(FastFrame.Joined.class);
        return new Leading(term, fast, disk, fast.commitDesk(disk.journal().orElseThrow(),
                SELF));
    }

    /** B commits one record of S, assigned, then confirms its own journaled copy. */
    private static void heldByB(Leading l) throws IOException {
        FastJournalRecord.IdempotencyKey key =
                new FastJournalRecord.IdempotencyKey("b", new UUID(8, 8), 1);
        var assigned = (FastWriteFrame.Assigned) l.fast().answerCommit(new FastFrame.Header(
                FastWriteFrame.KIND_COMMIT, l.term().epoch(), B.podUid(), SELF.podUid()),
                new FastWriteFrame.Commit(key, List.of(new FastWriteFrame.CommitRun(S,
                        List.of(new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(),
                                new byte[] {1}))))), l.desk());
        l.fast().answerConfirm(new FastFrame.Header(FastWriteFrame.KIND_CONFIRM,
                l.term().epoch(), B.podUid(), SELF.podUid()), new FastWriteFrame.Confirm(key,
                        assigned.assignedAfter(), List.of(new FastWriteFrame.RunAt(S,
                                assigned.runs().get(0).firstOffset())), true), l.desk());
    }

    private static final Duration MINUTE = Duration.ofMinutes(1);

    @Test
    void aHOLDERTheSliceDropsMakesTheUploadDueAtOnceOncePerLoss() throws Exception {
        Leading l = leading(new MemoryBinStore());
        try {
            heldByB(l);
            l.desk().uploaded(0);
            FastHolderLoss loss = new FastHolderLoss();
            assertThat(l.desk().holders()).as("the premise").containsExactly(B.podUid());

            loss.check(l.term(), l.disk(), SELF, uid -> true);
            assertThat(l.desk().uploadDue(1, MINUTE)).as("still listed").isFalse();
            loss.check(l.term(), l.disk(), SELF, uid -> !uid.equals(B.podUid()));
            assertThat(l.desk().uploadDue(1, MINUTE)).as("dropped: at once").isTrue();

            l.desk().uploaded(2);
            loss.check(l.term(), l.disk(), SELF, uid -> !uid.equals(B.podUid()));
            assertThat(l.desk().uploadDue(3, MINUTE)).as("reported once").isFalse();
            loss.check(l.term(), l.disk(), SELF, uid -> true);
            loss.check(l.term(), l.disk(), SELF, uid -> !uid.equals(B.podUid()));
            assertThat(l.desk().uploadDue(3, MINUTE)).as("listed, then dropped anew")
                    .isTrue();
        } finally {
            l.disk().close();
            l.term().close();
        }
    }

    @Test
    void withNOViewNobodyIsLost() throws Exception {
        Leading l = leading(new MemoryBinStore());
        try {
            heldByB(l);
            l.desk().uploaded(0);

            new FastHolderLoss().check(l.term(), l.disk(), SELF,
                    FastPeer.liveness(null));

            assertThat(l.desk().uploadDue(1, MINUTE)).isFalse();
        } finally {
            l.disk().close();
            l.term().close();
        }
    }

    @Test
    void aNEWTermStartsFromNothing() {
        // ⚠️ its review round 1, T1: a loss reported in term 3 is reported
        // again in term 4, whose desk never heard it.
        FastHolderLoss loss = new FastHolderLoss();
        FastFrameRouter.Liveness bGone = uid -> !uid.equals("uid-b");

        assertThat(loss.newlyLost(3, java.util.Set.of("uid-b"), bGone))
                .containsExactly("uid-b");
        assertThat(loss.newlyLost(3, java.util.Set.of("uid-b"), bGone)).isEmpty();
        assertThat(loss.newlyLost(4, java.util.Set.of("uid-b"), bGone))
                .containsExactly("uid-b");
    }

    @Test
    void aHOLDERReleasedThenHoldingAgainUnlistedIsReportedAnew() {
        // ⚠️ its review round 1, T1: its copies released, it was no holder.
        FastHolderLoss loss = new FastHolderLoss();
        FastFrameRouter.Liveness bGone = uid -> !uid.equals("uid-b");
        loss.newlyLost(3, java.util.Set.of("uid-b"), bGone);

        loss.newlyLost(3, java.util.Set.of(), bGone);

        assertThat(loss.newlyLost(3, java.util.Set.of("uid-b"), bGone))
                .containsExactly("uid-b");
    }
}
