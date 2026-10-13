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
 * The leader's side of publication over a real term (ADR-0081 §6; M13.30d):
 * an INTEREST taken from a rostered pod and refused from any other, what an
 * exposure owes sent to the interested pod as a PUBLISH from this leader in
 * this term, a pod no longer rostered as itself owed nothing (review round 1,
 * P1), a failed PUBLISH dropped and counted, and what one step owes cut under
 * a frame's bytes (T2).
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FastPublishServedTest {

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
        fast.catalogSays(Map.of(INDEX, 1));
        fast.flushTermRecord();
        // ⚠️ A SECOND POD (review round 1, T1): the leader's own endpoint and
        // uid would not tell a recipient from the sender.
        assertThat(fast.answerJoin(new FastFrame.Header(FastFrame.KIND_JOIN, term.epoch(),
                B.podUid(), SELF.podUid()), new FastFrame.Join(B, FastFrame.Held.NONE)))
                .isInstanceOf(FastFrame.Joined.class);
        return new Leading(term, fast, disk, fast.commitDesk(disk.journal().orElseThrow(),
                SELF));
    }

    private static FastFrame.Header interestFrom(long epoch, String sender) {
        return new FastFrame.Header(FastPublishFrame.KIND_INTEREST, epoch, sender,
                SELF.podUid());
    }

    /** One record of S, by {@code seq}, committed by this leader and exposed. */
    private static void exposeOne(Leading l, long seq) throws IOException {
        l.fast().answerCommit(new FastFrame.Header(FastWriteFrame.KIND_COMMIT,
                l.term().epoch(), SELF.podUid(), SELF.podUid()),
                new FastWriteFrame.Commit(new FastJournalRecord.IdempotencyKey("self",
                        new UUID(9, 9), seq), List.of(new FastWriteFrame.CommitRun(S,
                        List.of(new SegmentRecord("d" + seq, OpType.INDEX,
                                OptionalLong.empty(), new byte[] {1}))))), l.desk());
        l.desk().exposures();
    }

    @Test
    void anINTERESTIsTakenFromARosteredPodAndRefusedFromAnyOther() throws Exception {
        Leading l = leading(new MemoryBinStore());
        try {
            var interest = new FastPublishFrame.Interest(List.of(S), List.of());

            assertThat(FastLeaderFrames.answerInterest(l.term(), l.disk(), SELF,
                    interestFrom(l.term().epoch(), B.podUid()), interest))
                    .isEqualTo(new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE));
            assertThat(((FastFrame.Refused) FastLeaderFrames.answerInterest(l.term(), l.disk(),
                    SELF, interestFrom(l.term().epoch(), "uid-stranger"), interest)).reason())
                    .isEqualTo(FastFrame.Reason.NOT_ROSTERED);
            assertThat(((FastFrame.Refused) FastLeaderFrames.answerInterest(l.term(), l.disk(),
                    SELF, interestFrom(l.term().epoch() + 1, B.podUid()), interest)).reason())
                    .as("another term's").isEqualTo(FastFrame.Reason.NOT_ROSTERED);
        } finally {
            l.disk().close();
            l.term().close();
        }
    }

    @Test
    void anEXPOSUREIsPublishedToTheInterestedPodFromThisLeaderInThisTerm() throws Exception {
        Leading l = leading(new MemoryBinStore());
        try {
            FastLeaderFrames.answerInterest(l.term(), l.disk(), SELF,
                    interestFrom(l.term().epoch(), B.podUid()),
                    new FastPublishFrame.Interest(List.of(S), List.of()));
            exposeOne(l, 1);
            List<String> sent = new ArrayList<>();
            long counted = FastMetrics.publishes(true);

            int published = FastLeaderFrames.publish(l.term(), l.disk(), SELF,
                    (endpoint, frame) -> {
                        FastFrame.Frame f = FastFrame.decode(frame);
                        sent.add(endpoint + " " + f.header().kind() + " " + f.header().epoch()
                                + " " + f.header().senderUid() + "->"
                                + f.header().targetUid() + " "
                                + ((FastPublishFrame.Publish) f.body()).entries().size());
                        return new byte[0];
                    });

            assertThat(published).isEqualTo(1);
            assertThat(sent).containsExactly("b:1 7 " + l.term().epoch()
                    + " uid-self->uid-b 1");
            assertThat(FastMetrics.publishes(true) - counted).as("counted").isEqualTo(1);
        } finally {
            l.disk().close();
            l.term().close();
        }
    }

    @Test
    void aPODNoLongerRosteredAsItselfIsOwedNothing() throws Exception {
        // ⚠️ review round 1, P1: an INTEREST outlives its incarnation.
        Leading l = leading(new MemoryBinStore());
        try {
            Roster.Incarnation restarted = new Roster.Incarnation("b", "uid-b", "az-b", "b:2");
            l.desk().publications().interest(restarted, List.of(S), List.of());
            exposeOne(l, 1);
            List<String> sent = new ArrayList<>();

            FastLeaderFrames.publish(l.term(), l.disk(), SELF, (endpoint, frame) -> {
                sent.add(endpoint);
                return new byte[0];
            });
            exposeOne(l, 2);

            assertThat(sent).as("not the incarnation the roster lists").isEmpty();
            assertThat(l.desk().publications().take()).as("its interest forgotten").isEmpty();
        } finally {
            l.disk().close();
            l.term().close();
        }
    }

    @Test
    void aFAILEDPublishIsDroppedAndCounted() throws Exception {
        Leading l = leading(new MemoryBinStore());
        try {
            l.desk().publications().interest(B, List.of(S), List.of());
            exposeOne(l, 1);
            long dropped = FastMetrics.publishes(false);

            int published = FastLeaderFrames.publish(l.term(), l.disk(), SELF,
                    (endpoint, frame) -> {
                        throw new IOException("unanswered");
                    });

            assertThat(published).isZero();
            assertThat(FastMetrics.publishes(false) - dropped).isEqualTo(1);
            assertThat(l.desk().publications().take()).as("not owed again").isEmpty();
        } finally {
            l.disk().close();
            l.term().close();
        }
    }

    @Test
    void theLEADERsOwnSubscribersAreHandedTheirEntriesWithNoFrame() throws Exception {
        // ⚠️ M13.30k: the leader joins no term of its own, so no INTEREST or
        // PUBLISH of its reaches it; its own hub is fed directly.
        Leading l = leading(new MemoryBinStore());
        try {
            FastLeaderFrames.interestOwn(l.term(), l.disk(), SELF, java.util.Set.of(S));
            exposeOne(l, 1);
            List<String> frames = new ArrayList<>();
            List<Long> own = new ArrayList<>();

            FastLeaderFrames.publish(l.term(), l.disk(), SELF, (endpoint, frame) -> {
                frames.add(endpoint);
                return new byte[0];
            }, e -> own.add(e.firstOffset()));

            assertThat(own).as("its own hub's").containsExactly(0L);
            assertThat(frames).as("no frame to itself").isEmpty();
        } finally {
            l.disk().close();
            l.term().close();
        }
    }

    @Test
    void theLEADERsOwnInterestIsExactlyWhatItsHubWantsNow() throws Exception {
        Leading l = leading(new MemoryBinStore());
        try {
            FastLeaderFrames.interestOwn(l.term(), l.disk(), SELF, java.util.Set.of(S));
            FastLeaderFrames.interestOwn(l.term(), l.disk(), SELF, java.util.Set.of());
            exposeOne(l, 1);
            List<Long> own = new ArrayList<>();

            FastLeaderFrames.publish(l.term(), l.disk(), SELF, (endpoint, frame) -> new byte[0],
                    e -> own.add(e.firstOffset()));

            assertThat(own).as("its subscriber gone").isEmpty();
        } finally {
            l.disk().close();
            l.term().close();
        }
    }

    @Test
    void whatOneSTEPOwesIsCutUnderAFramesBytesInOrder() {
        // ⚠️ review round 1, T2.
        List<FastJournalRecord.Entry> entries = new ArrayList<>();
        for (long offset = 0; offset < 4; offset++) {
            entries.add(new FastJournalRecord.Entry(3, S, offset, 1, 0,
                    new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), offset),
                    List.of(new SegmentRecord("d" + offset, OpType.INDEX,
                            OptionalLong.empty(), new byte[] {1}))));
        }

        assertThat(FastLeaderFrames.chunks(entries, 8, e -> e.firstOffset() == 3 ? 9 : 4))
                .extracting(run -> run.stream().map(FastJournalRecord.Entry::firstOffset)
                        .toList())
                .containsExactly(List.of(0L, 1L), List.of(2L), List.of(3L));
    }
}
