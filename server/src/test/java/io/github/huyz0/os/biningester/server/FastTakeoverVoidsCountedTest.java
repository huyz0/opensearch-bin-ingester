// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastTakeoverFrame;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
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
import io.github.huyz0.os.biningester.sequencer.TermJoiner;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A takeover's voids reach fast mode's metric (M13.36a review round 1, T1):
 * term 1's leader {@code x} is gone, so the stream's quorum is lost, and the
 * recovery commits {@code m}'s entry at c0 and voids the rest of the bound --
 * counted, in offsets, where the takeover writes it.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FastTakeoverVoidsCountedTest {

    private static final UUID INDEX = new UUID(1, 1);
    private static final RunKey S = new RunKey(INDEX, 0);
    private static final Clock WALL = Clock.fixed(Instant.ofEpochMilli(1_000_000L),
            ZoneOffset.UTC);
    private static final MonotonicClock MONO = () -> 0L;
    private static final Roster.Incarnation SELF =
            new Roster.Incarnation("self", "uid-self", "az-a", "self:1");
    private static final Roster.Incarnation X =
            new Roster.Incarnation("x", "uid-x", "az-x", "x:1");
    private static final Roster.Incarnation M =
            new Roster.Incarnation("m", "uid-m", "az-m", "m:1");

    /** {@code m} answers FENCE with one entry of S at 0; {@code x} answers nothing. */
    private static TermJoiner.Transport members() {
        return (endpoint, frame) -> {
            FastFrame.Frame asked = FastFrame.decode(frame);
            String member = asked.header().targetUid();
            if (!member.equals("uid-m")
                    || !(asked.body() instanceof FastTakeoverFrame.Fence fence)) {
                throw new IOException("unanswered");
            }
            return FastFrame.encode(2, member, "uid-self", new FastTakeoverFrame.Collected(
                    fence.page(), true, List.of(new FastJournalRecord.Entry(1, S, 0, 1, 0,
                            new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), 0),
                            List.of(new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(),
                                    new byte[] {1}))))));
        };
    }

    @Test
    void aQUORUMLossRecoverysVoidsAreCounted() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LeaseConfig leases = new LeaseConfig("p", "self", "", Duration.ofSeconds(10),
                Duration.ofSeconds(3));
        LocalSequencer.start(store, "p", new LeaseManager(store, leases, WALL), 8)
                .orElseThrow().close();
        LocalSequencer term = LocalSequencer.start(store, "p",
                new LeaseManager(store, leases, WALL), 8).orElseThrow();
        Roster one = new Roster(1, -1, X, List.of(new Roster.Member(X, Roster.State.ROSTERED),
                new Roster.Member(M, Roster.State.ROSTERED)),
                List.of(new Roster.TermRecord(0, new TreeMap<>(Map.of(INDEX, 1)))), List.of(),
                0, 0, false);
        store.put(Roster.key("p", 1), Body.ofBytes(one.encode()));
        store.put(Roster.latestKey("p"), Body.ofBytes(Roster.encodeLatest(1)));
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(store, "p"),
                new EmptyTermCloser(store, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), WALL, MONO), SELF, Map::of)
                .open(2, term::close).orElseThrow();
        term.attach(new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(store, "p", 2, MONO, Duration.ofMillis(250)),
                        nanos -> { }),
                term::committedNext, store, "p", MONO, Duration.ofMillis(250)));
        long before = FastMetrics.voided();
        try (FastDisk disk = FastDisk.open(store, leases.leaseKey(),
                Optional.of(new FastJournalConfig("/var/fast", 1L << 20)),
                new FastDiskTest.Files())) {
            assertThat(FastTakeover.start(term, disk, SELF, members(),
                    incarnation -> incarnation.podUid().equals("uid-x"),
                    runs -> "seg-r", store, "p", WALL, Runnable::run)).isTrue();
        } finally {
            term.close();
        }

        assertThat(FastMetrics.voided() - before)
                .as("m's entry committed at 0, the rest of the bound voided")
                .isEqualTo(FastLeaderTerm.STREAM_BOUND - 1);
    }
}
