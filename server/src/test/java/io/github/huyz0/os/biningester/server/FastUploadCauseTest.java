// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * An upload is counted under its own cause where the node makes it (M13.36a
 * review round 1, T1): the leave's unpaced one as the leave's, never the
 * timer's.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FastUploadCauseTest {

    private static final UUID INDEX = new UUID(1, 1);
    private static final RunKey S = new RunKey(INDEX, 0);
    private static final Clock WALL = Clock.fixed(Instant.ofEpochMilli(1_000_000L),
            ZoneOffset.UTC);
    private static final MonotonicClock MONO = () -> 0L;
    private static final Roster.Incarnation SELF =
            new Roster.Incarnation("self", "uid-self", "az-a", "self:1");

    @Test
    void aLEAVEsUnpacedUploadIsCountedAsTheLeaves() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LeaseConfig leases = new LeaseConfig("p", "self", "", Duration.ofSeconds(10),
                Duration.ofSeconds(3));
        LocalSequencer term = LocalSequencer.start(store, "p",
                new LeaseManager(store, leases, WALL), 8).orElseThrow();
        try (FastDisk disk = FastDisk.open(store, leases.leaseKey(),
                Optional.of(new FastJournalConfig("/var/fast", 1L << 20)),
                new FastDiskTest.Files())) {
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
            CommitDesk desk = fast.commitDesk(disk.journal().orElseThrow(), SELF);
            FastFrame.Body assigned = fast.answerCommit(new FastFrame.Header(
                    FastWriteFrame.KIND_COMMIT, term.epoch(), "uid-self", "uid-self"),
                    new FastWriteFrame.Commit(new FastJournalRecord.IdempotencyKey("self",
                            new UUID(9, 9), 1), List.of(new FastWriteFrame.CommitRun(S,
                            List.of(new SegmentRecord("d", OpType.INDEX, OptionalLong.empty(),
                                    new byte[] {1}))))), desk);
            assertThat(assigned).as("the premise").isInstanceOf(FastWriteFrame.Assigned.class);
            assertThat(desk.exposures()).as("the premise: exposed at q = 1").hasSize(1);
            long timer = FastMetrics.uploads(true);
            long leave = FastMetrics.uploads(false);

            FastLeaderFrames.upload(term, disk, SELF, runs -> "seg-f", (endpoint, frame) -> {
                throw new IOException("no holder to release");
            }, (segmentKey, delta, epoch) -> { }, false);

            assertThat(term.committedNext(S)).as("uploaded and committed").isEqualTo(1);
            assertThat(FastMetrics.uploads(false) - leave).isEqualTo(1);
            assertThat(FastMetrics.uploads(true) - timer).isZero();
        } finally {
            term.close();
        }
    }
}
