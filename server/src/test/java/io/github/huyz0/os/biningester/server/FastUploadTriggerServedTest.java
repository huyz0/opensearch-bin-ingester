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
 * The node's paced upload waits for its trigger (ADR-0081 §7; M13.31c): the
 * term's first at once, the next only once the tightest {@code flush_timer}
 * has passed since it -- not on every drain tick with something exposed.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FastUploadTriggerServedTest {

    private static final UUID INDEX = new UUID(1, 1);
    private static final RunKey S = new RunKey(INDEX, 0);
    private static final Clock WALL = Clock.fixed(Instant.ofEpochMilli(1_000_000L),
            ZoneOffset.UTC);
    private static final MonotonicClock MONO = () -> 0L;
    private static final Roster.Incarnation SELF =
            new Roster.Incarnation("self", "uid-self", "az-a", "self:1");

    /** A COMMIT of one record of S from this leader, by {@code seq}. */
    private static FastWriteFrame.Commit commit(long seq) {
        return new FastWriteFrame.Commit(new FastJournalRecord.IdempotencyKey("self",
                new UUID(9, 9), seq), List.of(new FastWriteFrame.CommitRun(S,
                List.of(new SegmentRecord("d" + seq, OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1})))));
    }

    @Test
    void aPACEDUploadWaitsForTheFlushTimerAfterTheTermsFirst() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LeaseConfig leases = new LeaseConfig("p", "self", "", Duration.ofSeconds(10),
                Duration.ofSeconds(3));
        LocalSequencer term = LocalSequencer.start(store, "p",
                new LeaseManager(store, leases, WALL), 8).orElseThrow();
        java.util.concurrent.atomic.AtomicLong now = new java.util.concurrent.atomic.AtomicLong();
        Duration flushTimer = Duration.ofSeconds(5);
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
                    term::committedNext, store, "p", MONO, Duration.ofMillis(0));
            term.attach(fast);
            fast.catalogSays(Map.of(INDEX, 1));
            fast.flushTermRecord();
            CommitDesk desk = fast.commitDesk(disk.journal().orElseThrow(), SELF);
            FastFrame.Header header = new FastFrame.Header(FastWriteFrame.KIND_COMMIT,
                    term.epoch(), "uid-self", "uid-self");
            Runnable upload = () -> FastLeaderFrames.upload(term, disk, SELF, runs -> "seg",
                    (endpoint, frame) -> {
                        throw new java.io.IOException("no holder to release");
                    }, (segmentKey, delta, epoch) -> { }, true, now::get, flushTimer);

            fast.answerCommit(header, commit(1), desk);
            desk.exposures();
            upload.run();
            assertThat(term.committedNext(S)).as("the term's first, at once").isEqualTo(1);

            fast.answerCommit(header, commit(2), desk);
            desk.exposures();
            now.set(flushTimer.toNanos() - 1);
            upload.run();
            assertThat(term.committedNext(S)).as("inside the flush timer").isEqualTo(1);

            now.set(flushTimer.toNanos());
            upload.run();
            assertThat(term.committedNext(S)).as("at the flush timer").isEqualTo(2);
        } finally {
            term.close();
        }
    }
}
