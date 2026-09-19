// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.Body;
import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import binjava.format.SegmentCommit;
import binjava.format.SegmentKey;
import binjava.sequencer.ChainGc;
import binjava.sequencer.CommitLog;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A term whose chain reaches the retention floor sweeps the hours before it
 * began (M8.42, M8.10, FR-9, NFR-3).
 *
 * <p>⚠️ **WITHOUT THE FLOOR, THE CONFINEMENT IS NOT OPTIONAL.** A replay stops
 * at a checkpoint, so a new term's keep list lacked every delta below it, and
 * sweeping an hour before the term would list a committed segment, find it in
 * no delta, and delete it. So a crash's orphan -- written before its
 * successor's term began -- was swept by no term at all.
 *
 * <p>⚠️ **WITH IT, THE SAME HOURS ARE SAFE**: the backfill holds every delta
 * still in the bucket, and chain GC deletes a delta only once every segment it
 * names is gone, so a committed segment is always named.
 */
class RetentionBackfillSweepTest {

    private static final RunKey STREAM = new RunKey(new UUID(0x5151_5151L, 1), 0);
    private static final String PREFIX = "bucket";
    private static final Duration MIN = Duration.ofHours(6);
    private static final Duration MAX = Duration.ofHours(24);
    private static final Duration GRACE = OrphanSweep.DEFAULT_GRACE;

    private static final class MutableClock extends Clock {
        private Instant now;

        MutableClock(Instant start) {
            this.now = start;
        }

        void set(Instant at) {
            now = at;
        }

        @Override
        public Instant instant() {
            return now;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }
    }

    private static final class FreeLease implements GcLease {
        @Override
        public boolean acquire() {
            return true;
        }

        @Override
        public boolean stillHeld() {
            return true;
        }

        @Override
        public void release() {
        }
    }

    private final MemoryBinStore backing = new MemoryBinStore();
    private final CountingBinStore store = new CountingBinStore(backing);
    private final MutableClock clock = new MutableClock(Instant.parse("2026-09-17T10:30:00Z"));

    @AfterEach
    void close() throws Exception {
        backing.close();
    }

    private RetentionLoop loop(CommitLog log) {
        RetentionRule nothingDue = new RetentionRule(clock, MIN, MAX, 0, (key, age) -> { },
                stream -> new WatermarkTable.Watermark(true, true, 0));
        RetentionObservable observable = new RetentionObservable(clock, MIN, MAX,
                Duration.ofMinutes(1), alarm -> { });
        return new RetentionLoop(
                () -> Optional.of(new RetentionLoop.Term(log.chain(), retained -> { },
                        () -> true)),
                new LeasedGc(new FreeLease(), store), nothingDue, observable, clock, PREFIX,
                MIN, GRACE, SegmentGc.DEFAULT_DELETE_BATCH);
    }

    private String put(Instant writtenAt, long sequence) throws Exception {
        String key = new SegmentKey(PREFIX, writtenAt.toEpochMilli(), "poda", sequence, 12).key();
        backing.put(key, Body.ofBytes(new byte[] {1}));
        return key;
    }

    /** A predecessor's delta naming {@code segment}, as the backfill would read it. */
    private static ChainGc.DeltaAt predecessorNaming(String segment) {
        return new ChainGc.DeltaAt(1, 5, new CommitDelta(5, List.of(new SegmentCommit(segment,
                List.of(new RunCommit(STREAM, 1, 0)),
                new SegmentCommit.Attribution("poda", "i1", 0)))));
    }

    @Test
    void aBACKFILLEDChainSWEEPSACrashsOrphanAndKEEPSWhatThePredecessorCOMMITTED()
            throws Exception {
        CommitLog log = new CommitLog(store, PREFIX + "/ctl/log", 2);
        RetentionLoop loop = loop(log);
        loop.tick(); // the term is first seen at 10:30

        String orphan = put(Instant.parse("2026-09-17T09:10:00Z"), 1);
        String committedBefore = put(Instant.parse("2026-09-17T09:20:00Z"), 2);
        clock.set(Instant.parse("2026-09-17T11:00:01Z").plus(GRACE));
        loop.tick();

        assertThat(backing.stat(orphan))
                .as("the premise: without the floor, no hour before the term is entered")
                .isPresent();

        log.chain().backfill(List.of(predecessorNaming(committedBefore)));
        assertThat(log.chain().snapshot().fromFloor()).isTrue();
        // ⚠️ PACED: the widened sweep starts a retention window back and takes
        // a few hours a tick, so it reaches 09:00 only after catching up.
        for (int i = 0; i < 12; i++) {
            loop.tick();
        }

        assertThat(backing.stat(orphan))
                .as("⚠️ M8.10's ORPHAN: written before this term began, now swept")
                .isEmpty();
        assertThat(backing.stat(committedBefore))
                .as("⚠️ AND A SEGMENT NAMED ONLY BELOW THE CHECKPOINT IS KEPT -- the "
                        + "fail-open deletion the confinement existed to prevent")
                .isPresent();
    }

    @Test
    void theWIDENEDSweepGoesNoFurtherBackThanTheMAXIMUMRetention() throws Exception {
        CommitLog log = new CommitLog(store, PREFIX + "/ctl/log", 2);
        RetentionLoop loop = loop(log);
        loop.tick();
        String ancient = put(Instant.parse("2026-09-16T08:10:00Z"), 1);
        log.chain().backfill(List.of());

        clock.set(Instant.parse("2026-09-17T11:00:01Z").plus(GRACE));
        for (int i = 0; i < 40; i++) {
            loop.tick();
        }

        assertThat(backing.stat(ancient))
                .as("26 hours back, past the %s maximum: not this term's to list", MAX)
                .isPresent();
    }

    @Test
    void aCATCHUPSweepIsPACEDAcrossTicksNotOneBURST() throws Exception {
        CommitLog log = new CommitLog(store, PREFIX + "/ctl/log", 2);
        RetentionLoop loop = loop(log);
        loop.tick();
        log.chain().backfill(List.of());
        clock.set(Instant.parse("2026-09-17T11:00:01Z").plus(GRACE));

        long before = store.counts().lists();
        loop.tick();

        assertThat(store.counts().lists() - before)
                .as("⚠️ A DAY OF HOURS IN ONE TICK IS A LIST BURST (cost.md rule 2): at most "
                        + "%d hours a tick, plus ONE LIST of the inbox, whose intents' "
                        + "segments the sweep must keep (M8.14a)",
                        RetentionLoop.MAX_SWEEP_HOURS_PER_TICK)
                .isBetween(2L, (long) RetentionLoop.MAX_SWEEP_HOURS_PER_TICK + 1);
    }

    @Test
    void aChainThatDoesNOTReachTheFloorKeepsTheMARGIN() throws Exception {
        CommitLog log = new CommitLog(store, PREFIX + "/ctl/log", 2);
        RetentionLoop loop = loop(log);
        loop.tick();
        String orphan = put(Instant.parse("2026-09-17T09:10:00Z"), 1);

        clock.set(Instant.parse("2026-09-17T11:00:01Z").plus(GRACE));
        for (int i = 0; i < 5; i++) {
            loop.tick();
        }

        assertThat(backing.stat(orphan))
                .as("no backfill, no widening: the keep list may lack what named it")
                .isPresent();
        assertThat(log.chain().snapshot().fromFloor()).isFalse();
    }
}
