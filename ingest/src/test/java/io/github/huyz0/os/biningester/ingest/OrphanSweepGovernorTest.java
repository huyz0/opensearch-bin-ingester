// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CostGovernor;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.GoverningBinStore;
import io.github.huyz0.os.biningester.binstore.StoreCounts;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.sequencer.CommitLog;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Discretionary GC asks the governor first, and a refused sweep LIST DEFERS the
 * pass rather than failing it (M10.11, ADR-0075, criterion 14).
 *
 * <p>⚠️ **BEFORE THIS, A FAILED SWEEP LIST SKIPPED ITS HOUR FOR GOOD**: the loop
 * advanced past an hour whether or not its LIST was answered, which is the
 * safe direction for an outage (orphans are storage, not data) and the WRONG
 * one for a refusal, which says only "not now". A governor that refused a
 * sweep would then leave that hour's orphans in the bucket for ever.
 *
 * <p>⚠️ **THE GOVERNOR's CLOCK IS ITS OWN**, so the bucket refills only when a
 * case says so, while the loop's clock walks hours.
 */
class OrphanSweepGovernorTest {

    private static final String PREFIX = "bucket";
    private static final Duration MIN = Duration.ofHours(6);
    private static final Duration MAX = Duration.ofHours(24);

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
    private final CountingBinStore counting = new CountingBinStore(backing);
    private final MutableClock clock = new MutableClock(Instant.parse("2026-09-17T10:30:00Z"));
    private final MutableClock governorClock =
            new MutableClock(Instant.parse("2026-09-17T10:30:00Z"));
    /** Two LISTs a tick fit the burst: the inbox read, then the hour. */
    private final CostGovernor governor = new CostGovernor(
            new CostGovernor.Settings(1.0, 2, Duration.ofMinutes(1), 1 << 20), governorClock,
            () -> 250);
    private final GoverningBinStore governed = new GoverningBinStore(counting, governor);

    @AfterEach
    void close() throws Exception {
        backing.close();
    }

    private RetentionLoop loop(CommitLog log, BooleanSupplier discretionaryAllowed) {
        RetentionRule rule = new RetentionRule(clock, MIN, MAX, 0, (key, age) -> { },
                stream -> new WatermarkTable.Watermark(true, true, 0));
        RetentionObservable observable = new RetentionObservable(clock, MIN, MAX,
                Duration.ofMinutes(1), alarm -> { });
        return new RetentionLoop(
                () -> Optional.of(new RetentionLoop.Term(log.chain(), boundary -> { }, () -> true)),
                new LeasedGc(new FreeLease(), governed), rule, observable, clock, PREFIX, MIN,
                OrphanSweep.DEFAULT_GRACE, SegmentGc.DEFAULT_DELETE_BATCH, discretionaryAllowed);
    }

    private String orphanAt(String writtenAt) throws Exception {
        String key = new SegmentKey(PREFIX, Instant.parse(writtenAt).toEpochMilli(), "poda", 1,
                12).key();
        backing.put(key, Body.ofBytes(new byte[] {1}));
        return key;
    }

    @Test
    void aREFUSEDSweepLISTDefersTheHOURAndTheNEXTTickSweepsIt() throws Exception {
        RetentionLoop loop = loop(new CommitLog(backing, PREFIX + "/ctl/log", 1), () -> true);
        loop.tick(); // the term is first seen at 10:30, so the sweep may start at 12:00
        String orphan = orphanAt("2026-09-17T12:10:00Z");
        // ⚠️ ONE TOKEN LEFT: the inbox read takes it, and the hour's LIST is refused.
        assertThat(governor.admitList()).as("the premise: the burst had a token to spend")
                .isTrue();

        clock.set(Instant.parse("2026-09-17T14:00:01Z"));
        loop.tick();

        assertThat(governor.counts().listRefusals())
                .as("the premise: the hour's LIST reached the governor and was refused")
                .isEqualTo(1);
        assertThat(backing.stat(orphan)).as("nothing was listed, so nothing was swept")
                .isPresent();

        // ⚠️ THE BUCKET REFILLS; the loop's clock moves one second. The hour is
        // still owed, and a loop that had advanced past it never comes back.
        governorClock.set(Instant.parse("2026-09-17T10:30:10Z"));
        clock.set(Instant.parse("2026-09-17T14:00:02Z"));
        loop.tick();

        assertThat(backing.stat(orphan))
                .as("⚠️ THE DEFERRED HOUR IS SWEPT ON THE NEXT TICK: a refusal defers the "
                        + "pass, it does not skip the hour and it does not stop the loop")
                .isEmpty();
    }

    @Test
    void aHALTEDGovernorDefersTheWholePASSWithNOStoreRequestAndItRunsOnceALLOWED()
            throws Exception {
        AtomicBoolean allowed = new AtomicBoolean(false);
        RetentionLoop loop = loop(new CommitLog(backing, PREFIX + "/ctl/log", 1), allowed::get);
        loop.tick();
        String orphan = orphanAt("2026-09-17T12:10:00Z");

        clock.set(Instant.parse("2026-09-17T14:00:01Z"));
        StoreCounts before = counting.counts();
        loop.tick();

        assertThat(counting.counts().total() - before.total())
                .as("⚠️ DISCRETIONARY WORK ASKS FIRST: a halted pass issues no LIST, no GET, "
                        + "no DELETE")
                .isZero();
        assertThat(backing.stat(orphan)).isPresent();

        allowed.set(true);
        clock.set(Instant.parse("2026-09-17T14:00:02Z"));
        loop.tick();

        assertThat(backing.stat(orphan))
                .as("and the deferred pass runs once the halt lifts: nothing was skipped")
                .isEmpty();
    }
}
