// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The orphan sweep's grace is refused below what clock skew and an in-flight
 * commit together can eat (M8.52, FR-9, NFR-8).
 *
 * <p>⚠️ **A SEGMENT's AGE IS ITS WRITER's CLOCK AGAINST THE SWEEPER's.** A slow
 * writer and a fast sweeper each shift it by the skew, so the grace is eaten
 * by twice the skew: 10 min at criterion 14's +/-5 min. Below that plus the
 * longest commit delay, a segment whose commit is still in flight is deleted as
 * an orphan -- acknowledged data lost. The only guard was {@code grace > 0}.
 */
class OrphanGraceSkewTest {

    private final Clock clock = Clock.fixed(Instant.parse("2026-09-17T10:30:00Z"),
            ZoneOffset.UTC);

    @Test
    void theFLOORIsTwiceTheSkewPlusTheCommitDelay() {
        assertThat(OrphanSweep.MIN_GRACE)
                .isEqualTo(OrphanSweep.MAX_CLOCK_SKEW.multipliedBy(2)
                        .plus(OrphanSweep.MAX_COMMIT_DELAY));
        assertThat(OrphanSweep.MAX_CLOCK_SKEW)
                .as("criterion 14's skew, which ClockSkewIT runs at")
                .isEqualTo(Duration.ofMinutes(5));
        assertThat(OrphanSweep.DEFAULT_GRACE).as("and the default clears it")
                .isGreaterThanOrEqualTo(OrphanSweep.MIN_GRACE);
        assertThat(OrphanSweep.MIN_GRACE)
                .as("⚠️ THE VALUE, NOT ONLY THE FORMULA: a commit delay shortened to "
                        + "nothing would move a formula-only assertion with it")
                .isEqualTo(Duration.ofMinutes(15));
    }

    @Test
    void TWICETheSkewAloneIsREFUSED_ACommitCanStillBeInFlight() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            assertThatThrownBy(() -> new OrphanSweep(store, clock,
                    OrphanSweep.MAX_CLOCK_SKEW.multipliedBy(2), 1000, 1000))
                    .as("⚠️ 10 min covers the skew and nothing else")
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void aGRACEBelowTheFloorIsREFUSEDSayingWhy() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            assertThatThrownBy(() -> new OrphanSweep(store, clock,
                    OrphanSweep.MIN_GRACE.minusSeconds(1), 1000, 1000))
                    .as("⚠️ ONE SECOND UNDER: a segment still committing is deleted")
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("skew");
        }
    }

    @Test
    void theFLOORItselfIsACCEPTED() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            new OrphanSweep(store, clock, OrphanSweep.MIN_GRACE, 1000, 1000);
        }
    }

    @Test
    void theRETENTIONLoopRefusesItAtCONSTRUCTIONNotAtItsFirstSweep() throws Exception {
        // ⚠️ THE LOOP BUILDS ITS SWEEP INSIDE A TICK, hours after startup, where
        // a refusal is a log line on a scheduled task. So it checks up front.
        try (MemoryBinStore store = new MemoryBinStore()) {
            RetentionRule rule = new RetentionRule(clock, Duration.ofHours(6),
                    Duration.ofHours(24), 0, (key, age) -> { },
                    stream -> new WatermarkTable.Watermark(true, true, 0));
            GcLease lease = new GcLease() {
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
            };
            assertThatThrownBy(() -> new RetentionLoop(Optional::empty,
                    new LeasedGc(lease, store), rule,
                    new RetentionObservable(clock, Duration.ofHours(6), Duration.ofHours(24),
                            Duration.ofMinutes(1), alarm -> { }),
                    clock, "bucket", Duration.ofHours(6), Duration.ofMinutes(5),
                    SegmentGc.DEFAULT_DELETE_BATCH))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("skew");
        }
    }
}
