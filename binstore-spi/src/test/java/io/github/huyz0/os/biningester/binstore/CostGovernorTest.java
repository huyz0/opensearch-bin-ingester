// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * The governor's arithmetic (M10.10, ADR-0075, M10 criterion 13), T0 over an
 * injected clock.
 *
 * <p>⚠️ EVERY THRESHOLD IS CROSSED FROM BOTH SIDES. A governor that halts at
 * 3× or never halts, or that takes its expected rate at the floor rather than
 * at the spacing in force, passes any one-sided case.
 */
class CostGovernorTest {

    private static final long MIB = 1L << 20;
    private static final long SEGMENT = 8 * MIB;
    /** Small enough that the bytes term never decides the expected rate. */
    private static final long SMALL = 1_000;

    /** A clock the test moves. */
    private static final class TestClock extends Clock {
        private Instant now = Instant.ofEpochMilli(1_790_000_000_000L);

        @Override public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override public Instant instant() {
            return now;
        }

        void advance(Duration d) {
            now = now.plus(d);
        }
    }

    private final TestClock clock = new TestClock();
    private final AtomicLong spacingMillis = new AtomicLong(5_000);

    private CostGovernor governor(int burst) {
        return new CostGovernor(new CostGovernor.Settings(1.0, burst, Duration.ofSeconds(60),
                SEGMENT), clock, spacingMillis::get);
    }

    /** Spends one window issuing {@code puts} data PUTs of {@code bytes} each, evenly. */
    private void window(CostGovernor g, int puts, long bytes) {
        long step = 60_000L / Math.max(1, puts);
        for (int i = 0; i < puts; i++) {
            g.recordDataPut(bytes);
            clock.advance(Duration.ofMillis(step));
        }
        clock.advance(Duration.ofMillis(60_000L - step * puts));
    }

    @Test
    void anUndeclaredListPastTheBucketIsRefusedAndCountedAndTheBucketRefills() {
        CostGovernor g = governor(3);

        assertThat(g.admitList()).isTrue();
        assertThat(g.admitList()).isTrue();
        assertThat(g.admitList()).isTrue();
        assertThat(g.admitList()).as("the burst is spent").isFalse();
        assertThat(g.counts().listRefusals()).isEqualTo(1);

        clock.advance(Duration.ofSeconds(1));
        assertThat(g.admitList()).as("one token per second sustained").isTrue();
        assertThat(g.admitList()).isFalse();
        assertThat(g.counts().listRefusals()).isEqualTo(2);
    }

    @Test
    void aListInsideADeclaredRecoveryScopeIsNeverRefused() throws Exception {
        CostGovernor g = governor(1);
        assertThat(g.admitList()).isTrue();
        assertThat(g.admitList()).isFalse();

        int admitted = GovernorScope.recovery(() -> {
            int n = 0;
            for (int i = 0; i < 1_000; i++) {
                n += g.admitList() ? 1 : 0;
            }
            return n;
        });

        assertThat(admitted).as("⚠️ a takeover must recover its chain end with the bucket "
                + "empty, or it cannot commit").isEqualTo(1_000);
        assertThat(g.counts().recoveryLists()).isEqualTo(1_000);
        assertThat(g.counts().listRefusals()).isEqualTo(1);
        assertThat(g.admitList()).as("the scope ends with its call").isFalse();
    }

    @Test
    void theRatioAlarmsAtThreeHaltsAtTenAndRecoversWhenTheWindowIsNormal() {
        CostGovernor g = governor(300);

        window(g, 12, SMALL);
        assertThat(g.lastRatio()).as("5 s spacing: 12 PUTs per minute is expected")
                .isEqualTo(1.0);
        assertThat(g.alarmed()).isFalse();

        window(g, 30, SMALL);
        assertThat(g.alarmed()).as("2.5x is not an alarm").isFalse();
        window(g, 40, SMALL);
        assertThat(g.alarmed()).as("3.3x is").isTrue();
        assertThat(g.discretionaryAllowed()).as("an alarm is not a halt").isTrue();

        window(g, 115, SMALL);
        assertThat(g.discretionaryAllowed()).as("9.6x still runs discretionary work")
                .isTrue();
        window(g, 125, SMALL);
        assertThat(g.discretionaryAllowed()).as("10.4x halts it").isFalse();
        assertThat(g.counts().discretionaryRefusals()).isEqualTo(1);

        window(g, 12, SMALL);
        assertThat(g.discretionaryAllowed()).as("a halt lifts with the ratio").isTrue();
        assertThat(g.alarmed()).isFalse();
    }

    @Test
    void theKillSwitchTripsAtOneHundredAndHoldsUntilReset() {
        CostGovernor g = governor(300);

        window(g, 1_180, SMALL);
        assertThat(g.killSwitchTripped()).as("98x does not trip it").isFalse();
        window(g, 1_210, SMALL);
        assertThat(g.killSwitchTripped()).as("100.8x does").isTrue();

        window(g, 12, SMALL);
        window(g, 12, SMALL);
        assertThat(g.discretionaryAllowed())
                .as("⚠️ STICKY: silently resuming after a 100x event is what research 15 "
                        + "§5 forbids").isFalse();

        g.reset();
        assertThat(g.killSwitchTripped()).isFalse();
        assertThat(g.discretionaryAllowed()).isTrue();
    }

    @Test
    void aLowRatePerRecordRegressionReadsAboveTheHalt() {
        // ⚠️ THE CASE THAT A FLOOR-BASED EXPECTED RATE MISSES: 10 records/s
        // each PUT alone, with the interval at the 5 s ceiling. Against the
        // 250 ms floor that is 2.5x -- no alarm at all; against the spacing in
        // force it is 50x.
        CostGovernor g = governor(300);

        window(g, 600, 2_000);

        assertThat(g.lastRatio()).isEqualTo(50.0);
        assertThat(g.discretionaryAllowed()).isFalse();
    }

    @Test
    void expectedFollowsBytesInTheSizeTriggeredRegime() {
        // 100 MiB/s in full 8 MiB segments at the floor: 750 PUTs a minute is
        // exactly what the bytes demand, whatever the spacing says.
        spacingMillis.set(250);
        CostGovernor g = governor(300);

        window(g, 750, SEGMENT);

        assertThat(g.lastRatio()).isEqualTo(1.0);
        assertThat(g.alarmed()).isFalse();
    }

    @Test
    void expectedIsTakenAtTheSmallestSpacingTheWindowHeld() {
        // A lane +2 trickle shortens the spacing to 1.25 s for part of the
        // window: 48 flushes a minute are then in bounds, and reading the
        // spacing only at the window's end (5 s) would call them 4x.
        CostGovernor g = governor(300);

        spacingMillis.set(1_250);
        for (int i = 0; i < 47; i++) {
            g.recordDataPut(SMALL);
            clock.advance(Duration.ofMillis(1_250));
        }
        spacingMillis.set(5_000);
        g.recordDataPut(SMALL);
        clock.advance(Duration.ofMillis(60_000 - 47 * 1_250));
        g.discretionaryAllowed();

        assertThat(g.lastRatio()).isEqualTo(1.0);
        assertThat(g.alarmed()).isFalse();
    }

    @Test
    void aLongIdleRefillsTheBucketOnlyToItsBurst() {
        // ⚠️ "LIST UNBOUNDED": a refill without its cap lets a pod idle for a
        // day admit ~86,400 LISTs back to back.
        CostGovernor g = governor(5);

        clock.advance(Duration.ofDays(1));
        int admitted = 0;
        while (g.admitList()) {
            admitted++;
            if (admitted > 1_000) {
                break;
            }
        }

        assertThat(admitted).isEqualTo(5);
    }

    @Test
    void theSpacingIsResampledEveryWindowSoAShortOneDoesNotLingerAsAFloor() {
        CostGovernor g = governor(300);
        spacingMillis.set(250);
        window(g, 240, SMALL);
        assertThat(g.lastRatio()).as("at a 250 ms spacing, 240 a minute is expected")
                .isEqualTo(1.0);

        spacingMillis.set(5_000);
        window(g, 600, SMALL);

        assertThat(g.lastRatio())
                .as("⚠️ the next window at the 5 s ceiling is judged at 5 s: carried "
                        + "forward, the 250 ms sample would read this 50x regression as 2.5x")
                .isEqualTo(50.0);
        assertThat(g.discretionaryAllowed()).isFalse();
    }

    @Test
    void anIdleGapLiftsAHaltButNeverTheKillSwitch() {
        CostGovernor g = governor(300);
        window(g, 125, SMALL);
        assertThat(g.discretionaryAllowed()).isFalse();

        clock.advance(Duration.ofMinutes(3));
        assertThat(g.discretionaryAllowed()).as("empty windows are a ratio of 0").isTrue();
        assertThat(g.lastRatio()).isZero();

        window(g, 1_210, SMALL);
        clock.advance(Duration.ofMinutes(3));
        assertThat(g.killSwitchTripped()).as("sticky across idle windows too").isTrue();
        assertThat(g.discretionaryAllowed()).isFalse();
    }

    @Test
    void theRefusalListenerIsToldOfEachRefusalAsItHappensAndOfNothingElse() {
        CostGovernor governor = governor(1);
        java.util.List<String> told = new java.util.ArrayList<>();
        governor.onRefusal(new CostGovernor.RefusalListener() {
            @Override
            public void listRefused() {
                told.add("list");
            }

            @Override
            public void discretionaryRefused() {
                told.add("discretionary");
            }
        });

        assertThat(governor.admitList()).isTrue();
        assertThat(governor.discretionaryAllowed()).isTrue();
        assertThat(told).as("an admitted LIST and an allowed start are not refusals").isEmpty();

        assertThat(governor.admitList()).isFalse();
        // ⚠️ 10x the expected data PUTs in one window halts discretionary work.
        spacingMillis.set(60_000);
        for (int i = 0; i < 10; i++) {
            governor.recordDataPut(SMALL);
        }
        clock.advance(Duration.ofSeconds(60));
        assertThat(governor.discretionaryAllowed()).isFalse();

        assertThat(told)
                .as("⚠️ TOLD AS IT HAPPENS, one call per refusal of its class (M10.27): an "
                        + "exporter reading only the counts sees nothing until asked")
                .containsExactly("list", "discretionary");
        assertThat(governor.counts().listRefusals()).isEqualTo(1);
        assertThat(governor.counts().discretionaryRefusals()).isEqualTo(1);
    }
}
