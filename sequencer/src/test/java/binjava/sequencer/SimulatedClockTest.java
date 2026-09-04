// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The injectable clock the simulation's determinism rests on.
 *
 * <p>⚠️ IT SHIPPED WITH NO TEST AT ALL, and `check-tdd` structurally cannot say
 * so: a class with no test methods demands no red record, so 49 lines of public
 * code whose {@code advance} could be a no-op and whose {@code millis()} could
 * read the wall clock passed every gate. Round-3 review of M4.12 found it by
 * reading. That is the blind spot these four tests close.
 *
 * <p>⚠️ WHY IT MATTERS MORE THAN IT LOOKS: every claim the simulation makes about
 * reproducibility -- "a failing seed is a permanent, named regression rather than
 * a story about a flake" -- rests on nothing here reading a wall clock. A
 * {@code millis()} that did would make failures irreproducible, and the harness
 * would report them as flakes in the system rather than in itself.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class SimulatedClockTest {

    @Test
    void itStartsWhereItWasToldAndNEVERReadsTheWallClock() {
        SimulatedClock clock = new SimulatedClock(1_000_000L);

        assertThat(clock.millis())
                .as("the clock reports the instant it was constructed with, not `now`")
                .isEqualTo(1_000_000L);
        assertThat(clock.instant().toEpochMilli()).isEqualTo(1_000_000L);
        // ⚠️ TWO READS, NO ADVANCE. A clock reading `System.currentTimeMillis()`
        // would differ between these unless the machine were infinitely fast --
        // and would differ from 1,000,000 regardless.
        assertThat(clock.millis()).isEqualTo(clock.millis());
    }

    @Test
    void advanceMOVESItByExactlyWhatItWasGiven() {
        SimulatedClock clock = new SimulatedClock(1_000L);

        clock.advance(Duration.ofSeconds(11));
        assertThat(clock.millis())
                .as("11 seconds is 11,000 millis on top of 1,000 -- a no-op `advance` leaves "
                        + "1,000, and every TTL expiry in the simulation stops happening")
                .isEqualTo(12_000L);

        clock.advance(Duration.ofMillis(5));
        assertThat(clock.millis()).as("and advances accumulate").isEqualTo(12_005L);
        assertThat(clock.instant().toEpochMilli())
                .as("instant() agrees with millis(), or a LeaseManager reading one and a test "
                        + "reading the other disagree about whether a lease expired")
                .isEqualTo(12_005L);
    }

    @Test
    void withZoneReturnsACLOCKCarryingTheGivenZoneAndTheSAMETime() {
        // ⚠️ IT USED TO RETURN `this`, violating `Clock`'s contract: the caller
        // asked for a different zone and got an object that reports the old one
        // while agreeing it was re-zoned. Nothing here re-zones a clock today,
        // which is precisely why the defect would have been found the hard way.
        SimulatedClock clock = new SimulatedClock(7_000L);

        java.time.Clock zoned = clock.withZone(ZoneId.of("Australia/Sydney"));
        assertThat(zoned.getZone())
                .as("the copy carries the zone it was ASKED for")
                .isEqualTo(ZoneId.of("Australia/Sydney"));
        assertThat(clock.getZone())
                .as("and the original is unchanged").isEqualTo(ZoneOffset.UTC);
        assertThat(zoned.millis()).as("with the same time").isEqualTo(7_000L);

        // ⚠️ AND THE COPY'S OWN `withZone` IS CONSTRAINED TOO, because the fix for
        // the outer one added a second implementation and review measured it
        // unconstrained: making the copy return `this` left all four tests green.
        // That is the same contract defect this test exists to close, one level
        // down, under a javadoc claiming Clock-contract conformance.
        assertThat(zoned.withZone(ZoneId.of("Asia/Tokyo")).getZone())
                .as("re-zoning the copy AGAIN yields the newly asked-for zone, not the copy's")
                .isEqualTo(ZoneId.of("Asia/Tokyo"));
    }

    @Test
    void aReZonedCopySTILLMovesWhenTheOriginalAdvances() {
        // ⚠️ THE COPY DELEGATES rather than snapshotting, and that is the choice
        // that matters for a deterministic harness: two clocks that could drift
        // apart would let one part of a simulation believe a lease had expired
        // while another did not, from the same seed.
        // ⚠️ A DIFFERENT ZONE, and that identifier is the whole test. Re-zoning to
        // `ZoneOffset.UTC` -- which is what `getZone()` already returns -- makes
        // this pass for EVERY conformant implementation and for the bug too:
        // review measured that with `withZone` restored to `return this` only the
        // zone test failed and this one stayed green, and that the JDK's own shape
        // (`if (zone.equals(getZone())) return this; return Clock.fixed(instant(),
        // zone);`) -- a copy that SNAPSHOTS and therefore drifts -- passed all
        // four. A test for delegation that re-zones to the zone it already has
        // cannot observe delegation.
        SimulatedClock clock = new SimulatedClock(100L);
        java.time.Clock zoned = clock.withZone(ZoneId.of("Australia/Sydney"));

        clock.advance(Duration.ofMillis(900));

        assertThat(zoned.millis())
                .as("the re-zoned view followed the advance -- a snapshot would still say 100 "
                        + "and the two would disagree about the same instant")
                .isEqualTo(1_000L);
        assertThat(zoned.instant().toEpochMilli()).isEqualTo(1_000L);
    }
}
