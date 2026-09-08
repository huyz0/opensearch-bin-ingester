// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * ⚠️ A clock the simulation ADVANCES, so lease expiry is an event the seed
 * chooses rather than something a test waits for.
 *
 * <p>Without it every takeover in this module happens via a voluntary
 * {@code close()}, which is the polite path — the one where the old leader is
 * already gone. The interesting failover is the OTHER one: a leader that stops
 * answering and whose lease runs out while it still believes it holds the term.
 * Only a movable clock reaches it, and `Thread.sleep` would trade determinism
 * for wall-clock time, which is what makes a failing seed a flake instead of a
 * regression.
 */
public final class SimulatedClock extends Clock {

    private long millis;

    public SimulatedClock(long startMillis) {
        this.millis = startMillis;
    }

    public void advance(Duration d) {
        millis += d.toMillis();
    }

    @Override public long millis() {
        return millis;
    }

    @Override public Instant instant() {
        return Instant.ofEpochMilli(millis);
    }

    @Override public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    /**
     * ⚠️ A REAL COPY, not {@code this}. {@link Clock}'s contract says
     * {@code withZone} returns a clock with the SAME time and the GIVEN zone;
     * returning {@code this} silently keeps the old zone, so a caller that
     * re-zones gets an object that agrees it was re-zoned and was not. Nothing in
     * this repository re-zones a clock today, which is exactly why it would have
     * been found the hard way.
     *
     * <p>⚠️ IT SHARES THIS CLOCK'S TIME, deliberately: the copy delegates to
     * {@code millis()} rather than snapshotting it, so {@link #advance} still
     * moves both. A snapshot would give a simulation two clocks that drift, which
     * is the one thing a deterministic harness must not have.
     */
    @Override public Clock withZone(ZoneId zone) {
        SimulatedClock outer = this;
        return new Clock() {
            @Override public ZoneId getZone() {
                return zone;
            }

            @Override public Clock withZone(ZoneId other) {
                return outer.withZone(other);
            }

            @Override public long millis() {
                return outer.millis();
            }

            @Override public Instant instant() {
                return Instant.ofEpochMilli(outer.millis());
            }
        };
    }
}
