// SPDX-License-Identifier: Apache-2.0
package binjava.bench;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/** Paces records against an absolute schedule supplied by the benchmark. */
public final class RatePacer {
  private final Clock clock;
  private final Sleeper sleeper;
  private final Instant startedAt;
  private final long nanosPerRecord;

  public RatePacer(double recordsPerSecond, Clock clock, Sleeper sleeper) {
    if (!Double.isFinite(recordsPerSecond) || recordsPerSecond <= 0.0) {
      throw new IllegalArgumentException("recordsPerSecond must be positive and finite");
    }
    this.clock = Objects.requireNonNull(clock, "clock");
    this.sleeper = Objects.requireNonNull(sleeper, "sleeper");
    startedAt = clock.instant();
    nanosPerRecord = Math.max(1L, Math.round(1_000_000_000.0 / recordsPerSecond));
  }

  public void awaitTurn(long recordsEmitted) throws InterruptedException {
    if (recordsEmitted < 0) {
      throw new IllegalArgumentException("recordsEmitted must not be negative");
    }
    long dueNanos = Math.multiplyExact(recordsEmitted, nanosPerRecord);
    long elapsedNanos = Duration.between(startedAt, clock.instant()).toNanos();
    long remainingNanos = dueNanos - elapsedNanos;
    if (remainingNanos > 0) {
      sleeper.sleepNanos(remainingNanos);
    }
  }
}
