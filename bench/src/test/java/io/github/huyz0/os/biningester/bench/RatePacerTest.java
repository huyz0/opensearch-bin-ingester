// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * M9.3. The rate is a parameter, and the pacing that enforces it takes a clock
 * and a sleeper as seams (non-negotiable 7): the arithmetic that decides how
 * long to wait is what is worth pinning, and a {@code Thread.sleep} inside it
 * would make that arithmetic testable only in wall-clock time.
 */
class RatePacerTest {

  /** A clock the test advances by hand. */
  private static final class FakeClock extends Clock {
    private Instant now = Instant.parse("2026-09-20T00:00:00Z");

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

  /** Records what it was asked to sleep and advances the clock by exactly that. */
  private static final class RecordingSleeper implements Sleeper {
    private final FakeClock clock;
    private final List<Long> sleeps = new ArrayList<>();

    RecordingSleeper(FakeClock clock) {
      this.clock = clock;
    }

    @Override
    public void sleepNanos(long nanos) {
      sleeps.add(nanos);
      clock.now = clock.now.plusNanos(nanos);
    }
  }

  @Test
  void itSleepsUntilTheRecordIsDueAtTheConfiguredRate() throws Exception {
    FakeClock clock = new FakeClock();
    RecordingSleeper sleeper = new RecordingSleeper(clock);
    RatePacer pacer = new RatePacer(1_000.0, clock, sleeper);

    pacer.awaitTurn(0);
    pacer.awaitTurn(1);
    pacer.awaitTurn(2);

    // 1,000 records per second is one per millisecond, and record 0 is due
    // immediately.
    assertThat(sleeper.sleeps).containsExactly(1_000_000L, 1_000_000L);
  }

  @Test
  void aSlowerRateSleepsProportionallyLonger() throws Exception {
    FakeClock clock = new FakeClock();
    RecordingSleeper sleeper = new RecordingSleeper(clock);
    RatePacer pacer = new RatePacer(4.0, clock, sleeper);

    pacer.awaitTurn(0);
    pacer.awaitTurn(1);

    assertThat(sleeper.sleeps).containsExactly(250_000_000L);
  }

  @Test
  void itDoesNotSleepWhenAlreadyBehindSchedule() throws Exception {
    FakeClock clock = new FakeClock();
    RecordingSleeper sleeper = new RecordingSleeper(clock);
    RatePacer pacer = new RatePacer(1_000.0, clock, sleeper);

    pacer.awaitTurn(0);
    // Two seconds of work for a record due one millisecond in: a pacer that
    // slept here would be sleeping to catch up with its own lateness.
    clock.now = clock.now.plusSeconds(2);
    pacer.awaitTurn(1);

    assertThat(sleeper.sleeps).isEmpty();
  }

  @Test
  void theDeadlineIsAbsoluteSoLatenessIsNotCompounded() throws Exception {
    FakeClock clock = new FakeClock();
    RecordingSleeper sleeper = new RecordingSleeper(clock);
    RatePacer pacer = new RatePacer(1_000.0, clock, sleeper);

    pacer.awaitTurn(0);
    clock.now = clock.now.plusNanos(2_500_000); // 2.5 ms late
    pacer.awaitTurn(1); // due at 1 ms -- already past
    pacer.awaitTurn(2); // due at 2 ms -- already past
    pacer.awaitTurn(3); // due at 3 ms -- 500 us away

    assertThat(sleeper.sleeps).containsExactly(500_000L);
  }

  @Test
  void aNonPositiveRateIsRefused() {
    FakeClock clock = new FakeClock();
    assertThatThrownBy(() -> new RatePacer(0.0, clock, new RecordingSleeper(clock)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("recordsPerSecond");
  }
}
