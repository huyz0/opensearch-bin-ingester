// SPDX-License-Identifier: Apache-2.0
package binjava.server;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.server.ShutdownSequence.Step;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The shutdown runs in research 08 §7's order (M8.7, criterion 5).
 *
 * <p>⚠️ **THE ORDER IS ASSERTED AS A SEQUENCE OF EVENTS, NOT AS A CLEAN
 * EXIT.** Releasing the lease before the subscribers are told exits just as
 * cleanly, and it produces the stall §7 exists to prevent.
 */
class ShutdownOrderTest {

    /** A clock that moves 10 ms every time it is read. */
    private static final class SteppingClock extends Clock {
        private long millis = 1_000;

        @Override
        public long millis() {
            millis += 10;
            return millis;
        }

        @Override
        public Instant instant() {
            return Instant.ofEpochMilli(millis());
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

    /** Steps that record themselves, and throw where told to. */
    private static final class Recording implements ShutdownSequence.Steps {
        final List<String> events = new ArrayList<>();
        Step failing;

        private void step(Step step, String event) throws IOException {
            events.add(event);
            if (step == failing) {
                throw new IOException(event + " failed");
            }
        }

        @Override
        public void failReadiness() throws IOException {
            step(Step.FAIL_READINESS, "readiness failed");
        }

        @Override
        public void releaseSubscribers() throws IOException {
            step(Step.RELEASE_SUBSCRIBERS, "subscribers told");
        }

        @Override
        public void finishInFlight() throws IOException {
            step(Step.FINISH_IN_FLIGHT, "in-flight finished");
        }

        @Override
        public void flushAndCommit() throws IOException {
            step(Step.FLUSH_AND_COMMIT, "flushed");
        }

        @Override
        public void releaseLeases() throws IOException {
            step(Step.RELEASE_LEASES, "lease released");
        }
    }

    @Test
    void theSTEPSRunInSECTION7sOrder() {
        Recording steps = new Recording();

        ShutdownSequence.Report report = ShutdownSequence.run(steps, new SteppingClock());

        assertThat(steps.events)
                .as("⚠️ READINESS, SUBSCRIBERS, IN-FLIGHT, FLUSH, THEN THE LEASE -- a release "
                        + "before the subscribers are told exits just as cleanly and stalls "
                        + "every consumer for a TTL")
                .containsExactly("readiness failed", "subscribers told", "in-flight finished",
                        "flushed", "lease released");
        assertThat(report.failures()).isEmpty();
        assertThat(report.ran()).as("⚠️ AND THE REPORT SAYS SO, in the order they ran")
                .containsExactly(Step.values());
        assertThat(report.lines())
                .as("⚠️ WHAT `Main` PRINTS, for an operator and for ShutdownDrainIT")
                .containsExactly(
                        "shutdown step 1 FAIL_READINESS took 10 ms",
                        "shutdown step 2 RELEASE_SUBSCRIBERS took 10 ms",
                        "shutdown step 3 FINISH_IN_FLIGHT took 10 ms",
                        "shutdown step 4 FLUSH_AND_COMMIT took 10 ms",
                        "shutdown step 5 RELEASE_LEASES took 10 ms",
                        "graceful shutdown took 110 ms");
    }

    @Test
    void aFAILEDFlushStillRELEASESTheLease() {
        // ⚠️ THE LEASE IS WHY EVERY STEP RUNS. A flush that throws must not
        // leave the term held, or the pod that replaces this one waits out
        // the TTL on every deploy.
        for (Step failing : Step.values()) {
            Recording steps = new Recording();
            steps.failing = failing;

            ShutdownSequence.Report report = ShutdownSequence.run(steps, new SteppingClock());

            assertThat(steps.events).as("with %s failing", failing).hasSize(5)
                    .last().isEqualTo("lease released");
            assertThat(report.failures()).as("with %s failing", failing).hasSize(1);
        }
    }

    @Test
    void everySTEPIsTIMEDAndTheTOTALIsReportedAsANUMBER() {
        // ⚠️ `terminationGracePeriodSeconds` is set from this number, so it
        // is measured on the clock rather than asserted to exist. The clock
        // moves 10 ms per read: a step read twice took 10 ms.
        ShutdownSequence.Report report =
                ShutdownSequence.run(new Recording(), new SteppingClock());

        assertThat(report.took()).containsOnlyKeys(Step.values());
        assertThat(report.took().values()).allMatch(d -> d.equals(Duration.ofMillis(10)));
        assertThat(report.total())
                .as("⚠️ THE WHOLE SEQUENCE, from the first read to the last")
                .isEqualTo(Duration.ofMillis(10 + 5 * 20));
    }

    @Test
    void anINTERRUPTEDWaitDoesNotFailEveryLATERStepAndIsRESTOREDAtTheEnd() {
        // ⚠️ Restored at once, every later wait, flush and lease write would
        // fail on the flag, and the lease must still be released.
        List<Boolean> interruptedDuring = new ArrayList<>();
        ShutdownSequence.Steps interrupting = new ShutdownSequence.Steps() {
            @Override
            public void failReadiness() {
            }

            @Override
            public void releaseSubscribers() throws InterruptedException {
                throw new InterruptedException("the wait for subscribers was interrupted");
            }

            @Override
            public void finishInFlight() {
                interruptedDuring.add(Thread.currentThread().isInterrupted());
            }

            @Override
            public void flushAndCommit() {
                interruptedDuring.add(Thread.currentThread().isInterrupted());
            }

            @Override
            public void releaseLeases() {
                interruptedDuring.add(Thread.currentThread().isInterrupted());
            }
        };

        ShutdownSequence.Report report = ShutdownSequence.run(interrupting, new SteppingClock());
        boolean restored = Thread.interrupted();

        assertThat(interruptedDuring).as("⚠️ THE LATER STEPS RAN WITH THE FLAG CLEAR")
                .containsExactly(false, false, false);
        assertThat(report.failures()).hasSize(1);
        assertThat(restored).as("⚠️ AND THE CALLER STILL LEARNS IT WAS INTERRUPTED").isTrue();
    }
}
