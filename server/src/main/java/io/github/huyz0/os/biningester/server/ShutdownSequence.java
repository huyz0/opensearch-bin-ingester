// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Graceful shutdown, in research 08 §7's order (M8.7, NFR-9).
 *
 * <pre>
 * SIGTERM
 *   1. fail readiness            the load balancer stops sending new requests
 *   2. tell subscribers to go    and wait briefly for them to leave
 *   3. finish in-flight ingest   refuse new bulk, flush, wait for the rest
 *   4. flush and commit          whatever raced in, PUT and committed
 *   5. release held leases       voluntary, so a successor need not wait a TTL
 * </pre>
 *
 * <p>⚠️ **THE ORDER IS THE PRODUCT, AND A CLEAN EXIT CANNOT SHOW IT.**
 * Releasing the lease before the subscribers are told exits just as cleanly,
 * and produces exactly the stall §7 exists to prevent: every consumer waits
 * out its poll and then reconnects at the same moment. So the order is a
 * property of this sequence, asserted against a recording seam, and the real
 * steps are plugged in by {@link IngesterNode}.
 *
 * <p>⚠️ **EVERY STEP RUNS EVEN IF AN EARLIER ONE THREW.** Step 5 is the
 * reason. A flush that fails must not leave the term held, or the pod that
 * replaces this one waits out the TTL on every deploy. The failures are
 * collected and reported, and the first one is what the caller throws.
 *
 * <p>⚠️ **EACH STEP IS TIMED, AND THE TOTAL IS REPORTED AS A NUMBER.**
 * {@code terminationGracePeriodSeconds} must exceed the worst case of steps
 * 2 to 5, and an operator can only set it from a measurement. §7 says to
 * budget about 30 s and measure it.
 */
public final class ShutdownSequence {

    /** The steps, in the order they run. */
    public enum Step {
        FAIL_READINESS,
        RELEASE_SUBSCRIBERS,
        FINISH_IN_FLIGHT,
        FLUSH_AND_COMMIT,
        RELEASE_LEASES
    }

    /** What each step does on a real node. */
    public interface Steps {
        void failReadiness() throws Exception;

        void releaseSubscribers() throws Exception;

        void finishInFlight() throws Exception;

        void flushAndCommit() throws Exception;

        void releaseLeases() throws Exception;
    }

    /**
     * How long each step took, the whole sequence, and what failed.
     *
     * @param ran ⚠️ the steps in the order they RAN, recorded as each one
     *     finished -- the observed order, which {@code took}'s key order is
     *     not
     * @param total ⚠️ the number {@code terminationGracePeriodSeconds} is set
     *     from
     */
    public record Report(List<Step> ran, Map<Step, Duration> took, Duration total,
            List<Exception> failures) {

        public Report {
            ran = List.copyOf(ran);
            took = Collections.unmodifiableMap(new EnumMap<>(took));
            Objects.requireNonNull(total, "total");
            failures = List.copyOf(failures);
        }

        /**
         * One line per step, in the order they ran, then the total.
         *
         * <p>⚠️ **PRINTED BY {@link Main}, NOT LOGGED.** MEASURED (M8.7): the
         * JDK's logging resets itself in its own shutdown hook, which runs
         * alongside the node's, and every step logged after the second was
         * lost.
         */
        public List<String> lines() {
            List<String> lines = new ArrayList<>();
            for (Step step : ran) {
                lines.add("shutdown step " + (step.ordinal() + 1) + " " + step + " took "
                        + took.get(step).toMillis() + " ms");
            }
            lines.add("graceful shutdown took " + total.toMillis() + " ms");
            return lines;
        }
    }

    private ShutdownSequence() {
    }

    /** Runs every step in order, and says how long each one took. */
    public static Report run(Steps steps, Clock clock) {
        Objects.requireNonNull(steps, "steps");
        Objects.requireNonNull(clock, "clock");
        List<Step> ran = new ArrayList<>();
        Map<Step, Duration> took = new EnumMap<>(Step.class);
        List<Exception> failures = new ArrayList<>();
        boolean interrupted = false;
        long started = clock.millis();
        for (Step step : Step.values()) {
            long before = clock.millis();
            try {
                switch (step) {
                    case FAIL_READINESS -> steps.failReadiness();
                    case RELEASE_SUBSCRIBERS -> steps.releaseSubscribers();
                    case FINISH_IN_FLIGHT -> steps.finishInFlight();
                    case FLUSH_AND_COMMIT -> steps.flushAndCommit();
                    case RELEASE_LEASES -> steps.releaseLeases();
                    default -> throw new IllegalStateException("no such step: " + step);
                }
            } catch (InterruptedException wait) {
                // ⚠️ CARRIED ON WITH THE FLAG CLEAR, AND RESTORED AT THE END.
                // Restored here, every later wait, flush and lease write
                // would fail at once, and the lease must still be released.
                interrupted = true;
                failures.add(wait);
            } catch (Exception failed) {
                failures.add(failed);
            }
            Duration spent = Duration.ofMillis(clock.millis() - before);
            took.put(step, spent);
            ran.add(step);
        }
        Report report = new Report(ran, took, Duration.ofMillis(clock.millis() - started),
                failures);
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        return report;
    }
}
