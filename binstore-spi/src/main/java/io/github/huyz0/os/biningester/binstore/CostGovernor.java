// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * The pod's cost governor: counting turned into refusing (M10.10, FR-21,
 * ADR-0075, cost.md rules 12-15).
 *
 * <p>⚠️ **ONLY UNDECLARED LIST IS EVER REFUSED**, by a token bucket; a LIST in
 * a {@link GovernorScope#recovery} scope is counted and admitted. Nothing here
 * can refuse a write: a data or commit PUT refused is data loss (rule 14).
 *
 * <p>⚠️ **RATIO-TO-EXPECTED, PER WINDOW, FOR DATA PUTS.**
 * {@code expected = max(dataBytes ÷ segmentBytes, window ÷ spacing)}, where
 * {@code spacing} is the pod's flush spacing IN FORCE -- the smallest value
 * the supplier reported during the window, so a positive lane's shorter
 * spacing (ADR-0074) is never read as a regression. At 3× it alarms, at 10×
 * {@link #discretionaryAllowed()} is false for as long as it holds, and at
 * 100× the kill switch trips and HOLDS until {@link #reset()}: silently
 * resuming after a runaway is what research 15 §5 forbids.
 *
 * <p>⚠️ **IT TRUSTS THE SPACING IT IS TOLD.** A controller that holds its own
 * interval at the floor is invisible here; NFR-1's counted low-rate bound is
 * what catches that one.
 *
 * <p>⚠️ The clock is INJECTED (non-negotiable 7): windows and the bucket are
 * advanced by reading it, never by a timer, so an idle pod does no work here.
 */
public final class CostGovernor {

    /** Ratio at which the governor alarms. */
    public static final double ALARM = 3.0;
    /** Ratio at which discretionary work halts. */
    public static final double HALT = 10.0;
    /** Ratio at which the kill switch trips. */
    public static final double KILL = 100.0;

    private static final System.Logger LOG = System.getLogger(CostGovernor.class.getName());

    /**
     * @param listsPerSecond the LIST bucket's sustained refill rate
     * @param listBurst the LIST bucket's capacity
     * @param window the ratio's evaluation window
     * @param segmentBytes the target segment size, the size-triggered term's divisor
     */
    public record Settings(double listsPerSecond, int listBurst, Duration window,
            long segmentBytes) {
        public Settings {
            Objects.requireNonNull(window, "window");
            if (listsPerSecond <= 0 || listBurst < 1 || window.isNegative() || window.isZero()
                    || segmentBytes <= 0) {
                throw new IllegalArgumentException("a governor needs a positive LIST rate, a "
                        + "burst of at least 1, a window and a segment size");
            }
        }

        /** ADR-0075's defaults: 1 LIST/s sustained, 300 burst, a one-minute window. */
        public static Settings defaults(long segmentBytes) {
            return new Settings(1.0, 300, Duration.ofMinutes(1), segmentBytes);
        }
    }

    /**
     * Refusals per class, and the declared recovery LISTs admitted.
     *
     * <p>⚠️ ATTRIBUTION IS {@code (op, purpose, domain)} BY CONSTRUCTION: one
     * governor per pod, and a pod serves one trust domain, so the domain is
     * the pod's; {@code listRefusals} is op LIST with purpose undeclared,
     * {@code recoveryLists} op LIST with purpose recovery, and
     * {@code discretionaryRefusals} the sweep/prefetch purpose refused before
     * it issued any op. The per-INDEX half is M11's (ADR-0075).
     */
    public record Counts(long listRefusals, long discretionaryRefusals, long recoveryLists) {
    }

    /**
     * Told of each refusal as it happens, so a pod can export it (M10.27,
     * cost.md rule 17): the counts alone are read only when asked.
     *
     * <p>⚠️ **CALLED WHILE THE GOVERNOR's MONITOR IS HELD** for a LIST refusal
     * and a discretionary one alike, so an implementation must be quick and
     * must not call back into the governor. Incrementing a counter is both.
     */
    public interface RefusalListener {
        /** One undeclared LIST was refused. */
        void listRefused();

        /** One request to start discretionary work was refused. */
        void discretionaryRefused();
    }

    private static final RefusalListener SILENT = new RefusalListener() {
        @Override
        public void listRefused() {
        }

        @Override
        public void discretionaryRefused() {
        }
    };

    private volatile RefusalListener refusals = SILENT;

    private final Settings settings;
    private final Clock clock;
    private final LongSupplier spacingMillis;
    private final long windowMillis;

    private double tokens;
    private long lastRefillMillis;

    private long windowStartMillis;
    private long windowPuts;
    private long windowBytes;
    private long windowMinSpacing;

    private double lastRatio;
    private boolean alarmed;
    private boolean halted;
    private boolean killed;

    private long listRefusals;
    private long discretionaryRefusals;
    private long recoveryLists;

    /**
     * @param flushSpacingMillis the pod's flush spacing in force:
     *     {@code max(floor, interval ÷ 2^L)}. ⚠️ It is sampled at each data PUT,
     *     i.e. AFTER the flush has drained the buffer, so {@code L} must be the
     *     highest positive lane of the flush IN PROGRESS (or buffered since the
     *     last flush), not of what is buffered now -- a supplier reading the
     *     emptied buffer would report the ceiling and turn a legitimate lane
     *     {@code +2} into a routine 4× alarm
     */
    public CostGovernor(Settings settings, Clock clock, LongSupplier flushSpacingMillis) {
        this.settings = Objects.requireNonNull(settings, "settings");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.spacingMillis = Objects.requireNonNull(flushSpacingMillis, "flushSpacingMillis");
        this.windowMillis = settings.window().toMillis();
        long now = clock.millis();
        this.tokens = settings.listBurst();
        this.lastRefillMillis = now;
        this.windowStartMillis = now;
        this.windowMinSpacing = Long.MAX_VALUE;
    }

    /** Whether one LIST may go to the store now; a refusal is counted. */
    public boolean admitList() {
        if (GovernorScope.inRecovery()) {
            synchronized (this) {
                recoveryLists++;
            }
            return true;
        }
        synchronized (this) {
            long now = clock.millis();
            roll(now);
            tokens = Math.min(settings.listBurst(),
                    tokens + (now - lastRefillMillis) * settings.listsPerSecond() / 1000.0);
            lastRefillMillis = now;
            if (tokens >= 1.0) {
                tokens -= 1.0;
                return true;
            }
            listRefusals++;
            refusals.listRefused();
            return false;
        }
    }

    /** Records one data-segment PUT of {@code bytes}. Never refuses it. */
    public synchronized void recordDataPut(long bytes) {
        roll(clock.millis());
        windowPuts++;
        windowBytes += Math.max(0, bytes);
        windowMinSpacing = Math.min(windowMinSpacing, sampleSpacing());
    }

    /**
     * Whether discretionary work -- the sweeps, the prefetch -- may start now.
     * A {@code false} is counted as a refusal of that work.
     */
    public synchronized boolean discretionaryAllowed() {
        roll(clock.millis());
        boolean allowed = !killed && !halted;
        if (!allowed) {
            discretionaryRefusals++;
            refusals.discretionaryRefused();
        }
        return allowed;
    }

    /** The ratio of the last completed window. */
    public synchronized double lastRatio() {
        roll(clock.millis());
        return lastRatio;
    }

    /** Whether the last completed window was at or above {@link #ALARM}. */
    public synchronized boolean alarmed() {
        roll(clock.millis());
        return alarmed;
    }

    /** Whether the kill switch has tripped and not been reset. */
    public synchronized boolean killSwitchTripped() {
        roll(clock.millis());
        return killed;
    }

    /** Clears the kill switch and the halt: an operator's decision, never automatic. */
    public synchronized void reset() {
        killed = false;
        halted = false;
        alarmed = false;
        LOG.log(System.Logger.Level.WARNING, "cost governor reset: discretionary work resumes");
    }

    /** Replaces the listener told of each refusal; one per governor. */
    public void onRefusal(RefusalListener listener) {
        this.refusals = Objects.requireNonNull(listener, "listener");
    }

    public synchronized Counts counts() {
        return new Counts(listRefusals, discretionaryRefusals, recoveryLists);
    }

    private long sampleSpacing() {
        return Math.max(1, spacingMillis.getAsLong());
    }

    private void roll(long now) {
        long elapsed = now - windowStartMillis;
        if (elapsed < windowMillis) {
            return;
        }
        evaluate();
        long windows = elapsed / windowMillis;
        windowStartMillis += windows * windowMillis;
        if (windows > 1) {
            // ⚠️ THE WINDOWS BETWEEN WERE EMPTY, and an empty window is a
            // ratio of 0: a halt lifts, a tripped kill switch does not.
            lastRatio = 0;
            alarmed = false;
            halted = false;
        }
        windowPuts = 0;
        windowBytes = 0;
        // ⚠️ UNSET, NOT SAMPLED HERE: a roll happens at whatever call first
        // follows the boundary, when the supplier may still report the last
        // window's regime -- a 250 ms lane spacing carried into a quiet window
        // would read a 50x regression as 2.5x. Only a flush samples it.
        windowMinSpacing = Long.MAX_VALUE;
    }

    private void evaluate() {
        if (windowPuts == 0) {
            lastRatio = 0;
            alarmed = false;
            halted = false;
            return;
        }
        double expected = Math.max((double) windowBytes / settings.segmentBytes(),
                (double) windowMillis / windowMinSpacing);
        lastRatio = windowPuts / expected;
        alarmed = lastRatio >= ALARM;
        halted = lastRatio >= HALT;
        if (lastRatio >= KILL && !killed) {
            killed = true;
            LOG.log(System.Logger.Level.ERROR, "cost governor KILL SWITCH tripped: " + windowPuts
                    + " data PUTs against " + expected + " expected; discretionary work halts "
                    + "until reset");
        } else if (alarmed) {
            LOG.log(System.Logger.Level.WARNING, "cost governor alarm: data PUTs at "
                    + lastRatio + "x expected" + (halted ? "; discretionary work halted" : ""));
        }
    }
}
