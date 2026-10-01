// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

/**
 * A monotonic clock in nanoseconds, injected (non-negotiable 7): tests advance
 * one by hand, and production will pass the JDK's from {@code Main} once fast
 * mode has a caller (the leader's sequencer, M13.27) -- none is wired yet.
 *
 * <p>⚠️ ONLY DIFFERENCES MEAN ANYTHING: the origin is arbitrary and the value
 * may be negative, so compare by subtraction, never by {@code <}.
 */
@FunctionalInterface
public interface MonotonicClock {

    long nanos();
}
