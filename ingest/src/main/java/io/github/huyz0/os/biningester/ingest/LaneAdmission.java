// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import java.math.BigInteger;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Weighted fair-share admission of {@code _bulk} requests over the pod's
 * in-flight budget (FR-18, ADR-0074 decision 6).
 *
 * <p>Lane {@code l}'s share is {@code budget × 2^l ÷ Σ_active 2^k} and its
 * FLOOR is {@code max(1, ⌊share ÷ 2⌋)}. A request is admitted if the pod's
 * total in flight is below the budget, OR its lane's own in flight is below
 * its floor; otherwise it is refused, which the front door answers {@code 429}
 * -- never a 5xx, because nothing failed: the pod is busy, and a producer
 * already retries 429 (OpenSearch bulk semantics).
 *
 * <p>⚠️ **NOT STRICT PRIORITY.** A saturated pod refuses lane {@code +2} too,
 * once it holds its floor; ADR-0074 rejects strict priority by ADR-0014's
 * anti-starvation rule. And the floor is a GUARANTEE, not a cap: under budget
 * every lane is admitted however far past its floor it is. So the hard
 * ceiling is the budget plus the sum of floors.
 *
 * <p>⚠️ **AN INACTIVE LANE HAS NO FLOOR** and is admitted only under budget.
 * Whether a lane is active is the ingester's decision (M10.6), and the front
 * door asks it BEFORE asking here, so a producer naming an inactive lane is
 * answered its permanent 400 even while the pod is saturated, never a 429 it
 * would retry. This rule covers only a caller that did not ask.
 *
 * <p>⚠️ **IT COUNTS REQUESTS, NOT BYTES OR RECORDS**, and touches no clock or
 * socket: a permit is taken when a request is admitted and returned when it
 * completes, however it completes.
 */
public final class LaneAdmission {

    /** ADR-0074's default {@code ingest.admission.maxInFlightBulk}. */
    public static final int DEFAULT_MAX_IN_FLIGHT_BULK = 256;

    private final int budget;
    private final byte lowest;
    /** Indexed by {@code lane - lowest}; 0 for a lane outside the active set. */
    private final int[] floors;
    private final int[] inLane;
    private final ReentrantLock lock = new ReentrantLock();
    private int inFlight;

    /**
     * @throws IllegalArgumentException if {@code budget} is below 1
     */
    public LaneAdmission(int budget, LaneSet lanes) {
        Objects.requireNonNull(lanes, "lanes");
        if (budget < 1) {
            throw new IllegalArgumentException("maxInFlightBulk admits at least one request; got "
                    + budget);
        }
        this.budget = budget;
        byte[] active = lanes.lanes();
        this.lowest = active[0];
        int span = active[active.length - 1] - lowest + 1;
        this.floors = new int[span];
        this.inLane = new int[span];
        // ⚠️ WEIGHTS SCALED BY 2^-lowest, so every one is an integer and the
        // floor is exact: ⌊budget × w ÷ (2 × Σw)⌋ is ⌊share ÷ 2⌋ with no
        // intermediate rounding. ⚠️ IN BigInteger, ONCE, AT CONSTRUCTION:
        // `LaneSet` admits any i8 lanes up to +2, so a set such as {-128, 0}
        // spans 2^130 -- a `long` shift wraps modulo 64 and `budget × w`
        // overflows, which (review measured) gave lane -63 of {-63, 0} a floor
        // of 128 and lane 0 a floor of 1: priority inverted. A floor is at
        // most ⌊budget ÷ 2⌋, so the result always fits an int.
        BigInteger sum = BigInteger.ZERO;
        for (byte lane : active) {
            sum = sum.add(BigInteger.ONE.shiftLeft(lane - lowest));
        }
        BigInteger twiceSum = sum.shiftLeft(1);
        for (byte lane : active) {
            int floor = BigInteger.valueOf(budget).shiftLeft(lane - lowest)
                    .divide(twiceSum).intValueExact();
            floors[lane - lowest] = Math.max(1, floor);
        }
    }

    /**
     * Admits one request of {@code lane}, or refuses it.
     *
     * @return a permit to {@link Permit#release} when the request completes,
     *     or empty if the budget is saturated and {@code lane} holds its floor
     */
    public Optional<Permit> tryAcquire(byte lane) {
        int slot = lane - lowest;
        boolean active = slot >= 0 && slot < floors.length && floors[slot] > 0;
        lock.lock();
        try {
            if (inFlight >= budget && (!active || inLane[slot] >= floors[slot])) {
                return Optional.empty();
            }
            inFlight++;
            if (active) {
                inLane[slot]++;
            }
        } finally {
            lock.unlock();
        }
        return Optional.of(new Permit(active ? slot : -1));
    }

    /** The requests admitted and not yet released, across every lane. */
    public int inFlight() {
        lock.lock();
        try {
            return inFlight;
        } finally {
            lock.unlock();
        }
    }

    /** One admitted request's place in the budget; releasing twice returns it once. */
    public final class Permit implements AutoCloseable {

        private final int slot;
        private boolean released;

        private Permit(int slot) {
            this.slot = slot;
        }

        public void release() {
            lock.lock();
            try {
                if (released) {
                    return;
                }
                released = true;
                inFlight--;
                if (slot >= 0) {
                    inLane[slot]--;
                }
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void close() {
            release();
        }
    }
}
