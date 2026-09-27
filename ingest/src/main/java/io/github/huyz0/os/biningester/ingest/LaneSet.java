// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import java.util.Arrays;
import java.util.Objects;

/**
 * The pod's ACTIVE priority lanes (FR-18, ADR-0074): the values a producer's
 * {@code lane} parameter may take.
 *
 * <p>⚠️ **THREE RULES, ALL REFUSED AT CONFIGURATION.** At most 8 lanes (FR-18's
 * cap: every lane is a scheduling class). Lane 0 is always active, because it
 * is where a producer that says nothing lands (ADR-0014). And no lane above
 * {@code +2}: lane {@code +l} may flush up to {@code 2^l} times per interval
 * ceiling, so {@code +2} is what holds NFR-1's amended low-rate bound at 8
 * data+commit PUTs per pod per ceiling.
 *
 * <p>⚠️ PER POD, NOT PER INDEX as ADR-0014 wrote it: the catalog registering
 * index shapes carries no lane configuration (ADR-0074 states the amendment).
 */
public final class LaneSet {

    /** FR-18's cap on active lanes. */
    public static final int MAX_LANES = 8;

    /** The highest lane NFR-1's amended bound allows (ADR-0074). */
    public static final byte HIGHEST_ALLOWED = 2;

    private static final LaneSet DEFAULTS =
            of((byte) -2, (byte) -1, (byte) 0, (byte) 1, (byte) 2);

    /** Sorted, distinct. */
    private final byte[] lanes;

    private LaneSet(byte[] lanes) {
        this.lanes = lanes;
    }

    /** ADR-0074's default: {@code -2..2}. */
    public static LaneSet defaults() {
        return DEFAULTS;
    }

    /**
     * @throws IllegalArgumentException if the set has more than 8 lanes, lacks
     *     0, or holds a lane above {@code +2}
     */
    public static LaneSet of(byte... lanes) {
        Objects.requireNonNull(lanes, "lanes");
        byte[] sorted = lanes.clone();
        Arrays.sort(sorted);
        int n = 0;
        for (int i = 0; i < sorted.length; i++) {
            if (i == 0 || sorted[i] != sorted[i - 1]) {
                sorted[n++] = sorted[i];
            }
        }
        byte[] distinct = Arrays.copyOf(sorted, n);
        if (distinct.length == 0) {
            throw new IllegalArgumentException("lane 0 is always active: an empty set has none");
        }
        if (distinct.length > MAX_LANES) {
            throw new IllegalArgumentException("at most " + MAX_LANES + " lanes may be active "
                    + "(FR-18); got " + distinct.length);
        }
        if (Arrays.binarySearch(distinct, (byte) 0) < 0) {
            throw new IllegalArgumentException("lane 0 is always active: it is where a producer "
                    + "that names no lane lands (ADR-0014)");
        }
        if (distinct[distinct.length - 1] > HIGHEST_ALLOWED) {
            throw new IllegalArgumentException("no lane above +" + HIGHEST_ALLOWED + " may be "
                    + "active: lane +l spends up to 2^l flushes per interval ceiling, and "
                    + "NFR-1's bound holds only to +" + HIGHEST_ALLOWED + " (ADR-0074); got +"
                    + distinct[distinct.length - 1]);
        }
        return new LaneSet(distinct);
    }

    /**
     * Parses a comma-separated list, e.g. {@code "-2,-1,0,1,2"}.
     *
     * @throws IllegalArgumentException if an entry is not an integer in
     *     {@code i8}, or the set breaks a rule of {@link #of}
     */
    public static LaneSet parse(String text) {
        Objects.requireNonNull(text, "text");
        String[] parts = text.split(",");
        byte[] lanes = new byte[parts.length];
        for (int i = 0; i < parts.length; i++) {
            String part = parts[i].trim();
            int value;
            try {
                value = Integer.parseInt(part);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("a lane is an integer; got '" + part + "'");
            }
            if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                throw new IllegalArgumentException("a lane is a signed byte; got " + value);
            }
            lanes[i] = (byte) value;
        }
        return of(lanes);
    }

    public boolean contains(byte lane) {
        return Arrays.binarySearch(lanes, lane) >= 0;
    }

    /** The highest active lane. */
    public byte highest() {
        return lanes[lanes.length - 1];
    }

    /** The active lanes, ascending. */
    public byte[] lanes() {
        return lanes.clone();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof LaneSet that && Arrays.equals(lanes, that.lanes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(lanes);
    }

    @Override
    public String toString() {
        return Arrays.toString(lanes);
    }
}
