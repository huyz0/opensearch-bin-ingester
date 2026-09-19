// SPDX-License-Identifier: Apache-2.0
package binjava.server.chaos;

import java.time.Clock;
import java.time.Instant;

/**
 * Prints what a process's DIRECT wall-clock reads return, one per line, for
 * {@link ChaosNetworkIT} to compare with true time (M8.23).
 *
 * <p>⚠️ **THE READS A LEASE COMPARISON COULD MAKE WITHOUT THE SEAM.** A skew
 * observed only in what the ingester writes goes through the {@code Clock} it
 * was handed, which is the one read that is not in question.
 */
public final class SkewProbe {

    private SkewProbe() {
    }

    public static void main(String[] args) {
        System.out.println("currentTimeMillis " + System.currentTimeMillis());
        System.out.println("Instant.now " + Instant.now().toEpochMilli());
        System.out.println("Clock.systemUTC " + Clock.systemUTC().millis());
    }
}
