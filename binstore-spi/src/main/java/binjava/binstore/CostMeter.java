// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

import java.time.Duration;
import java.util.Objects;
import java.util.OptionalDouble;

/**
 * One reading of what a store's requests cost: requests per MiB written and
 * read, the idle request rate, and $/TiB ingested (research 02 §7, cost rule
 * R9).
 *
 * <p>⚠️ IT LIVES IN {@code binstore-spi}, BESIDE {@link CountingBinStore},
 * because that is where the counts are made. A meter in the harness would be
 * unavailable to the server's own cost endpoint and to the plugin; a meter in
 * a module of its own would be one record and a division. Every module that
 * can see a {@link BinStore} can already see this.
 *
 * <p>⚠️ IT IS ARITHMETIC AND NOTHING ELSE (non-negotiable 7): no clock, no
 * socket, no store. The counts are handed to it, the byte totals are handed to
 * it, and the window the idle rate is taken over is a PARAMETER rather than
 * something read from a clock — which is also what makes every reading
 * reproducible and T0.
 *
 * <h2>What it deliberately does not model</h2>
 *
 * <p>⚠️ REQUESTS ONLY. {@link CostTable} prices requests, and this meter
 * multiplies counts by those prices. It does NOT model:
 *
 * <ul>
 *   <li><b>Storage</b> ($/GB-month). It is the larger term past ~3 h of
 *       retention (cost rule 8) and it is a function of retention and of the
 *       bytes resident, neither of which is a request count.</li>
 *   <li><b>Cross-AZ transfer</b> ($/GB each direction). NFR-5 is measured as
 *       BYTES on named transports, not inferred from store requests, and the
 *       price is a property of the DEPLOYMENT — the same reason
 *       {@code CostTable} carries no per-byte term.</li>
 *   <li><b>DELETE.</b> Free on S3, so deletes are counted and billed at
 *       nothing. A provider that charges for them needs a fourth price, which
 *       is a change to {@code CostTable} and therefore to
 *       {@link Capabilities}.</li>
 *   <li><b>Anything that is not a request this store issued</b> — compute,
 *       network to the producer, the OpenSearch cluster.</li>
 * </ul>
 *
 * <p>So the dollar figures here are the API bill, they are MODELLED rather
 * than billed, and the curve document that publishes them says so.
 *
 * <p>⚠️ AND A HEAD IS PRICED AS A GET. S3 bills the two alike, so
 * {@link StoreCounts#stats()} is multiplied by {@link CostTable#getPerThousand}
 * rather than going unpriced.
 *
 * @param counts what the store issued, from {@link CountingBinStore#counts()}
 * @param prices the per-request price table, injected — never AWS by
 *     construction. {@link CostTable#free()} is the right table for the
 *     local-FS and in-memory backends and bills nothing
 */
public record CostMeter(StoreCounts counts, CostTable prices) {

    private static final long BYTES_PER_MIB = 1024L * 1024L;
    private static final long BYTES_PER_TIB = 1024L * 1024L * 1024L * 1024L;

    private static final double NANOS_PER_SECOND = 1_000_000_000d;

    /** {@link CostTable} is quoted in micro-dollars. */
    private static final double MICRO_DOLLARS_PER_DOLLAR = 1_000_000d;

    /** {@link CostTable} prices 1,000 requests at a time. */
    private static final double REQUESTS_PER_PRICED_BLOCK = 1_000d;

    public CostMeter {
        Objects.requireNonNull(counts, "counts");
        Objects.requireNonNull(prices, "prices");
    }

    /**
     * The requests NFR-1 budgets: PUTs, conditional or not.
     *
     * <p>⚠️ LISTS AND DELETES ARE EXCLUDED ON PURPOSE. A LIST is a discovery
     * request whose own bound is an enumeration of the three recovery paths
     * that may issue one, and a DELETE belongs to the retention sweep and is
     * free; counting either here would let a recovery path spend the write
     * budget NFR-1 reserves for flushes and commits.
     */
    public long writeRequests() {
        return counts.puts();
    }

    /** GETs, ranged or whole-object, plus the HEADs billed like them. */
    public long readRequests() {
        return counts.gets() + counts.stats();
    }

    /**
     * Write requests per mebibyte written — NFR-1's ratio, budgeted below 0.30.
     *
     * <p>⚠️ MEBIBYTES (2^20), because every budget in this project is quoted in
     * MiB. Dividing by 10^6 reports every ratio 4.9% low, which is the
     * difference between 0.2288 and Scenario A's 0.24 against a bound of 0.30.
     *
     * <p>⚠️ ABSENT, NOT ZERO AND NOT NaN, WHEN NOTHING WAS WRITTEN. A silent
     * 0.0 reports an idle pod as the cheapest design ever measured and passes
     * {@code < 0.30} having divided by nothing; a NaN fails every comparison
     * silently and prints as a word in a cost report. Both are lies a reader
     * cannot see. An empty reading forces whoever renders it to say the ratio
     * is undefined.
     *
     * @param bytesWritten the bytes those requests carried, 0 when none were
     * @throws IllegalArgumentException if {@code bytesWritten} is negative
     */
    public OptionalDouble requestsPerMiBWritten(long bytesWritten) {
        return perMiB(writeRequests(), bytesWritten, "bytesWritten");
    }

    /**
     * Read requests — GETs AND HEADs — per mebibyte read; absent when no bytes
     * were read, for the reason {@link #requestsPerMiBWritten} gives.
     *
     * <p>⚠️ THIS IS NOT RESEARCH 02 §5's 1.08, WHICH COUNTS GETs ALONE. The
     * two agree exactly when nothing probes before it fetches, and diverge by
     * the HEAD rate when something does; a HEAD is a billed request, so the
     * cost reading includes it. ⚠️ M9.9 and M9.15 must therefore SAY WHICH
     * QUANTITY THEY PUBLISH — this ratio, or {@link StoreCounts#gets()} alone
     * against the same bytes — because a curve comparing one against the
     * other's budget would move without any behaviour changing.
     *
     * @throws IllegalArgumentException if {@code bytesRead} is negative
     */
    public OptionalDouble requestsPerMiBRead(long bytesRead) {
        return perMiB(readRequests(), bytesRead, "bytesRead");
    }

    /**
     * Every request, of every kind, per second of the window given — the
     * reading NFR-2 and cost rule R3 require to be zero for an idle pod.
     *
     * <p>⚠️ THE WINDOW IS A PARAMETER, NOT A CLOCK (non-negotiable 7). The
     * meter cannot ask what time it is, so a rate can only be taken by a
     * caller that recorded how long it watched.
     *
     * <p>⚠️ EVERY KIND, including DELETE: an idle pod issuing free requests is
     * still a pod doing something, and R3's failure mode — a poll loop
     * discovering nothing — is invisible to a rate that only counts the verbs
     * that cost money.
     *
     * @throws IllegalArgumentException if the window is zero or negative. That
     *     is the caller's bug rather than an absent reading: unlike a byte
     *     total, which a genuinely idle pod really does report as zero, the
     *     window is chosen by whoever took the measurement
     */
    public double idleRequestsPerSecond(Duration window) {
        Objects.requireNonNull(window, "window");
        // ⚠️ NANOSECONDS, not seconds: a 250 ms window is a real measurement
        // and `toSeconds()` would floor it to zero, turning every sub-second
        // reading into a division by nothing.
        long nanos = window.toNanos();
        if (nanos <= 0) {
            throw new IllegalArgumentException(
                    "a window of " + window + " measures no rate -- the window is the caller's, "
                            + "and a reading taken over no time is not an idle rate");
        }
        return (double) counts.total() * NANOS_PER_SECOND / (double) nanos;
    }

    /** What these requests cost, in dollars, under the injected price table. */
    public double estimatedUsd() {
        double microDollars = counts.puts() * (double) prices.putPerThousand()
                + counts.lists() * (double) prices.listPerThousand()
                // A HEAD is billed at the GET rate; a DELETE is free and the
                // table carries no price for it.
                + (counts.gets() + counts.stats()) * (double) prices.getPerThousand();
        return microDollars / REQUESTS_PER_PRICED_BLOCK / MICRO_DOLLARS_PER_DOLLAR;
    }

    /**
     * {@code estimated_usd_per_tib_ingested} (research 02 §7): what these
     * requests cost, scaled to one tebibyte of the bytes they carried.
     *
     * <p>⚠️ A RATE, NOT A TOTAL. Two runs costing the same dollars over
     * different volumes have different $/TiB, and that is the number a
     * capacity plan multiplies.
     *
     * @param bytesIngested producer bytes this reading covers; absent when 0
     * @throws IllegalArgumentException if {@code bytesIngested} is negative
     */
    public OptionalDouble estimatedUsdPerTibIngested(long bytesIngested) {
        requireNotNegative(bytesIngested, "bytesIngested");
        if (bytesIngested == 0) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of(estimatedUsd() * BYTES_PER_TIB / (double) bytesIngested);
    }

    private static OptionalDouble perMiB(long requests, long bytes, String name) {
        requireNotNegative(bytes, name);
        if (bytes == 0) {
            return OptionalDouble.empty();
        }
        return OptionalDouble.of((double) requests * BYTES_PER_MIB / (double) bytes);
    }

    private static void requireNotNegative(long bytes, String name) {
        if (bytes < 0) {
            throw new IllegalArgumentException(name + " of " + bytes + " is not a byte count");
        }
    }
}
