// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * M9 criterion 2: the meter computes what research 02 §7 names, T0.
 *
 * <p>⚠️ THE EXPECTED VALUES ARE EXACT RATIONALS, NEVER THE PROSE'S ROUNDING.
 * Research 02 §5 is the source of the INPUTS — 24 write requests per second
 * against 100 MiB/s, 108 read requests per second against the same — and the
 * expected value is the division done here: 24/100 is 0.24 and nothing else.
 * Asserting "about 0.24" against two significant figures of prose would pass
 * for a meter dividing by 10^6 instead of 2^20, which is 0.2288, or for one
 * pricing a LIST as a GET.
 */
class CostMeterTest {

    private static final long MIB = 1024L * 1024L;
    private static final long TIB = 1024L * 1024L * 1024L * 1024L;

    /** Research 02 §1, AWS us-east-1 S3 Standard: $0.005 and $0.0004 per 1,000. */
    private static final CostTable AWS = CostTable.awsS3Standard();

    private static CostMeter meter(StoreCounts counts) {
        return new CostMeter(counts, AWS);
    }

    private static StoreCounts counts(long puts, long gets, long lists, long stats, long deletes) {
        return new StoreCounts(puts, gets, lists, stats, deletes);
    }

    // --- requests per MiB ------------------------------------------------

    /**
     * Research 02 §5, Scenario A's write path: 12 data PUT/s plus 12 commit
     * PUT/s against 100 MiB/s of ingest. The prose says "0.24 requests per MiB
     * ingested"; the arithmetic says 24/100 exactly.
     */
    @Test
    void scenarioAIsPointTwoFour() {
        CostMeter m = meter(counts(24, 0, 0, 0, 0));
        assertThat(m.requestsPerMiBWritten(100 * MIB).orElseThrow())
                .isCloseTo(0.24d, within(1e-9d));
    }

    /**
     * Research 02 §5, Scenario A's recommended read path: one whole-object GET
     * per node, 12 objects/s × 9 nodes = 108 GET/s against 100 MiB/s.
     */
    @Test
    void scenarioAReadPathIsOnePointZeroEight() {
        CostMeter m = meter(counts(0, 108, 0, 0, 0));
        assertThat(m.requestsPerMiBRead(100 * MIB).orElseThrow())
                .isCloseTo(1.08d, within(1e-9d));
    }

    /**
     * ⚠️ A MEBIBYTE, NOT A MEGABYTE. One request against 10^6 bytes is
     * 1.048576 requests per MiB, not 1.0 — a meter dividing by 10^6 reports
     * every ratio 4.9% low, which is the difference between 0.2288 and 0.24
     * against a budget of 0.30.
     */
    @Test
    void perMiBDividesByMebibytesNotMegabytes() {
        assertThat(meter(counts(1, 0, 0, 0, 0)).requestsPerMiBWritten(1_000_000L).orElseThrow())
                .isCloseTo(1.048576d, within(1e-9d));
        assertThat(meter(counts(1, 0, 0, 0, 0)).requestsPerMiBWritten(MIB).orElseThrow())
                .isCloseTo(1.0d, within(1e-9d));
    }

    /**
     * NFR-1 is a WRITE budget, and a LIST is a discovery request whose own
     * bound is criterion 3's enumeration. Counting it here would let a
     * recovery LIST consume the write budget, and a DELETE — free, and issued
     * by retention — would do the same.
     */
    @Test
    void writeRequestsArePutsAloneNotListsOrDeletes() {
        assertThat(meter(counts(3, 0, 7, 0, 11)).writeRequests()).isEqualTo(3);
        assertThat(meter(counts(3, 0, 7, 0, 11)).requestsPerMiBWritten(MIB).orElseThrow())
                .isCloseTo(3.0d, within(1e-9d));
    }

    /**
     * A HEAD is a read: it is billed at the GET rate and it is issued to learn
     * something about an object, so leaving it out of the read ratio
     * under-reports a reader that probes before it fetches.
     */
    @Test
    void readRequestsAreGetsAndStats() {
        assertThat(meter(counts(0, 5, 9, 2, 0)).readRequests()).isEqualTo(7);
        assertThat(meter(counts(0, 5, 9, 2, 0)).requestsPerMiBRead(MIB).orElseThrow())
                .isCloseTo(7.0d, within(1e-9d));
    }

    // --- the division nobody wants to think about ------------------------

    /**
     * ⚠️ NO BYTES WRITTEN IS NOT ZERO REQUESTS PER MiB, AND IT IS NOT NaN.
     * A silent 0.0 reports an idle pod as the cheapest build there is and
     * would pass NFR-1's {@code < 0.30} while measuring nothing; a NaN passes
     * no comparison at all and prints as a word in a cost report. The reading
     * is absent, and the caller has to say so.
     */
    @Test
    void perMiBIsAbsentRatherThanZeroOrNaNWhenNoBytesMoved() {
        CostMeter m = meter(counts(24, 108, 0, 0, 0));
        assertThat(m.requestsPerMiBWritten(0L)).isEmpty();
        assertThat(m.requestsPerMiBRead(0L)).isEmpty();
        assertThat(m.estimatedUsdPerTibIngested(0L)).isEmpty();
    }

    /** Negative bytes are a caller's bug, not a reading with a sign. */
    @Test
    void negativeBytesAreRefused() {
        CostMeter m = meter(counts(1, 1, 1, 1, 1));
        assertThatThrownBy(() -> m.requestsPerMiBWritten(-1L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> m.requestsPerMiBRead(-1L))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> m.estimatedUsdPerTibIngested(-1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // --- the idle rate ---------------------------------------------------

    /**
     * NFR-2 and cost rule R3: the most valuable assertion in the project is
     * that an idle pod issues nothing. The window is a PARAMETER — the meter
     * reads no clock (non-negotiable 7), so a caller cannot get a rate by
     * accident and every rate names the window it was taken over.
     */
    @Test
    void idleRequestRateIsEveryVerbOverTheWindowGiven() {
        CostMeter m = meter(counts(10, 10, 5, 4, 1));
        assertThat(m.idleRequestsPerSecond(Duration.ofSeconds(60)))
                .isCloseTo(0.5d, within(1e-9d));
        assertThat(m.idleRequestsPerSecond(Duration.ofSeconds(30)))
                .isCloseTo(1.0d, within(1e-9d));
    }

    /** The reading NFR-2 demands: nothing issued over five idle minutes. */
    @Test
    void anIdlePodRatesZero() {
        CostMeter m = meter(counts(0, 0, 0, 0, 0));
        assertThat(m.idleRequestsPerSecond(Duration.ofMinutes(5))).isZero();
    }

    /**
     * A sub-second window is a real measurement and must not truncate to
     * seconds: 3 requests over 250 ms is 12/s.
     */
    @Test
    void idleRequestRateUsesSubSecondWindows() {
        assertThat(meter(counts(3, 0, 0, 0, 0)).idleRequestsPerSecond(Duration.ofMillis(250)))
                .isCloseTo(12.0d, within(1e-9d));
    }

    /**
     * ⚠️ A ZERO WINDOW IS THE CALLER'S BUG, NOT AN ABSENT READING. Unlike the
     * byte totals — which a legitimately idle pod really does report as zero —
     * the window is chosen by whoever takes the reading, so an empty answer
     * here would hide a harness that forgot to record its own duration.
     */
    @Test
    void aZeroOrNegativeWindowIsRefused() {
        CostMeter m = meter(counts(1, 0, 0, 0, 0));
        assertThatThrownBy(() -> m.idleRequestsPerSecond(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> m.idleRequestsPerSecond(Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // --- dollars ---------------------------------------------------------

    /**
     * ⚠️ requests × pricePerRequest, EXACTLY. One TiB ingested at the 8 MiB
     * segment size of Scenario A is 2^40/2^23 = 131,072 PUTs, and a PUT is
     * $0.005/1,000 = $5 × 10^-6. 131,072 × 5e-6 = $0.65536 — a literal,
     * written here rather than recomputed from the meter's own formula.
     */
    @Test
    void usdPerTibIsRequestsTimesPricePerRequest() {
        CostMeter m = meter(counts(131_072, 0, 0, 0, 0));
        assertThat(m.estimatedUsdPerTibIngested(TIB).orElseThrow())
                .isCloseTo(0.65536d, within(1e-9d));
        assertThat(m.estimatedUsd()).isCloseTo(0.65536d, within(1e-9d));
    }

    /**
     * $/TiB is a RATE, so halving the bytes those same requests carried
     * doubles it. A meter reporting the run's total dollars under a per-TiB
     * name would report both runs as costing the same.
     */
    @Test
    void usdPerTibScalesWithTheBytesTheRequestsCarried() {
        CostMeter m = meter(counts(131_072, 0, 0, 0, 0));
        assertThat(m.estimatedUsdPerTibIngested(TIB / 2).orElseThrow())
                .isCloseTo(1.31072d, within(1e-9d));
    }

    /** A GET is $0.0004/1,000: 131,072 × 4e-7 = $0.0524288. */
    @Test
    void getsArePricedAtTheGetRate() {
        assertThat(meter(counts(0, 131_072, 0, 0, 0)).estimatedUsd())
                .isCloseTo(0.0524288d, within(1e-9d));
    }

    /**
     * ⚠️ A LIST COSTS WHAT A PUT COSTS — 12.5 GETs (research 02 §1) — and the
     * table prices it separately so a provider where it does not is still
     * described. A meter pricing LIST as a GET reports the runaway cost rule
     * 15 exists to catch at 8% of its real price.
     */
    @Test
    void listIsPricedAsAListNotAsAGet() {
        assertThat(meter(counts(0, 0, 131_072, 0, 0)).estimatedUsd())
                .isCloseTo(0.65536d, within(1e-9d));
    }

    /**
     * ⚠️ THREE DISTINCT PRICES, BECAUSE EVERY OTHER TABLE HERE IS DEGENERATE.
     * {@link CostTable#awsS3Standard()} prices a PUT and a LIST alike (5,000
     * each) and {@link CostTable#free()} prices everything at zero, so under
     * either of them a meter that multiplied puts by the LIST price and lists
     * by the PUT price would bill exactly the right dollars — measured, and it
     * left the whole suite green. This case separates the components that
     * {@code CostTable}'s own javadoc calls the most consequential ratio in the
     * cost model.
     *
     * <p>At $0.007/1,000 PUTs, $0.0004/1,000 GETs and $0.003/1,000 LISTs:
     * 1,000 × 7,000 + 50 × 400 + 100 × 3,000 = 7,320,000 micro-dollars per
     * 1,000 requests = 7,320 micro-dollars = $0.00732. Swapping the PUT and
     * LIST components gives $0.00372.
     */
    @Test
    void putAndListArePricedByTheirOwnComponents() {
        CostMeter m = new CostMeter(counts(1_000, 50, 100, 0, 0),
                new CostTable(7_000L, 400L, 3_000L));
        assertThat(m.estimatedUsd()).isCloseTo(0.00732d, within(1e-9d));
    }

    /** A HEAD is billed at the GET rate, so a stat costs what a get costs. */
    @Test
    void statIsPricedAsAGet() {
        assertThat(meter(counts(0, 0, 0, 131_072, 0)).estimatedUsd())
                .isCloseTo(0.0524288d, within(1e-9d));
    }

    /**
     * DELETE is free on S3 (research 02 §1) and the table carries no price for
     * it, so deletes are COUNTED — the retention sweep's rate matters — and
     * billed at nothing.
     */
    @Test
    void deletesAreCountedButNotPriced() {
        assertThat(meter(counts(0, 0, 0, 0, 1_000_000)).estimatedUsd()).isZero();
        assertThat(meter(counts(0, 0, 0, 0, 1_000_000)).idleRequestsPerSecond(Duration.ofSeconds(1)))
                .isCloseTo(1_000_000d, within(1e-9d));
    }

    /**
     * ⚠️ THE PRICE TABLE IS INJECTED, NEVER AWS BY CONSTRUCTION. The local-FS
     * and in-memory backends report {@link CostTable#free()} and a modelled
     * bill of zero is the right answer for them, not a degenerate one.
     */
    @Test
    void aFreeBackendBillsNothingWhateverItIssued() {
        CostMeter free = new CostMeter(counts(131_072, 131_072, 131_072, 131_072, 131_072),
                CostTable.free());
        assertThat(free.estimatedUsd()).isZero();
        assertThat(free.estimatedUsdPerTibIngested(TIB).orElseThrow()).isZero();
        assertThat(free.requestsPerMiBWritten(TIB).orElseThrow())
                .isCloseTo(0.125d, within(1e-9d));
    }

    /**
     * The whole write path of Scenario A, read as a bill: 24 PUT/s for a month
     * is 24 × 2,592,000 = 62,208,000 PUTs at $5 × 10^-6 = $311.04, which is the
     * $312 the prose rounds to.
     */
    @Test
    void scenarioAMonthlyWritePathIsThreeHundredAndEleven() {
        assertThat(meter(counts(62_208_000L, 0, 0, 0, 0)).estimatedUsd())
                .isCloseTo(311.04d, within(1e-9d));
    }

    // --- construction ----------------------------------------------------

    @Test
    void aMeterNeedsBothCountsAndPrices() {
        assertThatThrownBy(() -> new CostMeter(null, AWS))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new CostMeter(counts(0, 0, 0, 0, 0), null))
                .isInstanceOf(NullPointerException.class);
    }
}
