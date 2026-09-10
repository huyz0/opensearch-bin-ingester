// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.Capabilities;
import binjava.binstore.CostTable;
import binjava.format.FetchMode;
import org.junit.jupiter.api.Test;

/**
 * Who chooses the fetch mode, and on what (M5.11, FR-6).
 *
 * <p>⚠️ NO FIXTURE, NO STORE, NO CLOCK -- and that is an assertion about the
 * design rather than a convenience. Non-negotiable 7 says business logic
 * touches no socket, clock or object store; a policy needing any of them to be
 * tested would be in the wrong layer. Every case here is a record in and an
 * enum out.
 */
class FetchPolicyTest {

    /** S3's real prices: $0.0004 per 1,000 GETs, in micro-dollars. */
    private static final CostTable S3 = new CostTable(5_000L, 400L, 5_000L);

    private static Capabilities caps(boolean presign) {
        return new Capabilities(true, true, presign, 1024L, 5L * 1024 * 1024, S3);
    }

    private static FetchPolicy policy() {
        return new FetchPolicy(FetchPolicyConfig.defaultsFor(S3));
    }

    private static SegmentDelivery sameAz(long bytes, int fanOut) {
        return new SegmentDelivery(bytes, true, fanOut, false);
    }

    private static SegmentDelivery otherAz(long bytes, int fanOut) {
        return new SegmentDelivery(bytes, false, fanOut, false);
    }

    // ---- criterion 5: the crossover is DERIVED ----------------------------

    /**
     * The cross-AZ crossover is cost model R12's arithmetic, not a literal.
     *
     * <p>⚠️ THE EXACT NUMBER MATTERS because every document in this repository
     * quotes it. R12 is (one GET) / ($ per GB): $0.0004/1,000 GETs over
     * $0.02/GB is 20,000 bytes, which is the 19.53 KiB the corpus calls
     * "≈19.5 KiB". A crossover that came out at 21.0 KiB would mean someone
     * divided by 2^30 instead of 10^9 -- the bill is in decimal GB.
     */
    @Test
    void theCrossAZCrossoverIsTHEARITHMETICOfCostModelR12() {
        assertThat(FetchPolicyConfig.defaultsFor(S3).crossAzCrossoverBytes())
                .as("(one GET) / ($ per GB) at S3's prices is exactly 20,000 bytes")
                .isEqualTo(20_000L);
    }

    /**
     * A backend with a different GET price has a different crossover.
     *
     * <p>⚠️ THIS IS THE ONE THAT MAKES "DERIVED" MEAN ANYTHING. A policy that
     * hard-coded 20,000 passes the case above and fails this one: at ten times
     * the GET price it costs ten times as much to make a consumer fetch, so
     * ten times as many bytes are worth shipping to avoid it.
     */
    @Test
    void aDEARERGetMovesTheCrossoverUP() {
        CostTable dear = new CostTable(5_000L, 4_000L, 5_000L);
        assertThat(FetchPolicyConfig.defaultsFor(dear).crossAzCrossoverBytes())
                .as("ten times the GET price, ten times the bytes worth shipping to avoid one")
                .isEqualTo(200_000L);
    }

    /**
     * The $/GB half of the derivation moves the crossover too.
     *
     * <p>⚠️ ROUND-1 REVIEW MEASURED THIS HALF UNPINNED: every other case here
     * passes the DEFAULT transfer price or zero, so
     * {@code costs.getPerThousand() * 50L} -- the divisor deleted outright --
     * passed all eighteen tests. Criterion 5 amended M5's SPEC specifically to
     * say this half is configuration rather than {@code CostTable}, which made
     * it the one input the argument turned on and the one nothing constrained.
     *
     * <p>⚠️ HALVING THE TRANSFER PRICE DOUBLES THE CROSSOVER, because it is a
     * division: cheaper bytes mean more of them are worth shipping to avoid
     * one GET.
     */
    @Test
    void aCHEAPERCrossAZTransferMovesTheCrossoverUPToo() {
        assertThat(FetchPolicyConfig.derivedFrom(S3, 10_000L,
                        FetchPolicyConfig.DEFAULT_INLINE_CAP_BYTES, 1).crossAzCrossoverBytes())
                .as("half the price per byte, twice the bytes worth shipping")
                .isEqualTo(40_000L);
        assertThat(FetchPolicyConfig.derivedFrom(S3, 80_000L,
                        FetchPolicyConfig.DEFAULT_INLINE_CAP_BYTES, 1).crossAzCrossoverBytes())
                .as("and four times the price, a quarter of the bytes")
                .isEqualTo(5_000L);
    }

    /**
     * The same-AZ inline cap is a dial, not a literal.
     *
     * <p>⚠️ Review MEASURED that hard-coding {@code 256 * 1024} in place of
     * {@code config.inlineCapBytes()} survived: the cap was asserted at its
     * default and never anywhere else, so the field was decorative.
     */
    @Test
    void theSameAZInlineCapIsCONFIGURATION() {
        FetchPolicyConfig tight = FetchPolicyConfig.derivedFrom(
                S3, FetchPolicyConfig.DEFAULT_CROSS_AZ_MICRO_DOLLARS_PER_GB, 32L * 1024, 1);
        FetchPolicy p = new FetchPolicy(tight);
        assertThat(p.modeFor(sameAz(32L * 1024, 64), caps(true))).isEqualTo(FetchMode.INLINE);
        assertThat(p.modeFor(sameAz(32L * 1024 + 1, 64), caps(true)))
                .as("past the CONFIGURED cap, not past the default one")
                .isEqualTo(FetchMode.PROXY);
    }

    /**
     * Fan-out 0 is below a threshold of 1, and that is deliberate.
     *
     * <p>⚠️ Review MEASURED that adding {@code delivery.segmentFanOut() > 0} to the
     * guard -- a real behaviour change at the DEFAULT threshold -- survived
     * the suite, because fan-out 0 was only ever asserted at a threshold of 0
     * where the {@code > 0} guard already answers. Pinned here so the boundary
     * has one meaning: a delivery with no subscriber is one nobody is served,
     * so the mode is moot, and 0 is simply below 1.
     */
    @Test
    void fanOutZEROIsBelowTheDefaultThresholdOfONE() {
        assertThat(policy().modeFor(otherAz(4L * 1024 * 1024, 0), caps(true)))
                .isEqualTo(FetchMode.DIRECT);
    }

    // ---- what the constructors refuse (testing.md rule 14) ----------------

    /**
     * Every constructor guard in the change refuses what it says it refuses.
     *
     * <p>⚠️ ROUND-1 REVIEW DELETED ALL OF THEM AT ONCE -- both records' checks,
     * all three {@code requireNonNull} calls and {@code multiplyExact} -- and
     * the suite stayed green. Validation nothing exercises is a comment that
     * throws.
     */
    @Test
    void theConstructorsREFUSEWhatTheirMessagesSayTheyRefuse() {
        assertThatThrownBy(() -> new SegmentDelivery(-1L, true, 1, false))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not a batch");
        assertThatThrownBy(() -> new SegmentDelivery(1L, true, -1, false))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not a fan-out");
        assertThatThrownBy(() -> new FetchPolicyConfig(0L, 1L, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inlines nothing ever");
        assertThatThrownBy(() -> new FetchPolicyConfig(1L, -1L, 1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not a size");
        assertThatThrownBy(() -> new FetchPolicyConfig(1L, 1L, -1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("names no fan-out");
        assertThatThrownBy(() -> new FetchPolicy(null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("config");
        // ⚠️ hasMessage, NOT hasMessageContaining, and only here. Round-3
        // review MEASURED that deleting `requireNonNull(delivery, "delivery")`
        // survived the containment form: `delivery.bytesInServingAz()` then
        // throws an implicit NPE carrying the JDK's helpful message, `Cannot
        // invoke "...SegmentDelivery.bytesInServingAz()" because "delivery" is
        // null`, which contains the word. The assertion was satisfied by the
        // JVM rather than by the production line it names. `requireNonNull`'s
        // message is EXACTLY the label, so the exact form tells them apart.
        assertThatThrownBy(() -> policy().modeFor(null, caps(true)))
                .isInstanceOf(NullPointerException.class).hasMessage("delivery");
        assertThatThrownBy(() -> policy().modeFor(sameAz(1L, 1), null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("capabilities");
    }

    /**
     * A GET price large enough to overflow the derivation is refused, not
     * wrapped.
     *
     * <p>⚠️ WRAPPING PRODUCES A NEGATIVE CROSSOVER, which the canonical
     * constructor does refuse -- but blaming the crossover rather than the
     * price that produced it, which is the wrong end of the stack to debug.
     */
    @Test
    void aGetPriceThatOVERFLOWSTheDerivationIsREFUSED() {
        CostTable absurd = new CostTable(0L, Long.MAX_VALUE / 2, 0L);
        assertThatThrownBy(() -> FetchPolicyConfig.defaultsFor(absurd))
                .isInstanceOf(ArithmeticException.class);
    }

    /**
     * A free backend crosses over at zero, and inlines nothing cross-AZ.
     *
     * <p>⚠️ THE RIGHT ANSWER, NOT A DEGENERATE ONE. {@code CostTable.free()}
     * is what the local-FS and in-memory backends report: a GET costs nothing,
     * so there is never a reason to spend cross-AZ bytes to avoid one.
     */
    @Test
    void aFREEBackendInlinesNOTHINGCrossAZ() {
        FetchPolicy free = new FetchPolicy(FetchPolicyConfig.defaultsFor(CostTable.free()));
        assertThat(free.config().crossAzCrossoverBytes()).isZero();
        assertThat(free.modeFor(otherAz(1L, 64), caps(false)))
                .as("a GET that costs nothing is never worth avoiding")
                .isEqualTo(FetchMode.PROXY);
    }

    /**
     * A transfer price of zero is refused rather than dividing by it.
     *
     * <p>⚠️ IT IS THE DIVISOR. Zero would mean bytes move between AZs for
     * free, which is the one thing ADR-0012 measured false at 419x.
     */
    @Test
    void aFREECrossAZTransferIsREFUSED() {
        assertThatThrownBy(() -> FetchPolicyConfig.derivedFrom(S3, 0L, 1024L, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("419x");
    }

    // ---- criterion 4: inline is the default -------------------------------

    /**
     * A small same-AZ segment is {@code INLINE} at EVERY fan-out.
     *
     * <p>⚠️ FAN-OUT 1 IS THE CASE THAT MATTERS, because it is also the
     * condition for {@code DIRECT}. Inline still wins: it costs the consumer
     * no request at all, where `direct` costs one. A policy that checked its
     * `direct` conditions first would answer DIRECT here and make the escape
     * hatch the default for every catch-up replay -- ADR-0004's rejected
     * design, arrived at by ordering rather than by intent.
     */
    @Test
    void aSmallSameAZSegmentIsINLINEAtEVERYFanOut() {
        FetchPolicy p = policy();
        for (int fanOut : new int[] {0, 1, 2, 64, 300}) {
            assertThat(p.modeFor(sameAz(8L * 1024, fanOut), caps(true)))
                    .as("fan-out %d", fanOut)
                    .isEqualTo(FetchMode.INLINE);
        }
    }

    /**
     * A SMALL batch is {@code INLINE} even where every {@code DIRECT}
     * condition holds -- the ordering claim, pinned.
     *
     * <p>⚠️ {@code FetchPolicy}'s javadoc calls "INLINE comes first"
     * load-bearing, and round-2 review MEASURED that claim UNPINNED: hoisting
     * the {@code DIRECT} block above the inline bound left all 24 tests green.
     * Nothing reached the cell where both arms answer, so the order could not
     * matter to any assertion.
     *
     * <p>⚠️ ROUND ONE'S OWN FIXES OPENED IT, which is the part worth
     * remembering. Adding the cache-residency conjunct made every same-AZ
     * inline case immune to a reorder, and re-pointing every DIRECT-vs-PROXY
     * case at cold bytes emptied the cold/small/low-fan-out cell -- the two
     * cross-AZ inline tests both use fan-out 64. A fix closed one hole and
     * opened another in the same file.
     *
     * <p>⚠️ BOTH ARMS, because they are reached differently: the fan-out arm
     * needs cold bytes, the shedding arm does not. Under the reorder the first
     * case answers {@code DIRECT} for a batch doc 04 prices at 0.4x a GET and
     * says to inline -- "`direct` becomes the default by accident", the SPEC's
     * named failure mode for this row, arrived at exactly as the javadoc
     * predicts.
     */
    @Test
    void aSmallBatchIsINLINEEvenWhereEveryDIRECTConditionHolds() {
        FetchPolicy p = policy();
        assertThat(p.modeFor(otherAz(8L * 1024, 1), caps(true)))
                .as("cold, fan-out 1, presign-capable -- and still inline, because 8 KiB across "
                        + "an AZ costs 0.4x the GET it saves")
                .isEqualTo(FetchMode.INLINE);
        assertThat(p.modeFor(new SegmentDelivery(8L * 1024, true, 300, true), caps(true)))
                .as("and a shedding pod does not shed what it can simply send")
                .isEqualTo(FetchMode.INLINE);
    }

    /**
     * A trickle index's batch is inlined even CROSS-AZ.
     *
     * <p>⚠️ RESEARCH DOC 04 CALLS THIS OUT as a refinement over a blanket ban:
     * "a trickle index's 8 KiB batch is 0.4x a GET and is worth inlining even
     * cross-AZ, while anything past ~20 KiB is not." A policy that refused to
     * inline anything cross-AZ passes every same-AZ case and loses this.
     */
    @Test
    void aTrickleBatchIsInlinedEVENCrossAZ() {
        assertThat(policy().modeFor(otherAz(8L * 1024, 64), caps(true)))
                .as("8 KiB is 0.4x a GET: shipping it costs less than the GET it saves")
                .isEqualTo(FetchMode.INLINE);
    }

    /**
     * Past the crossover, cross-AZ bytes are NOT inlined.
     *
     * <p>⚠️ THE BOUND IS EXACT AND BOTH SIDES ARE ASSERTED. At 20,000 bytes
     * inlining still breaks even; at one byte more it does not. Inlining
     * 100 MiB/s cross-AZ is ~$340/day, which is what the wrong side of this
     * comparison buys.
     */
    @Test
    void pastTheCrossoverCrossAZBytesAreNOTInlined() {
        FetchPolicy p = policy();
        assertThat(p.modeFor(otherAz(20_000L, 64), caps(true)))
                .as("at the crossover, inlining still breaks even")
                .isEqualTo(FetchMode.INLINE);
        assertThat(p.modeFor(otherAz(20_001L, 64), caps(true)))
                .as("one byte past it, it does not")
                .isEqualTo(FetchMode.PROXY);
    }

    /**
     * The same-AZ cap is a SEPARATE, larger bound than the cross-AZ crossover.
     *
     * <p>⚠️ TWO THRESHOLDS, NOT ONE, and collapsing them is the mutation this
     * catches. Intra-AZ transfer is FREE, so the cap there protects the push
     * channel and the consumer's heap rather than a bill -- research doc 04
     * defaults it to 256 KiB, twelve times the cross-AZ crossover. A policy
     * using the crossover for both would send a 64 KiB same-AZ segment by
     * PROXY and pay a fetch to avoid free bytes.
     */
    @Test
    void theSameAZCapIsSEPARATEAndLarger() {
        FetchPolicy p = policy();
        assertThat(p.modeFor(sameAz(64L * 1024, 64), caps(true)))
                .as("64 KiB is past the cross-AZ crossover but well inside the free same-AZ cap")
                .isEqualTo(FetchMode.INLINE);
        assertThat(p.modeFor(sameAz(256L * 1024, 64), caps(true)))
                .as("at the cap")
                .isEqualTo(FetchMode.INLINE);
        assertThat(p.modeFor(sameAz(256L * 1024 + 1, 64), caps(true)))
                .as("one byte past it")
                .isEqualTo(FetchMode.PROXY);
    }

    // ---- criteria 6 and 7: direct is the escape hatch ---------------------

    /**
     * A large segment at fan-out 1 is {@code DIRECT} -- catch-up replay.
     *
     * <p>⚠️ THIS IS THE ONLY MODE THAT ADDS A CONSUMER-SIDE GET, and fan-out 1
     * is what makes it cheap: one GET serves one consumer. At fan-out 64 the
     * same choice would be 64 GETs where PROXY is none.
     */
    @Test
    void aLargeCOLDBatchAtFanOutONEIsDIRECT() {
        assertThat(policy().modeFor(otherAz(4L * 1024 * 1024, 1), caps(true)))
                .as("catch-up replay reads bytes no pod in this AZ holds")
                .isEqualTo(FetchMode.DIRECT);
    }

    /**
     * The SAME delivery, but the bytes are already here: {@code PROXY}.
     *
     * <p>⚠️ THE CONJUNCT ROUND-1 REVIEW FOUND MISSING, and it blocked the
     * commit. Research doc 10 §4 writes the rule as an iff -- "redirect iff
     * N == 1 AND THE SEGMENT IS NOT ALREADY CACHED" -- and the first
     * implementation tested only the fan-out half. Its own test pinned the
     * WRONG answer with a same-AZ delivery, which is the cached case doc 10
     * excludes.
     *
     * <p>⚠️ WHAT IT COSTS IS THE POINT. The pod that wrote an 8 MiB segment
     * still holds it, and a stream whose shard lives on one node in that AZ
     * has fan-out 1 -- so the defect issues a signed URL for bytes the pod
     * would have streamed from RAM for zero requests. ONE GET PER STREAM PER
     * FLUSH: a rate scaling with streams, which non-negotiable 6 forbids by
     * name, against a budget of "2 GETs/segment at 3 AZs, flat in node count".
     */
    @Test
    void theSameBatchIsPROXYWhenTheBytesAreALREADYHERE() {
        assertThat(policy().modeFor(sameAz(4L * 1024 * 1024, 1), caps(true)))
                .as("a signed URL for bytes in this pod's own RAM is a GET bought for nothing")
                .isEqualTo(FetchMode.PROXY);
    }

    /**
     * The same segment at fan-out above the threshold is {@code PROXY}.
     *
     * <p>⚠️ THE PAIR IS THE TEST. Either case alone passes under a policy that
     * ignores fan-out entirely and answers one mode for every large segment.
     */
    @Test
    void theSameBatchAtHIGHFanOutIsPROXY() {
        assertThat(policy().modeFor(otherAz(4L * 1024 * 1024, 2), caps(true)))
                .as("two consumers is two GETs where proxy is none")
                .isEqualTo(FetchMode.PROXY);
    }

    /**
     * A pod shedding load answers {@code DIRECT} at ANY fan-out.
     *
     * <p>⚠️ THE SECOND CONDITION RESEARCH DOC 04 GIVES, and it is not the
     * fan-out one: "catch-up with fan-out 1, OR the pod shedding load". A
     * policy implementing only the fan-out half passes every other case here.
     */
    @Test
    void aPodUNDERPRESSUREAnswersDIRECTAtAnyFanOut() {
        SegmentDelivery shedding = new SegmentDelivery(4L * 1024 * 1024, true, 300, true);
        assertThat(policy().modeFor(shedding, caps(true)))
                .as("the load-shedding row of doc 10 §4 reads fan-out ANY, and the bytes being "
                        + "cached does not change it -- a pod that is queueing would rather pay "
                        + "a GET than keep the queue")
                .isEqualTo(FetchMode.DIRECT);
    }

    /**
     * {@code DIRECT} is never chosen against a backend that cannot presign.
     *
     * <p>⚠️ BOTH SHIPPING BACKENDS ARE THIS BACKEND (ADR-0041), so without
     * this the policy's answer for every catch-up replay in the tree today is
     * a mode nothing can serve. M5.13's startup refusal would be the only
     * guard, and it fires at startup for a deployment that ENABLED `direct` --
     * not for a policy that chose it on its own.
     */
    @Test
    void DIRECTIsNeverChosenWhenTheBackendCannotPRESIGN() {
        FetchPolicy p = policy();
        assertThat(p.modeFor(otherAz(4L * 1024 * 1024, 1), caps(false)))
                .as("catch-up replay falls back to proxy rather than naming an unservable mode")
                .isEqualTo(FetchMode.PROXY);
        SegmentDelivery shedding = new SegmentDelivery(4L * 1024 * 1024, true, 300, true);
        assertThat(p.modeFor(shedding, caps(false)))
                .as("and so does a shedding pod")
                .isEqualTo(FetchMode.PROXY);
    }

    /**
     * The `direct` fan-out threshold is CONFIGURATION, not a constant.
     *
     * <p>⚠️ MEASUREMENT M3 IS DEFERRED TO M9, so the fan-out at which `direct`
     * wins is a dial here -- the same discipline M4 applied to the lease TTL.
     * A policy comparing against a literal 1 passes every other case in this
     * file.
     */
    @Test
    void theDIRECTFanOutThresholdIsCONFIGURATION() {
        FetchPolicyConfig wide = FetchPolicyConfig.derivedFrom(
                S3, FetchPolicyConfig.DEFAULT_CROSS_AZ_MICRO_DOLLARS_PER_GB,
                FetchPolicyConfig.DEFAULT_INLINE_CAP_BYTES, 8);
        assertThat(new FetchPolicy(wide).modeFor(otherAz(4L * 1024 * 1024, 8), caps(true)))
                .as("fan-out 8 is at the configured threshold, so direct still wins")
                .isEqualTo(FetchMode.DIRECT);
        assertThat(policy().modeFor(otherAz(4L * 1024 * 1024, 8), caps(true)))
                .as("and the same delivery under the default threshold of 1 is proxy")
                .isEqualTo(FetchMode.PROXY);
    }

    /**
     * A threshold of zero turns `direct` off entirely.
     *
     * <p>⚠️ THE OPERATOR'S OFF SWITCH, and it needs an assertion or the dial
     * has an untested end. Fan-out 0 -- nobody subscribed -- must not sneak
     * past a {@code <=} comparison into a mode nobody asked for.
     */
    @Test
    void aThresholdOfZERODisablesDIRECT() {
        FetchPolicyConfig off = FetchPolicyConfig.derivedFrom(
                S3, FetchPolicyConfig.DEFAULT_CROSS_AZ_MICRO_DOLLARS_PER_GB,
                FetchPolicyConfig.DEFAULT_INLINE_CAP_BYTES, 0);
        FetchPolicy p = new FetchPolicy(off);
        assertThat(p.modeFor(otherAz(4L * 1024 * 1024, 0), caps(true))).isEqualTo(FetchMode.PROXY);
        assertThat(p.modeFor(otherAz(4L * 1024 * 1024, 1), caps(true))).isEqualTo(FetchMode.PROXY);
    }
}
