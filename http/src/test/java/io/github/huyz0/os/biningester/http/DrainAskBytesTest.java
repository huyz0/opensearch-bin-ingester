// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * What the inbox drain's cross-AZ figure actually counts (M9.2, NFR-5,
 * ADR-0058).
 *
 * <p>⚠️ **THE NUMBER IS A FLOOR AND THESE CASES SAY SO IN ARITHMETIC.** It is
 * the request target's characters — the path, {@code ?pod=} and the requester's
 * id — and nothing else: no method, no version, no header, no TLS or TCP
 * framing, no percent-encoding. A reader who takes it for the wire cost is out
 * by an order of magnitude, so the cases below state the exact value rather
 * than "greater than zero", which is what let the arithmetic be mutated three
 * ways and stay green.
 */
class DrainAskBytesTest {

    private static final long TARGET = HttpSequencerTransport.DRAIN_PATH.length()
            + HttpSequencerTransport.POD_PARAM.length();

    @Test
    void theAskIsThePathTheParameterAndTheRequestersId() {
        assertThat(HttpSequencerTransport.drainAskBytes("pod9")).isEqualTo(TARGET + 4);
    }

    @Test
    void aLONGERRequesterIdCostsPROPORTIONALLYMore() {
        // ⚠️ RED IF THE ADDITION BECOMES A SUBTRACTION, which "greater than
        // zero" cannot see: the path alone is 10 characters, so a subtraction
        // still answers a plausible positive number.
        assertThat(HttpSequencerTransport.drainAskBytes("ingester000000000000"))
                .isEqualTo(TARGET + 20)
                .isGreaterThan(HttpSequencerTransport.drainAskBytes("pod9"));
    }

    @Test
    void aRequesterWithNoNameCostsTheTargetAlone() {
        assertThat(HttpSequencerTransport.drainAskBytes("")).isEqualTo(TARGET);
        assertThat(HttpSequencerTransport.drainAskBytes(null))
                .as("⚠️ NULL IS THE SAME AS EMPTY HERE and must not throw: the counter "
                        + "runs before the request, and a cross-AZ figure is not a place "
                        + "to raise a new failure on the commit path")
                .isEqualTo(TARGET);
    }

    @Test
    void theFigureIsSMALLAndIsNotTheWireCost() {
        // ⚠️ PINNED BECAUSE THE JAVADOC CLAIMS IT: "tens, not hundreds". A
        // reader deciding whether this term matters to NFR-5 is deciding from
        // this magnitude.
        assertThat(HttpSequencerTransport.drainAskBytes("pod9")).isLessThan(100);
    }
}
