// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * A consumer declares its zone on the poll, or declares nothing (M9.2, NFR-5).
 *
 * <p>⚠️ **THE DIFFERENCE BETWEEN "NOTHING" AND AN EMPTY VALUE IS THE WHOLE
 * CASE.** {@code az=} is a claim: the ingester reads a zone whose name is the
 * empty string, compares it with its own, finds a mismatch, and counts the
 * bytes as cross-AZ ATTRIBUTED. Sending nothing is counted cross-AZ too --
 * the safe side -- but reported as UNATTRIBUTED, which is what tells a reader
 * of the cost report that the number is an upper bound rather than a
 * measurement.
 */
class ConsumerAzParamTest {

    @Test
    void aZoneThisConsumerKnowsIsSENT() {
        assertThat(HttpSubscriptionTransport.azParam("az-b")).containsExactly("az-b");
    }

    @Test
    void aZoneItWasNotGivenIsNOTSENTAtAll() {
        assertThat(HttpSubscriptionTransport.azParam("")).isEmpty();
        assertThat(HttpSubscriptionTransport.azParam("   ")).isEmpty();
        assertThat(HttpSubscriptionTransport.azParam(null)).isEmpty();
    }

    @Test
    void aZoneIsTRIMMEDBeforeItIsCLAIMED() {
        assertThat(HttpSubscriptionTransport.azParam(" az-c ")).containsExactly("az-c");
    }
}
