// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.binstore;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * ⚠️ M1.3's cost meter is the next reader of these numbers, and a wrong
 * {@code free()} would make the local-FS and in-memory backends report a bill.
 */
class CostTableTest {

    @Test
    void freeIsActuallyFree() {
        CostTable free = CostTable.free();
        assertThat(free.putPerThousand()).isZero();
        assertThat(free.getPerThousand()).isZero();
        assertThat(free.listPerThousand()).isZero();
    }

    /**
     * ⚠️ THE NUMBERS ARE RESEARCH 02 §1's, IN MICRO-DOLLARS PER 1,000: a
     * PUT/COPY/POST or a LIST at $0.005 per 1,000 is 5,000, and a GET at
     * $0.0004 per 1,000 is 400. It is a DEFAULT the caller may replace, not a
     * constant the code is built on — every other provider has the same shape
     * with different constants.
     */
    @Test
    void awsS3StandardIsResearchZeroTwoSectionOnesPrices() {
        CostTable aws = CostTable.awsS3Standard();
        assertThat(aws.putPerThousand()).isEqualTo(5_000L);
        assertThat(aws.getPerThousand()).isEqualTo(400L);
        assertThat(aws.listPerThousand()).isEqualTo(5_000L);
    }

    @Test
    void carriesTheRatesItWasGiven() {
        CostTable t = new CostTable(5000, 400, 5000);
        assertThat(t.putPerThousand()).isEqualTo(5000);
        assertThat(t.getPerThousand()).isEqualTo(400);
        assertThat(t.listPerThousand()).isEqualTo(5000);
    }
}
