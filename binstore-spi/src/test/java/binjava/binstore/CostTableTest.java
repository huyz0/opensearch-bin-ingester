// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

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

    @Test
    void carriesTheRatesItWasGiven() {
        CostTable t = new CostTable(5000, 400, 5000);
        assertThat(t.putPerThousand()).isEqualTo(5000);
        assertThat(t.getPerThousand()).isEqualTo(400);
        assertThat(t.listPerThousand()).isEqualTo(5000);
    }
}
