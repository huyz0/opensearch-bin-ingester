// SPDX-License-Identifier: Apache-2.0
package binjava.fix;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class NestedFixture {

    @Nested
    class Inner {

        @Test
        void inside() {
            assertThat(1).isEqualTo(1);
        }
    }

    @Test
    void outside() {
        assertThat(2).isEqualTo(2);
    }
}
