// SPDX-License-Identifier: Apache-2.0
package p;

import org.junit.jupiter.api.Test;

class SpacedAnnotationTest {

    @SuppressWarnings ("unchecked")
    @Test
    void spaced() {
        assertThat(1).isEqualTo(1);
    }
}
