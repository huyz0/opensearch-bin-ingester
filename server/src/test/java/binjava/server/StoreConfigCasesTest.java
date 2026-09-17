// SPDX-License-Identifier: Apache-2.0
package binjava.server;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;
import org.junit.jupiter.api.Test;

/** The configuration record's own refusals (M8.1). */
class StoreConfigCasesTest {

    @Test
    void aNULLKindIsREFUSED() {
        assertThatThrownBy(() -> new StoreConfig(null, Optional.empty()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void aBLANKKindIsREFUSED() {
        assertThatThrownBy(() -> new StoreConfig("  ", Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("store");
    }

    @Test
    void aNULLRootOptionalIsREFUSED() {
        assertThatThrownBy(() -> new StoreConfig("memory", null))
                .isInstanceOf(NullPointerException.class);
    }
}
