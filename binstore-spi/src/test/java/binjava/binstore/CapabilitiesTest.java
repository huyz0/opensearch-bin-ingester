// SPDX-License-Identifier: Apache-2.0
package binjava.binstore;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * ADR-0008's startup check: a backend lacking conditional writes must fail
 * loudly rather than be wired in silently.
 */
class CapabilitiesTest {

    private static Capabilities with(boolean conditionalWrites) {
        return new Capabilities(conditionalWrites, true, 1024, 0, CostTable.free());
    }

    @Test
    void requireConditionalWritesFailsLoudlyWhenTheBackendLacksThem() {
        // ⚠️ M2.1: nothing called this before this task, so a backend
        // advertising conditionalWrites=false was wired in silently -- this is
        // the ADR-0008 check actually happening, not aspiration.
        assertThatThrownBy(() -> with(false).requireConditionalWrites())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("conditional writes");
    }

    @Test
    void requireConditionalWritesPassesSilentlyWhenTheBackendHasThem() {
        assertThatCode(() -> with(true).requireConditionalWrites()).doesNotThrowAnyException();
    }
}
