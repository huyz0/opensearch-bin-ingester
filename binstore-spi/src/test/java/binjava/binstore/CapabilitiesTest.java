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
        return new Capabilities(conditionalWrites, true, false, 1024, 0, CostTable.free());
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

    /**
     * ⚠️ THE PASSING BRANCH, which nothing covered. Review MEASURED
     * {@code if (!presignedUrls)} rewritten to {@code if (true)} surviving the
     * whole 1,051-test suite: a startup check refusing EVERY backend, a capable
     * one included, would have shipped green. The conformance suite cannot
     * catch it -- its call sits inside the branch no shipping backend executes.
     * `requireConditionalWrites` has carried this pair since M2.1; the new
     * capability arrived with only the refusing half.
     */
    @Test
    void requirePresignedUrlsPassesSilentlyWhenTheBackendHasThem() {
        new Capabilities(true, true, true, 1024, 0, CostTable.free())
                .requirePresignedUrls();
    }

    @Test
    void requirePresignedUrlsFailsLoudlyWhenTheBackendLacksThem() {
        org.assertj.core.api.Assertions
                .assertThatThrownBy(() -> new Capabilities(true, true, false, 1024, 0,
                        CostTable.free()).requirePresignedUrls())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("refusing to start");
    }
}
