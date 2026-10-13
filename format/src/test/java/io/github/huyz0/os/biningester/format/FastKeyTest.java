// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/**
 * A fast batch's key (ADR-0082 §6; M13.30j), against a golden computed by an
 * independent encoder from the layout (Python's {@code %016x} and base64url,
 * not this codec): every field where the ADR puts it, the index as the
 * OpenSearch UUID the plugin registered, and never over the key bound.
 */
class FastKeyTest {

    private static final RunKey ORDERS_12 = RunKey.ofIndexUuid("nVzgup36TLqWp7VBBREj1w", 12);

    @Test
    void aFASTKeyIsTheLayoutADR0082Gives() {
        assertThat(FastKey.of("bins/cluster-a", 7, 0xFFFF_FFFEL, ORDERS_12, 40))
                .isEqualTo("bins/cluster-a/fast/0000000000000007/fffffffe/"
                        + "nVzgup36TLqWp7VBBREj1w/12/0000000000000028");
    }

    @Test
    void aFASTKeyParsesToItsFieldsAndNothingElseDoes() {
        // ⚠️ M13.30l: what /seg serves by; a prefix may itself hold "/fast/".
        String key = FastKey.of("bins/fast/a", 7, 0xFFFF_FFFEL, ORDERS_12, 40);

        assertThat(FastKey.parse(key)).hasValue(new FastKey.Parsed("bins/fast/a", 7,
                0xFFFF_FFFEL, ORDERS_12, 40));
        assertThat(FastKey.parse(key.replace("/fffffffe/", "/FFFFFFFE/")))
                .as("hex that parses, but not as it is written").isEmpty();
        assertThat(FastKey.parse("bins/cluster-a/data/2026/10/13/08/x.bseg")).isEmpty();
        assertThat(FastKey.parse(null)).isEmpty();
    }

    @Test
    void aKEYOverTheBoundOrAnAssignedAfterOverAU32IsNone() {
        assertThatThrownBy(() -> FastKey.of("p".repeat(1_000), 7, 0, ORDERS_12, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FastKey.of("p", 7, 1L << 32, ORDERS_12, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
