// SPDX-License-Identifier: Apache-2.0
package binjava.fix;

import org.junit.jupiter.api.Test;

/** A sentence a maintainer added. */
class BindingFixture {

    // ⚠️ A text block and an annotated field in the REMAINDER. Round 1's
    // blocking finding was demonstrated on an NDJSON text block, and in real
    // files such a payload is a shared constant -- which lands here, not in a
    // test declaration. Without one, blanking remainder text blocks is a no-op
    // on every fixture and the mutation cannot be caught.
    private static final String PAYLOAD = """
        {"id":"aaa"}
        """;

    private static int helper() {
        return 1;
    }

    @Timeout(5)
    @Test
    void alpha() {
        assertThat(helper()).isEqualTo(1);
        assertThat("expected-text").isNotNull();
        assertThat("two  spaces").isNotNull();
        assertThat('a').isNotNull();
        assertThat("""
            payload-aaa
            """).isNotNull();
    }

    @SuppressWarnings("unused")
    private static final String SHARED = "shared  const";

    @Test
    void beta() {
        assertThat(helper()).isEqualTo(1);
    }
}
