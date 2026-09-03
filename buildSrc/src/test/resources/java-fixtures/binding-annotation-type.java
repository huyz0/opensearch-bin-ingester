// SPDX-License-Identifier: Apache-2.0
package binjava.fix;

import org.junit.jupiter.api.Test;

class AnnotationTypeFixture {

    // ⚠️ The MARKER form, with no `;` inside. `annotation-type.java` next door
    // is saved only by accident: its body carries a `default` clause whose
    // semicolon resets the annotation run. The marker form is what a composed
    // test annotation actually looks like, and with the run left dangling the
    // next test's span starts at `@Timeout(60)` and swallows this whole
    // declaration out of the shared remainder.
    @Timeout(60)
    @Retention(RetentionPolicy.RUNTIME)
    @interface Slow {}

    @Test
    void alpha() {
        assertThat(1).isEqualTo(1);
    }

    @Test
    void beta() {
        assertThat(2).isEqualTo(2);
    }
}
