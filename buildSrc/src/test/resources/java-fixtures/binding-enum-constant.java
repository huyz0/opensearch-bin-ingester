// SPDX-License-Identifier: Apache-2.0
package binjava.fix;

import org.junit.jupiter.api.Test;

class EnumConstantFixture {

    // ⚠️ An ANNOTATED ENUM CONSTANT whose enum body has no trailing `;`. This is
    // the construct round 7 could not build and therefore recorded the `close`
    // branch's `run_start` reset as an equivalent mutant; round 8 built it. The
    // annotation run arms on `@Deprecated`, no `;` or `(` follows it, and the
    // body closes with `}` while the run is still open -- so `close` is the only
    // branch that can clear it. Delete that reset and `alpha`'s span starts at
    // `@Deprecated`, swallowing the whole enum out of the shared remainder:
    // renaming the `OLD` DECLARATION would then leave `beta`'s red record
    // valid. A FALSE GREEN, not churn. (The declaration occurrence is the whole
    // claim: `beta`'s body names `OLD` too, so a GLOBAL rename still moves its
    // key even under the mutation.) Caught by the head predicate, which sees
    // `}` before the body.
    // ⚠️ Plausible rather than present: three of this tree's four enums already
    // omit the trailing `;`, but NONE annotates a constant -- and the annotation
    // is the half that arms the run. Deprecating one enum value is ordinary Java,
    // so this is one edit away. An earlier draft of this line claimed both halves
    // were already present in `AmbiguousPutStore`; review grepped it, and they
    // were not.
    enum Phase {
        @Deprecated
        OLD,
        NEW
    }

    @Test
    void alpha() {
        assertThat(Phase.NEW).isNotNull();
    }

    @Test
    void beta() {
        assertThat(Phase.OLD).isNotNull();
    }
}
