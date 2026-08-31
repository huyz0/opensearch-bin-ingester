// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

/** ⚠️ An ack means durable (FR-4), and offsets within one append are contiguous. */
class AppendResultTest {

    @Test
    void aSingleRecordHasOneOffset() {
        AppendResult r = new AppendResult(1, 42, 42);
        // ⚠️ The VALUES, so a constructor that blanks or swaps them fails here
        // rather than surfacing as a consumer resuming from offset zero.
        assertThat(r.firstOffset()).isEqualTo(42);
        assertThat(r.lastOffset()).isEqualTo(42);
        assertThat(r.recordCount()).isEqualTo(1);
        AppendResult many = new AppendResult(4, 100, 103);
        assertThat(many.firstOffset()).isEqualTo(100);
        assertThat(many.lastOffset()).isEqualTo(103);
    }

    @Test
    void theOffsetRangeMustMatchTheRecordCount() {
        assertThat(new AppendResult(3, 10, 12).recordCount()).isEqualTo(3);
        // ⚠️ A gap in the range is a gap in the LOG, and a gap stalls every
        // consumer of that partition -- which is why offsets are assigned at
        // commit and never pre-allocated before the PUT.
        assertThatThrownBy(() -> new AppendResult(3, 10, 99))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AppendResult(3, 10, 11))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anEmptyAppendHasNoResult() {
        // ⚠️ The MESSAGE, not just the type. recordCount=0 also fails the
        // range check (0-0 != -1), so asserting only IllegalArgumentException
        // passed while the recordCount guard itself was deleted -- the test was
        // green for a reason it does not name.
        assertThatThrownBy(() -> new AppendResult(0, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("append of nothing");
    }

    @Test
    void offsetsAreNeverNegativeAndNeverRunBackwards() {
        assertThatThrownBy(() -> new AppendResult(1, -1, -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new AppendResult(1, 10, 9))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
