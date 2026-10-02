// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.RunKey;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The leader's per-stream cursor and its bound {@code B} (ADR-0081 §4; M13.27).
 */
class FastCursorTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);
    private static final RunKey B = new RunKey(new UUID(1, 2), 0);

    @Test
    void OFFSETSAreAssignedFromTheCommittedNextOffsetPerStream() {
        FastCursor cursor = new FastCursor(100);
        cursor.open(A, 40);
        cursor.open(B, 0);

        assertThat(cursor.assign(A, 3)).isEqualTo(OptionalLong.of(40));
        assertThat(cursor.assign(A, 2)).isEqualTo(OptionalLong.of(43));
        assertThat(cursor.assign(B, 1)).isEqualTo(OptionalLong.of(0));
        assertThat(cursor.cursor(A)).isEqualTo(45);
    }

    @Test
    void aBATCHThatWouldPassTheBoundWaitsUntilACommitMovesIt() {
        FastCursor cursor = new FastCursor(10);
        cursor.open(A, 0);
        assertThat(cursor.assign(A, 8)).isEqualTo(OptionalLong.of(0));

        assertThat(cursor.assign(A, 3)).as("11 uncommitted offsets would pass B = 10").isEmpty();
        assertThat(cursor.assign(A, 2)).as("exactly B").isEqualTo(OptionalLong.of(8));
        assertThat(cursor.assign(A, 1)).isEmpty();
        cursor.committed(A, 4);

        assertThat(cursor.assign(A, 4)).isEqualTo(OptionalLong.of(10));
    }

    @Test
    void anUPLOADIsDueAtHalfTheBound() {
        FastCursor cursor = new FastCursor(10);
        cursor.open(A, 0);
        cursor.assign(A, 4);
        assertThat(cursor.uploadDue(A)).isFalse();

        cursor.assign(A, 1);

        assertThat(cursor.uploadDue(A)).isTrue();
        cursor.committed(A, 5);
        assertThat(cursor.uploadDue(A)).isFalse();
    }

    @Test
    void aDECISIONResetsTheCursorButNeverBelowTheCommitted() {
        FastCursor cursor = new FastCursor(100);
        cursor.open(A, 10);
        cursor.assign(A, 20);
        cursor.committed(A, 15);

        cursor.resume(A, 18);

        assertThat(cursor.assign(A, 1)).as("the entries from 18 up are superseded")
                .isEqualTo(OptionalLong.of(18));
        assertThatThrownBy(() -> cursor.resume(A, 14))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aCOMMITNeverMovesTheBoundDownAndCarriesALaggingCursor() {
        FastCursor cursor = new FastCursor(100);
        cursor.open(A, 10);
        cursor.committed(A, 5);
        assertThat(cursor.cursor(A)).isEqualTo(10);

        cursor.committed(A, 30);

        assertThat(cursor.cursor(A)).as("a takeover committed past the cursor").isEqualTo(30);
    }

    @Test
    void anUNOPENEDStreamOrABadBatchIsRefused() {
        FastCursor cursor = new FastCursor(100);

        assertThatThrownBy(() -> cursor.assign(A, 1)).isInstanceOf(IllegalStateException.class);
        cursor.open(A, 0);
        assertThatThrownBy(() -> cursor.assign(A, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FastCursor(1)).isInstanceOf(IllegalArgumentException.class);
        cursor.open(A, 50);
        assertThat(cursor.cursor(A)).as("a re-open at a higher committed offset raises it")
                .isEqualTo(50);
    }
}
