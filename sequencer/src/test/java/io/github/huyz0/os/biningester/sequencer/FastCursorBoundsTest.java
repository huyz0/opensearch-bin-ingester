// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.RunKey;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A stale commit, a resume exactly at the committed offset, a batch past
 * {@code B}, a re-open after a switch and negative offsets (M13.27 review
 * round 1, P1, P2, T1-T3).
 */
class FastCursorBoundsTest {

    private static final RunKey A = new RunKey(new UUID(1, 1), 0);

    @Test
    void aSTALECommitNeverLowersTheBoundAResumeIsCheckedAgainst() {
        FastCursor cursor = new FastCursor(100);
        cursor.open(A, 0);
        cursor.committed(A, 30);

        cursor.committed(A, 5);

        assertThatThrownBy(() -> cursor.resume(A, 10))
                .as("offsets 10-29 are committed").isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aRESUMEExactlyAtTheCommittedOffsetIsAccepted() {
        FastCursor cursor = new FastCursor(100);
        cursor.open(A, 0);
        cursor.assign(A, 40);
        cursor.committed(A, 40);

        cursor.resume(A, 40);

        assertThat(cursor.assign(A, 1)).isEqualTo(OptionalLong.of(40));
    }

    @Test
    void aBATCHLargerThanTheBoundIsRefusedNeverLeftWaiting() {
        FastCursor cursor = new FastCursor(10);
        cursor.open(A, 0);

        assertThatThrownBy(() -> cursor.assign(A, 11)).isInstanceOf(IllegalArgumentException.class);
        assertThat(cursor.assign(A, 10)).isEqualTo(OptionalLong.of(0));
    }

    @Test
    void aSTREAMSwitchedBackIsReopenedAtItsNewCommittedOffset() {
        FastCursor cursor = new FastCursor(1_000);
        cursor.open(A, 0);
        cursor.assign(A, 500);
        cursor.committed(A, 500);

        cursor.open(A, 1_000);

        assertThat(cursor.assign(A, 1)).as("500-999 were committed on the default path")
                .isEqualTo(OptionalLong.of(1_000));
    }

    @Test
    void aNEGATIVEOffsetIsRefused() {
        FastCursor cursor = new FastCursor(100);

        assertThatThrownBy(() -> cursor.open(A, -1)).isInstanceOf(IllegalArgumentException.class);
        cursor.open(A, 0);
        assertThatThrownBy(() -> cursor.committed(A, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
