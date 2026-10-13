// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A live delivery left undecoded while a gap repair is pending, then
 * overlapped by the replay that repaired it (M13.30h review round 1, P1;
 * ADR-0085): its commit is undone to what the replay committed, never below
 * it -- so the next poll hands on only the offsets the replay did not.
 */
class GapTrackerOverlapRaceTest {

    private static final RunKey KEY = new RunKey(new UUID(1, 0xac), 0);

    private static Delivery delivery(long firstOffset, int records) {
        return new Delivery(KEY, "seg-" + firstOffset, records, firstOffset,
                FetchMode.INLINE, new byte[0]);
    }

    @Test
    void anUNDONEOverlapIsUndoneToWhatTheRaceCommittedNotBelowIt() {
        GapTracker tracker = new GapTracker(KEY);
        tracker.onGap(gap -> { });
        tracker.commit(delivery(0, 10), false, true);
        Delivery live = delivery(15, 5);
        assertThat(tracker.commit(live, false, false).report())
                .as("PREMISE: offsets 10 to 14 are a gap, repaired by a replay").isNotNull();
        assertThat(tracker.wouldDecode(live, false))
                .as("PREMISE: held while the repair is pending").isFalse();

        tracker.commit(delivery(10, 8), true, true);
        GapTracker.Commit undone = tracker.commit(live, false, false);
        GapTracker.Commit next = tracker.commit(live, false, true);

        assertThat(undone.undone()).as("decoded nothing, so not consumed").isTrue();
        assertThat(next.keepFrom()).as("the replay's 10 to 17 are not handed on twice")
                .isEqualTo(18);
        assertThat(next.covered()).isEqualTo(2);
    }
}
