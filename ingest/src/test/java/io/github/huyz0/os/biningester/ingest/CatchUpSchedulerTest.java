// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class CatchUpSchedulerTest {

    @Test
    void servicesCatchUpAfterTheBoundedLiveQuantum() {
        CatchUpScheduler<String> scheduler = new CatchUpScheduler<>(2);
        scheduler.enqueueLive("live-1");
        scheduler.enqueueLive("live-2");
        scheduler.enqueueLive("live-3");
        scheduler.enqueueCatchUp("catch-up-1");

        assertThat(List.of(scheduler.next().orElseThrow(), scheduler.next().orElseThrow(),
                scheduler.next().orElseThrow())).containsExactly(
                "live-1", "live-2", "catch-up-1");
        assertThat(scheduler.next()).contains("live-3");
        assertThat(scheduler.next()).isEmpty();
    }

    @Test
    void liveWorkStillWinsWhenNoCatchUpIsPending() {
        CatchUpScheduler<String> scheduler = new CatchUpScheduler<>(1);
        scheduler.enqueueLive("live");

        assertThat(scheduler.next()).contains("live");
        assertThat(scheduler.next()).isEmpty();
    }

    @Test
    void catchUpWorkDrainsWhenTheLiveLaneIsEmpty() {
        CatchUpScheduler<String> scheduler = new CatchUpScheduler<>(1);
        scheduler.enqueueCatchUp("catch-up");

        assertThat(scheduler.next()).contains("catch-up");
        assertThat(scheduler.next()).isEmpty();
    }

    @Test
    void rejectsWorkWhenTheBoundIsFull() {
        CatchUpScheduler<String> scheduler = new CatchUpScheduler<>(1, 1);

        assertThat(scheduler.enqueueCatchUp("first")).isTrue();
        assertThat(scheduler.enqueueLive("second")).isFalse();
        assertThat(scheduler.next()).contains("first");
    }

    @Test
    void rejectsInvalidBounds() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new CatchUpScheduler<>(0))
                .isInstanceOf(IllegalArgumentException.class);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> new CatchUpScheduler<>(1, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
