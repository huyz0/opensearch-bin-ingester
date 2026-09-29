// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.client.SegmentFetchHeldException;
import io.github.huyz0.os.biningester.client.SegmentSource;
import io.github.huyz0.os.biningester.format.Grant;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * Where a held failure's count starts again (M10.28 review T1 and F3): after
 * a success, and after a failure long past. And what a held answer tells the
 * run: the hold's time left and the node's own fetch count.
 */
class NodeSegmentSourceFailureResetTest {

    private static final long FLOOR = NodeSegmentSource.FAILURE_HOLD_FLOOR.toMillis();

    /** Fails while {@code failing}; four bytes otherwise. */
    private static final class Switch implements SegmentSource {
        final AtomicInteger calls = new AtomicInteger();
        final AtomicBoolean failing = new AtomicBoolean(true);

        @Override
        public byte[] fetch(Grant grant) {
            throw new AssertionError("route only");
        }

        @Override
        public byte[] fetchSegment(String segmentKey) throws IOException {
            calls.incrementAndGet();
            if (failing.get()) {
                throw new IOException("503 from the store");
            }
            return new byte[4];
        }
    }

    /** ⚠️ A HOLD OF NO BYTES, so a success is never answered from held bytes. */
    private static NodeSegmentSource unheldBytes(Switch delegate, AtomicLong millis) {
        NodeSegmentSource source = new NodeSegmentSource(delegate, 0);
        source.holdFailures(millis::get, java.util.function.LongUnaryOperator.identity());
        return source;
    }

    @Test
    void aSuccessStartsTheNextFailuresHoldAtTheFloorAgain() throws Exception {
        Switch delegate = new Switch();
        AtomicLong millis = new AtomicLong(1_000_000);
        NodeSegmentSource source = unheldBytes(delegate, millis);

        assertThatThrownBy(() -> source.fetchSegment("seg")).isInstanceOf(IOException.class);
        millis.addAndGet(FLOOR);
        assertThatThrownBy(() -> source.fetchSegment("seg")).isInstanceOf(IOException.class);
        millis.addAndGet(2 * FLOOR);
        delegate.failing.set(false);
        assertThat(source.fetchSegment("seg")).hasSize(4);
        delegate.failing.set(true);
        assertThatThrownBy(() -> source.fetchSegment("seg")).isInstanceOf(IOException.class);
        assertThat(delegate.calls).as("the premise: four fetches").hasValue(4);

        millis.addAndGet(FLOOR);
        assertThatThrownBy(() -> source.fetchSegment("seg")).isInstanceOf(IOException.class);
        assertThat(delegate.calls)
                .as("⚠️ THE SUCCESS CLEARED THE COUNT: the new failure was held one floor, "
                        + "not the four a third consecutive failure would be")
                .hasValue(5);
    }

    @Test
    void aFailureLongPastStartsTheNextAtTheFloor() {
        Switch delegate = new Switch();
        AtomicLong millis = new AtomicLong(1_000_000);
        NodeSegmentSource source = unheldBytes(delegate, millis);
        for (int i = 0; i < 6; i++) {
            assertThatThrownBy(() -> source.fetchSegment("seg")).isInstanceOf(IOException.class);
            millis.addAndGet(NodeSegmentSource.FAILURE_HOLD_CEILING.toMillis());
        }
        assertThat(delegate.calls).as("the premise: six consecutive failures").hasValue(6);

        millis.addAndGet(NodeSegmentSource.FAILURE_HOLD_CEILING.toMillis() + 1);
        assertThatThrownBy(() -> source.fetchSegment("seg")).isInstanceOf(IOException.class);
        millis.addAndGet(FLOOR);
        assertThatThrownBy(() -> source.fetchSegment("seg")).isInstanceOf(IOException.class);
        assertThat(delegate.calls)
                .as("⚠️ AN HOUR-OLD OUTAGE's COUNT IS NOT THIS ONE's: held one floor, not "
                        + "the ceiling")
                .hasValue(8);
    }

    @Test
    void aHeldAnswerCarriesTheHoldsTimeLeftAndTheNodesFetchCount() {
        Switch delegate = new Switch();
        AtomicLong millis = new AtomicLong(1_000_000);
        NodeSegmentSource source = unheldBytes(delegate, millis);
        assertThatThrownBy(() -> source.fetchSegment("seg")).isInstanceOf(IOException.class);
        millis.addAndGet(FLOOR);
        assertThatThrownBy(() -> source.fetchSegment("seg")).isInstanceOf(IOException.class);

        millis.addAndGet(500);
        assertThatThrownBy(() -> source.fetchSegment("seg"))
                .isInstanceOfSatisfying(SegmentFetchHeldException.class, held -> {
                    assertThat(held.remaining()).isEqualTo(Duration.ofMillis(2 * FLOOR - 500));
                    assertThat(held.nodeAttempts()).isEqualTo(2);
                    assertThat(held).hasRootCauseMessage("503 from the store");
                });
    }
}
