// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.client.SegmentSource;
import io.github.huyz0.os.biningester.format.Grant;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * A FAILED fetch is held per node and segment, so the node's K runs of one
 * failing segment cost the delegate one fetch per backoff, not one each
 * (M10.28, NFR-4, non-negotiable 6).
 */
class NodeSegmentSourceFailureHoldTest {

    private static final Grant GRANT = new Grant("https://store.example/seg",
            Instant.EPOCH.plus(Duration.ofMinutes(1)));

    /** Fails while {@code failing}, counting calls. */
    private static final class Flaky implements SegmentSource {
        final AtomicInteger calls = new AtomicInteger();
        final AtomicBoolean failing = new AtomicBoolean(true);

        @Override
        public byte[] fetch(Grant grant) throws IOException {
            return answer();
        }

        @Override
        public byte[] fetchSegment(String segmentKey) throws IOException {
            return answer();
        }

        private byte[] answer() throws IOException {
            calls.incrementAndGet();
            if (failing.get()) {
                throw new IOException("503 from the store");
            }
            return new byte[4];
        }
    }

    private static NodeSegmentSource held(Flaky delegate, AtomicLong millis) {
        NodeSegmentSource source = new NodeSegmentSource(delegate, 1L << 20);
        source.holdFailures(millis::get, java.util.function.LongUnaryOperator.identity());
        return source;
    }

    @Test
    void theNodesRunsOfOneFailingSegmentShareOneFetchPerHold() {
        Flaky delegate = new Flaky();
        AtomicLong millis = new AtomicLong(1_000_000);
        NodeSegmentSource source = held(delegate, millis);

        for (int run = 0; run < 16; run++) {
            assertThatThrownBy(() -> source.fetchSegment("seg"))
                    .as("⚠️ EVERY RUN STILL SEES THE FAILURE: never an empty segment")
                    .isInstanceOf(IOException.class).hasMessageContaining("503 from the store");
            assertThatThrownBy(() -> source.fetch(GRANT)).isInstanceOf(IOException.class);
        }

        assertThat(delegate.calls)
                .as("⚠️ ONE FETCH PER KEY FOR 16 RUNS: the node's hold, not one per run")
                .hasValue(2);
    }

    @Test
    void theHoldDoublesWithEachFailureUpToTheRetryCeilingAndASuccessClearsIt()
            throws Exception {
        Flaky delegate = new Flaky();
        AtomicLong millis = new AtomicLong(1_000_000);
        NodeSegmentSource source = held(delegate, millis);
        Duration floor = NodeSegmentSource.FAILURE_HOLD_FLOOR;

        assertThatThrownBy(() -> source.fetchSegment("seg")).isInstanceOf(IOException.class);
        millis.addAndGet(floor.toMillis() - 1);
        assertThatThrownBy(() -> source.fetchSegment("seg")).isInstanceOf(IOException.class);
        assertThat(delegate.calls).as("inside the first hold").hasValue(1);

        millis.addAndGet(1);
        assertThatThrownBy(() -> source.fetchSegment("seg")).isInstanceOf(IOException.class);
        assertThat(delegate.calls).as("the first hold is over: fetched again").hasValue(2);
        millis.addAndGet(2 * floor.toMillis() - 1);
        assertThatThrownBy(() -> source.fetchSegment("seg")).isInstanceOf(IOException.class);
        assertThat(delegate.calls).as("⚠️ THE SECOND HOLD IS TWICE THE FIRST").hasValue(2);

        long ceiling = NodeSegmentSource.FAILURE_HOLD_CEILING.toMillis();
        for (int i = 0; i < 20; i++) {
            millis.addAndGet(ceiling);
            assertThatThrownBy(() -> source.fetchSegment("seg")).isInstanceOf(IOException.class);
        }
        assertThat(delegate.calls).as("the premise: every hold at most the ceiling").hasValue(22);
        millis.addAndGet(ceiling - 1);
        assertThatThrownBy(() -> source.fetchSegment("seg")).isInstanceOf(IOException.class);
        assertThat(delegate.calls).as("still held just inside the ceiling").hasValue(22);
        millis.addAndGet(1);
        assertThatThrownBy(() -> source.fetchSegment("seg")).isInstanceOf(IOException.class);
        assertThat(delegate.calls)
                .as("⚠️ THE HOLD STOPS DOUBLING AT THE CEILING: 21 failures, 30 s, not 2^20 s")
                .hasValue(23);

        delegate.failing.set(false);
        millis.addAndGet(NodeSegmentSource.FAILURE_HOLD_CEILING.toMillis());
        assertThat(source.fetchSegment("seg")).hasSize(4);
        delegate.failing.set(true);
        source.fetchSegment("seg");
        assertThat(delegate.calls).as("a success is held as bytes, as before").hasValue(24);
        assertThatThrownBy(() -> source.fetchSegment("other")).isInstanceOf(IOException.class);
        millis.addAndGet(floor.toMillis());
        assertThatThrownBy(() -> source.fetchSegment("other")).isInstanceOf(IOException.class);
        assertThat(delegate.calls).as("a new failure starts at the floor again").hasValue(26);
    }

    @Test
    void withNoClockInstalledEveryFetchReachesTheDelegateAsBefore() {
        Flaky delegate = new Flaky();
        NodeSegmentSource source = new NodeSegmentSource(delegate, 1L << 20);

        for (int run = 0; run < 3; run++) {
            assertThatThrownBy(() -> source.fetchSegment("seg")).isInstanceOf(IOException.class);
        }
        assertThat(delegate.calls).hasValue(3);
    }
}
