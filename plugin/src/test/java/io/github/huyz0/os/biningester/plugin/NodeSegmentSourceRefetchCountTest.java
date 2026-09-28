// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.SegmentSource;
import io.github.huyz0.os.biningester.client.SubscriptionMetrics;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.Grant;
import io.github.huyz0.os.biningester.format.RunKey;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The node hold's two re-fetch fall-backs are COUNTED (M10.25, FR-6, NFR-4): a
 * segment too large to hold, fetched again by every run on the node, and one
 * evicted before every run had read it. Either sets the ingester's request
 * rate by shards -- so a hold smaller than the working set shows up as a
 * number an operator can see, not as a request rate nobody attributes.
 */
class NodeSegmentSourceRefetchCountTest {

    private static Grant grant(String name) {
        return new Grant("https://store.example/" + name,
                Instant.EPOCH.plus(Duration.ofMinutes(1)));
    }

    /** Answers {@code size} bytes per key, counting calls. */
    private static final class Sized implements SegmentSource {
        final Map<String, Integer> sizes;
        final AtomicInteger calls = new AtomicInteger();

        Sized(Map<String, Integer> sizes) {
            this.sizes = sizes;
        }

        @Override
        public byte[] fetch(Grant grant) {
            calls.incrementAndGet();
            return new byte[sizes.get(grant.url().substring(grant.url().lastIndexOf('/') + 1))];
        }

        @Override
        public byte[] fetchSegment(String segmentKey) {
            calls.incrementAndGet();
            return new byte[sizes.get(segmentKey)];
        }
    }

    @Test
    void aSegmentTooLargeToHoldIsCountedEachTimeItIsFetched() throws Exception {
        Sized delegate = new Sized(Map.of("big", 20, "small", 5));
        NodeSegmentSource source = new NodeSegmentSource(delegate, 10);

        for (int run = 0; run < 3; run++) {
            source.fetch(grant("big"));
        }
        source.fetch(grant("small"));
        source.fetch(grant("small"));

        assertThat(delegate.calls).as("the premise: three GETs for one segment, one for the "
                + "other").hasValue(4);
        assertThat(source.oversizeFetches())
                .as("⚠️ EVERY FETCH OF THE OVERSIZE SEGMENT, each a GET the hold could not save")
                .isEqualTo(3);
        assertThat(source.refetchesAfterEviction()).isZero();
    }

    @Test
    void aSegmentEvictedAndThenAskedForAgainIsCountedOnceForThatRefetch() throws Exception {
        Sized delegate = new Sized(Map.of("a", 8, "b", 8));
        NodeSegmentSource source = new NodeSegmentSource(delegate, 10);

        source.fetchSegment("a");
        source.fetchSegment("a");
        assertThat(source.refetchesAfterEviction()).as("a hit is not a re-fetch").isZero();
        source.fetchSegment("b");
        assertThat(source.refetchesAfterEviction()).as("a first fetch is not a re-fetch")
                .isZero();

        source.fetchSegment("a");

        assertThat(delegate.calls).as("the premise: b evicted a, so a was fetched again")
                .hasValue(3);
        assertThat(source.refetchesAfterEviction())
                .as("⚠️ THE GET AN EVICTION COST: a segment held once, dropped, and fetched "
                        + "again").isEqualTo(1);
        assertThat(source.oversizeFetches()).isZero();
    }

    @Test
    void theNodesSubscriptionsExportBothCounts() throws Exception {
        Sized delegate = new Sized(Map.of("big", 20, "a", 8, "b", 8));
        NodeSegmentSource source = new NodeSegmentSource(delegate, 10);
        SubscriptionTransport transport = new SubscriptionTransport() {
            @Override
            public AutoCloseable subscribe(RunKey key, Listener listener) {
                return () -> { };
            }
        };
        try (NodeSubscriptions node = new NodeSubscriptions(transport, 16, source)) {
            source.fetchSegment("big");
            source.fetchSegment("a");
            source.fetchSegment("b");
            source.fetchSegment("a");

            assertThat(node.metrics().count(
                    SubscriptionMetrics.Counter.SEGMENT_HOLD_OVERSIZE_FETCHES))
                    .as("the node's metrics, which the plugin exports").isEqualTo(1);
            assertThat(node.metrics().count(
                    SubscriptionMetrics.Counter.SEGMENT_HOLD_REFETCHES_AFTER_EVICTION))
                    .isEqualTo(1);
        }
    }
}
