// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.client.SegmentFetchHeldException;
import io.github.huyz0.os.biningester.client.SegmentSource;
import io.github.huyz0.os.biningester.format.Grant;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongUnaryOperator;
import org.junit.jupiter.api.Test;

/**
 * M12.19b (M10.25 T1/T2, M10.28b T1/T3): four edges of the node's segment hold
 * that no case pinned -- a segment exactly the hold's size is held; a
 * re-fetch after eviction is counted once, even when its first try fails; a
 * failure is held from when it FAILED, not from when its fetch began; and the
 * held-failure map is bounded, oldest dropped first.
 */
class NodeSegmentSourcePinsTest {

    /** Serves {@code size} bytes for every key, failing the keys in {@code failing}. */
    private static final class Store implements SegmentSource {
        final AtomicInteger calls = new AtomicInteger();
        final Set<String> failing = new HashSet<>();
        final int size;
        Runnable duringFetch = () -> { };

        Store(int size) {
            this.size = size;
        }

        @Override
        public byte[] fetch(Grant grant) {
            throw new AssertionError("by key only");
        }

        @Override
        public byte[] fetchSegment(String segmentKey) throws IOException {
            calls.incrementAndGet();
            duringFetch.run();
            if (failing.contains(segmentKey)) {
                throw new IOException("503 from the store for " + segmentKey);
            }
            return new byte[size];
        }
    }

    @Test
    void aSegmentExactlyTheHoldsSizeIsHeldNotRefetched() throws Exception {
        Store store = new Store(100);
        NodeSegmentSource source = new NodeSegmentSource(store, 100);

        source.fetchSegment("a");
        source.fetchSegment("a");

        assertThat(store.calls).as("⚠️ AT THE CEILING IS WITHIN IT: the second read is a hit")
                .hasValue(1);
        assertThat(source.oversizeFetches()).isZero();
    }

    @Test
    void aRefetchAfterEvictionIsCountedOnceThoughItsFirstTryFails() throws Exception {
        Store store = new Store(100);
        NodeSegmentSource source = new NodeSegmentSource(store, 100);
        source.fetchSegment("a");
        source.fetchSegment("b"); // evicts a: the hold is one segment
        store.failing.add("a");

        assertThatThrownBy(() -> source.fetchSegment("a")).isInstanceOf(IOException.class);
        assertThat(source.refetchesAfterEviction()).as("the failed re-fetch is the re-fetch")
                .isEqualTo(1);
        store.failing.remove("a");
        source.fetchSegment("a");

        assertThat(source.refetchesAfterEviction())
                .as("⚠️ ONCE: the retry that succeeds is the same re-fetch, not a second one")
                .isEqualTo(1);
    }

    @Test
    void aFailureIsHeldFromWhenItFailedNotFromWhenItsFetchBegan() throws Exception {
        AtomicLong millis = new AtomicLong(1_000_000);
        Store store = new Store(10);
        store.failing.add("slow");
        long slow = 5_000;
        store.duringFetch = () -> millis.addAndGet(slow); // the fetch takes 5 s, then fails
        NodeSegmentSource source = new NodeSegmentSource(store, 1 << 20);
        source.holdFailures(millis::get, LongUnaryOperator.identity());
        long started = millis.get();

        assertThatThrownBy(() -> source.fetchSegment("slow")).isInstanceOf(IOException.class);
        store.duringFetch = () -> { };
        long floor = NodeSegmentSource.FAILURE_HOLD_FLOOR.toMillis();
        millis.set(started + slow + floor - 1);

        assertThatThrownBy(() -> source.fetchSegment("slow"))
                .as("⚠️ STILL HELD a floor after the FAILURE; measured from the fetch's start "
                        + "the hold would have ended %d ms ago", slow - 1)
                .isInstanceOf(SegmentFetchHeldException.class);
        assertThat(store.calls).hasValue(1);
    }

    @Test
    void theHeldFailuresAreBoundedAndTheOldestIsDroppedFirst() throws Exception {
        AtomicLong millis = new AtomicLong(1_000_000);
        Store store = new Store(10);
        NodeSegmentSource source = new NodeSegmentSource(store, 1 << 20);
        source.holdFailures(millis::get, LongUnaryOperator.identity());
        int bound = NodeSegmentSource.EVICTED_KEYS_REMEMBERED;
        for (int i = 0; i <= bound; i++) {
            String key = "k" + i;
            store.failing.add(key);
            assertThatThrownBy(() -> source.fetchSegment(key)).isInstanceOf(IOException.class);
        }
        int calls = store.calls.get();

        assertThatThrownBy(() -> source.fetchSegment("k" + bound))
                .as("the newest failure is still held").isInstanceOf(SegmentFetchHeldException.class);
        assertThatThrownBy(() -> source.fetchSegment("k1"))
                .as("⚠️ AND THE SECOND-OLDEST: exactly %d are held, not fewer", bound)
                .isInstanceOf(SegmentFetchHeldException.class);
        assertThatThrownBy(() -> source.fetchSegment("k0"))
                .as("⚠️ THE OLDEST WAS DROPPED at %d + 1 held failures: fetched again, not held",
                        bound)
                .isInstanceOf(IOException.class)
                .isNotInstanceOf(SegmentFetchHeldException.class);
        assertThat(store.calls.get() - calls).as("one fetch: k0's").isEqualTo(1);
    }
}
