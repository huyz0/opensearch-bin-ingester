// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * The durable-segment prefetch honours the governor's discretionary halt
 * (M10.11, ADR-0075, criterion 14).
 *
 * <p>⚠️ **A PREFETCH IS AN OPTIMISATION**, so halting it is safe: a consumer
 * the ring owner has not warmed reads through ADR-0012's ladder, down to a cold
 * GET. What a halt must not do is let the GET out anyway.
 */
class SegmentPrefetchGovernorTest {

    private static final String SEGMENT = "bins/seg-durable";

    @Test
    void aHALTEDPrefetcherIssuesNOGetAndAFTERTheHaltTheSegmentIsStillWarmed() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        store.put(SEGMENT, Body.ofBytes(new byte[8192]));
        // ⚠️ THE AZ's ONLY POD, IN AN AZ THAT DID NOT WRITE IT: the ring owner,
        // so every other reason not to fetch is ruled out.
        Peer self = new Peer("az-b-pod0", "http://b:9000", "az-b");
        AtomicBoolean allowed = new AtomicBoolean(false);
        SegmentPrefetcher prefetcher = new SegmentPrefetcher(
                new StaticMembership(self, List.of(self)),
                new SegmentProxy(store, 64 * 1024, SegmentCache.forSegmentsOf(1L << 20)),
                allowed::get);
        long before = store.counts().gets();

        boolean fetchedWhileHalted = prefetcher.onDurable(SEGMENT, "az-a");

        assertThat(fetchedWhileHalted).as("halted: nothing fetched").isFalse();
        assertThat(store.counts().gets() - before)
                .as("⚠️ NO GET WHILE DISCRETIONARY WORK IS HALTED").isZero();

        allowed.set(true);
        assertThat(prefetcher.onDurable(SEGMENT, "az-a"))
                .as("⚠️ A HALTED SIGNAL IS NOT REMEMBERED AS FETCHED, so the next one warms "
                        + "the segment rather than being deduplicated against nothing")
                .isTrue();
        assertThat(store.counts().gets() - before).isEqualTo(1);
    }
}
