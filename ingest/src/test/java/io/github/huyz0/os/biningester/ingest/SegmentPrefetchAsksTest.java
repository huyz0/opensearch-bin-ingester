// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The prefetcher asks the governor only when it would fetch (M11.11, H6;
 * M10.11 review R2 and T4): not for a segment its own AZ wrote, not with no
 * cache, not when another pod owns it, and not for a repeat of one already
 * fetched. The governor counts every {@code false} as a refusal, so each
 * needless ask on a halted pod is a refusal of nothing.
 */
class SegmentPrefetchAsksTest {

    private static final String SEGMENT = "bins/seg-durable";

    private static SegmentPrefetcher prefetcher(Peer self, List<Peer> az, long cacheBytes,
            AtomicInteger asked) throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        store.put(SEGMENT, Body.ofBytes(new byte[8192]));
        return new SegmentPrefetcher(new StaticMembership(self, az),
                new SegmentProxy(store, 64 * 1024, new SegmentCache(cacheBytes), new IndexCostLedger()),
                () -> {
                    asked.incrementAndGet();
                    return true;
                });
    }

    @Test
    void theGovernorIsNotAskedForASegmentThisPodWouldNotFetch() throws Exception {
        Peer self = new Peer("az-b-pod0", "http://b0:9000", "az-b");
        AtomicInteger asked = new AtomicInteger();

        prefetcher(self, List.of(self), 1L << 20, asked).onDurable(SEGMENT, "az-b");
        assertThat(asked).as("its own AZ wrote it").hasValue(0);
        prefetcher(self, List.of(self), 0, asked).onDurable(SEGMENT, "az-a");
        assertThat(asked).as("no cache to warm").hasValue(0);
        Peer other = new Peer("az-b-pod1", "http://b1:9000", "az-b");
        List<Peer> both = List.of(self, other);
        Peer owner = PeerRing.ownerOf(SEGMENT, new AzPeers("az-b", both)).orElseThrow();
        Peer notTheOwner = owner.podId().equals(self.podId()) ? other : self;
        SegmentPrefetcher notOwner = prefetcher(notTheOwner, both, 1L << 20, asked);
        notOwner.onDurable(SEGMENT, "az-a");
        assertThat(asked).as("another pod owns it").hasValue(0);
    }

    @Test
    void aRepeatSignalForASegmentAlreadyFetchedDoesNotAskTheGovernor() throws Exception {
        Peer self = new Peer("az-b-pod0", "http://b0:9000", "az-b");
        AtomicInteger asked = new AtomicInteger();
        SegmentPrefetcher prefetcher = prefetcher(self, List.of(self), 1L << 20, asked);

        assertThat(prefetcher.onDurable(SEGMENT, "az-a")).as("the premise: fetched").isTrue();
        assertThat(asked).hasValue(1);
        assertThat(prefetcher.onDurable(SEGMENT, "az-a")).isFalse();
        assertThat(asked)
                .as("⚠️ THE REPEAT IS ALREADY FETCHED: not asked, so a halted pod does not "
                        + "count a refusal of work it would not have done")
                .hasValue(1);
    }
}
