// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A REPEATED signal refreshes its key in the remembered set (M11.11 review
 * R1): the set is evicted in ACCESS order, so the key that pays a second GET
 * is the one nobody has mentioned lately, never the oldest one still being
 * signalled.
 */
class SegmentPrefetchAccessOrderTest {

    @Test
    void aKeySignalledAgainIsNotTheOneEvicted() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        Peer self = new Peer("az-b-pod0", "http://b:9000", "az-b");
        SegmentPrefetcher prefetcher = new SegmentPrefetcher(
                new StaticMembership(self, List.of(self)),
                new SegmentProxy(store, 1024, new SegmentCache(4096), new IndexCostLedger()));
        int n = SegmentPrefetcher.KEYS_REMEMBERED;
        for (int i = 0; i <= n; i++) {
            store.put("bins/seg-" + i, Body.ofBytes(new byte[64]));
        }
        for (int i = 0; i < n; i++) {
            prefetcher.onDurable("bins/seg-" + i, "az-a");
        }
        assertThat(prefetcher.onDurable("bins/seg-0", "az-a"))
                .as("the premise: the oldest key is remembered, so the repeat fetches nothing")
                .isFalse();
        prefetcher.onDurable("bins/seg-" + n, "az-a");
        long before = store.counts().gets();

        assertThat(prefetcher.onDurable("bins/seg-0", "az-a"))
                .as("⚠️ SIGNALLED AGAIN, SO NOT THE LEAST RECENT: the overflow evicted seg-1")
                .isFalse();
        assertThat(prefetcher.onDurable("bins/seg-1", "az-a"))
                .as("seg-1 was the least recently signalled, and it is the one re-read")
                .isTrue();
        assertThat(store.counts().gets() - before).isEqualTo(1);
    }
}
