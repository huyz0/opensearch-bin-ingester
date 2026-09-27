// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.PutPurposeCounts;
import io.github.huyz0.os.biningester.binstore.StoreCounts;
import io.github.huyz0.os.biningester.binstore.Version;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class MacroCountsJsonTest {

    @Test
    void concurrentPutSnapshotAlwaysReportsAnAggregateEqualToItsPurposeBreakdown() throws Exception {
        BinStore delegate = (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "put" -> new Version("v1");
                    case "close" -> null;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        CountingBinStore store = new CountingBinStore(delegate);
        Body body = new Body(0, InputStream::nullInputStream);
        int writers = 4;
        CountDownLatch ready = new CountDownLatch(writers);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(writers);
        try (var executor = Executors.newFixedThreadPool(writers)) {
            java.util.List<Future<?>> results = new java.util.ArrayList<>();
            for (int writer = 0; writer < writers; writer++) {
                int writerId = writer;
                results.add(executor.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        for (int i = 0; i < 25_000; i++) {
                            store.put("bins/cluster-a/data/segment-" + writerId + ".bseg", body);
                        }
                    } finally {
                        finished.countDown();
                    }
                    return null;
                }));
            }
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            start.countDown();
            do {
                assertPurposeSnapshotReconciles(store);
            } while (finished.getCount() > 0);
            assertThat(finished.await(5, TimeUnit.SECONDS)).isTrue();
            for (Future<?> result : results) {
                result.get();
            }
            assertThat(store.counts().puts()).isEqualTo(writers * 25_000L);
            assertThat(store.putPurposeCounts().total()).isEqualTo(store.counts().puts());
            assertPurposeSnapshotReconciles(store);
        }
    }

    private static void assertPurposeSnapshotReconciles(CountingBinStore store) {
        PutPurposeCounts purposes = store.putPurposeCounts();
        String json = FrontDoor.macroCountsJson("pod", store.counts(), purposes,
                CrossAzBytes.untracked());
        assertThat(json).contains("\"puts\":" + purposes.total());
    }

    @Test
    void macroCountSnapshotCarriesEveryCounter() {
        String podId = "pod\n\"a\\\t" + (char) 0;
        assertThat(FrontDoor.macroCountsJson(podId, new StoreCounts(3, 5, 7, 11, 13)))
                .isEqualTo("{\"podId\":\"pod\\n\\\"a\\\\\\t\\u0000\",\"puts\":3,\"gets\":5,\"lists\":7,\"stats\":11,\"deletes\":13}\n");
        assertThat(FrontDoor.macroCountsJson("pod", new StoreCounts(3, 5, 7, 11, 13),
                CrossAzBytes.untracked()))
                .isEqualTo("{\"podId\":\"pod\",\"puts\":3,\"gets\":5,\"lists\":7,\"stats\":11,\"deletes\":13}\n");
        PutPurposeCounts purposes = new PutPurposeCounts(17, 19, 5, 23, 24);
        assertThat(FrontDoor.macroCountsJson("pod", new StoreCounts(89, 5, 7, 11, 13),
                purposes, CrossAzBytes.untracked()))
                .isEqualTo("{\"podId\":\"pod\",\"puts\":88,\"gets\":5,\"lists\":7,\"stats\":11,\"deletes\":13,\"dataPuts\":17,\"commitPuts\":19,\"checkpointPuts\":5,\"leasePuts\":23,\"otherPuts\":24}\n");

        CrossAzBytes crossAz = new CrossAzBytes("az-a");
        crossAz.sent(CrossAzBytes.Transport.PROXY_READ, "az-b", 2);
        crossAz.sent(CrossAzBytes.Transport.INLINE_PUSH, "az-b", 3);
        crossAz.sent(CrossAzBytes.Transport.CONSUMER_POLL, "az-b", 5);
        crossAz.sent(CrossAzBytes.Transport.COMMIT_FORWARD, "az-b", 7);
        crossAz.sent(CrossAzBytes.Transport.INBOX_DRAIN, "az-b", 11);
        crossAz.sent(CrossAzBytes.Transport.DURABLE_SEGMENT_SIGNAL, "az-b", 13);
        assertThat(FrontDoor.macroCountsJson("pod", new StoreCounts(0, 0, 0, 0, 0), crossAz))
                .contains("\"crossAzBytes\":41")
                .contains("\"proxyRead\":2")
                .contains("\"inlinePush\":3")
                .contains("\"consumerPoll\":5")
                .contains("\"commitForward\":7")
                .contains("\"inboxDrain\":11")
                .contains("\"durableSegmentSignal\":13");
        assertThat(FrontDoor.macroCountsJson("pod", new StoreCounts(89, 0, 0, 0, 0),
                purposes, crossAz))
                .contains("\"puts\":88")
                .contains("\"dataPuts\":17")
                .contains("\"checkpointPuts\":5")
                .contains("\"leasePuts\":23")
                .contains("\"crossAzBytes\":41");
    }

    /**
     * M10.4: NFR-5's full proof reads the SAME-AZ proxy bytes, so the snapshot
     * carries them -- apart from the cross-AZ ones, which a same-AZ byte must
     * never inflate.
     */
    @Test
    void macroCountSnapshotCarriesSameAzProxyReadApartFromCrossAz() {
        CrossAzBytes crossAz = new CrossAzBytes("az-b");
        crossAz.sent(CrossAzBytes.Transport.PROXY_READ, "az-b", 17);
        crossAz.sent(CrossAzBytes.Transport.PROXY_READ, "az-a", 5);
        crossAz.sent(CrossAzBytes.Transport.INLINE_PUSH, "az-b", 3);
        assertThat(FrontDoor.macroCountsJson("pod", new StoreCounts(0, 0, 0, 0, 0), crossAz))
                .contains("\"sameAzProxyRead\":17")
                .contains("\"proxyRead\":5")
                .contains("\"crossAzBytes\":5");
    }
}
