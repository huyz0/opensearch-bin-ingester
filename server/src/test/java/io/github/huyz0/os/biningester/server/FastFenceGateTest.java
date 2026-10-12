// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastTakeoverFrame;
import io.github.huyz0.os.biningester.sequencer.FastFrameRouter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A node's FENCE pages under the pod's own holder gate (M13.33b review round
 * 2, T5): a page waits for an append holding that gate, so no copy lands
 * between two pages -- a private gate would exclude nothing.
 */
@Timeout(60)
class FastFenceGateTest {

    @Test
    void aNODEsFencePageWaitsForAnAppendHoldingThePodsGate() throws Exception {
        MemoryBinStore store = FastDepartureTest.leaseHeldBy("uid-l");
        try (FastDisk disk = FastDepartureTest.journaledDisk(store)) {
            Map<Integer, FastFrameRouter.Handler> handlers = new HashMap<>();
            FastHolderFrames.register(handlers::put, disk);
            CountDownLatch inside = new CountDownLatch(1);
            CountDownLatch open = new CountDownLatch(1);
            CompletableFuture<Object> appending = CompletableFuture.supplyAsync(() -> {
                try {
                    return disk.gate().whileAccepting(() -> {
                        inside.countDown();
                        try {
                            open.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return true;
                    });
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            assertThat(inside.await(10, TimeUnit.SECONDS)).isTrue();

            CompletableFuture<FastFrame.Body> paging = CompletableFuture.supplyAsync(() -> {
                try {
                    return handlers.get(FastTakeoverFrame.KIND_FENCE).answer(
                            new FastFrame.Header(FastTakeoverFrame.KIND_FENCE, 9, "uid-s",
                                    "uid-pod1"), new FastTakeoverFrame.Fence(List.of(7L), 6));
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });

            assertThat(paging).as("the page waits for the pod's gate")
                    .failsWithin(300, TimeUnit.MILLISECONDS);
            open.countDown();
            appending.get(10, TimeUnit.SECONDS);
            assertThat(paging.get(10, TimeUnit.SECONDS))
                    .isInstanceOf(FastTakeoverFrame.Collected.class);
        }
    }
}
