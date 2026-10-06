// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/**
 * The node's watch pauses exactly the configured renew interval between looks
 * -- the ADR's rate, one lease GET per interval (ADR-0081 §1, amended by
 * M13.27n; M13.27r review round 1, T1, P3).
 */
class FastPeerPaceTest {

    @Test
    void theWATCHPausesTheConfiguredRenewIntervalBetweenLooks() throws Exception {
        ServerConfig config = new ServerConfig("pod1", "az-a", "cluster-a", "bins/cluster-a",
                new StoreConfig("memory", Optional.empty()),
                Duration.ofSeconds(10), Duration.ofSeconds(3), "http://pod1:8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", java.util.Set.of("logs"),
                RetentionConfig.defaults(), Optional.empty(), "uid-pod1",
                io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL,
                io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false,
                Optional.empty());
        MemoryBinStore store = new MemoryBinStore();
        String leaseKey = SequencerAssembly.leaseConfig(config).leaseKey();
        store.put(leaseKey, Body.ofBytes(new Lease(1, "pod1", "uid-pod1", "http://pod1:8080",
                1_000L).encode()));
        List<Duration> paused = new CopyOnWriteArrayList<>();

        try (FastDisk disk = FastDisk.open(store, leaseKey, Optional.empty(),
                new FastDiskTest.Files())) {
            FastPeer peer = FastPeer.start(config, store, disk, CrossAzBytes.untracked(),
                    Duration.ofSeconds(1), () -> false, interval -> {
                        paused.add(interval);
                        throw new InterruptedException("one look is enough");
                    });
            await().atMost(Duration.ofSeconds(10)).until(() -> !peer.watching());

            assertThat(peer.leaseReads()).isEqualTo(1);
        }
        assertThat(paused).as("the renew interval, whole").containsExactly(Duration.ofSeconds(3));
    }
}
