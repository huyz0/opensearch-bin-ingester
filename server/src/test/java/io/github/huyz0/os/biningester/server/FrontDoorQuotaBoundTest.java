// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.ingest.IndexQuotas;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.ingest.CostTopKReporter;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * M12.4 on the assembled front door (review T1): the quotas it builds know
 * exactly the catalog's indices, so a name a producer invents leaves no bucket
 * behind, whatever the default quota.
 */
class FrontDoorQuotaBoundTest {

    @Test
    void theFrontDoorsQuotasKnowTheCatalogsIndicesAndNoOthers() throws Exception {
        ServerConfig config = new ServerConfig("writera", "az-a", "cluster-a", "bins/cluster-a",
                new StoreConfig("memory", Optional.empty()), Duration.ofSeconds(10),
                Duration.ofSeconds(3), "http://writer-a:8080", IngestConfig.defaults("cluster-a"),
                0, "producer", Set.of("logs"),
                new RetentionConfig(Duration.ofMinutes(1), Duration.ofHours(2),
                        Duration.ofSeconds(10), Duration.ofHours(3), Duration.ofDays(1)),
                Optional.empty(), "uid-writera", CostTopKReporter.DEFAULT_INTERVAL,
                new IndexQuotas.Config(new IndexQuotas.Limit(0, 1_000), Map.of(), 8), false, java.util.Optional.empty(), PeerConfig.off(0));
        SequencerTransport noPeers = new SequencerTransport() {
            @Override
            public io.github.huyz0.os.biningester.format.CommitDelta send(
                    String endpoint, CommitRequest request) {
                throw new AssertionError("unexpected peer commit");
            }

            @Override
            public void close() {
            }
        };
        try (var store = new MemoryBinStore();
                var assembly = Assembly.open(config, store, noPeers, Clock.systemUTC());
                var door = FrontDoor.start(assembly, Clock.systemUTC())) {
            assembly.catalog().register(new IndexRegistration("AAAAAAAAQACAAAAAAAAAqg", "logs",
                    List.of(), 4, 4, 1, 1));

            for (int i = 0; i < 100; i++) {
                door.quotas().admit("invented-" + i).ticket().orElseThrow().release();
            }
            assertThat(door.quotas().bucketCount()).as("⚠️ NO BUCKET PER INVENTED NAME")
                    .isZero();

            door.quotas().admit("logs").ticket().orElseThrow().release();
            assertThat(door.quotas().bucketCount()).as("the registered index gets one")
                    .isEqualTo(1);
        }
    }
}
