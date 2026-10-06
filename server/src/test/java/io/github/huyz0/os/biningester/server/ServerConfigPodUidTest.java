// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.ingest.CostTopKReporter;
import io.github.huyz0.os.biningester.ingest.IndexQuotas;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * A pod's UID is required by the record as it is by the parser (M13.27f):
 * every term's roster names its leader by its UID (ADR-0081 §1), so a pod
 * without one could take the lease and then start no term.
 */
class ServerConfigPodUidTest {

    private static ServerConfig withUid(String podUid) {
        return new ServerConfig("pod1", "az-a", "cluster-a", "bins/cluster-a",
                new StoreConfig("memory", Optional.empty()),
                Duration.ofSeconds(10), Duration.ofSeconds(3), "http://pod1:8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", Set.of("logs"),
                RetentionConfig.defaults(), Optional.empty(), podUid,
                CostTopKReporter.DEFAULT_INTERVAL, IndexQuotas.Config.none(), false, java.util.Optional.empty());
    }

    @Test
    void anEMPTYPodUidIsREFUSEDAndNamed() {
        assertThatThrownBy(() -> withUid(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("podUid");
    }

    @Test
    void aBLANKPodUidIsREFUSEDAndNamed() {
        assertThatThrownBy(() -> withUid("   "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("podUid");
    }
}
