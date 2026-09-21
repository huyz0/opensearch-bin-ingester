// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.ingest.IngestConfig;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * What one node's configuration refuses (M8.1).
 *
 * <p>⚠️ **EVERY CASE HERE IS A DEPLOYMENT MISTAKE THAT OTHERWISE FAILS LATE**,
 * and late means at the first write, in a message that names something the
 * operator did not set. The record is where an unset environment variable
 * becomes visible, because it is the one place that holds all of them at once.
 */
class ServerConfigTest {

    private static ServerConfig config(String podId, String trustDomain, String prefix,
            String endpoint, IngestConfig ingest) {
        return new ServerConfig(podId, "az-a", trustDomain, prefix,
                new StoreConfig("memory", Optional.empty()),
                Duration.ofSeconds(10), Duration.ofSeconds(3), endpoint, ingest,
                0, "producer-1", java.util.Set.of("logs"));
    }

    private static ServerConfig valid() {
        return config("pod1", "cluster-a", "bins/cluster-a", "http://pod1:8080",
                IngestConfig.defaults("cluster-a"));
    }

    @Test
    void theZONEIsPARTOfAConfigsIDENTITYAndOfWhatItPRINTS() {
        // ⚠️ TWO PODS' CONFIGURATIONS THAT DIFFER ONLY BY ZONE ARE DIFFERENT
        // CONFIGURATIONS (M9.2). A record whose identity ignored the zone
        // would make a fleet spread over three of them compare as one, and
        // the zone is the thing every peer transport compares against.
        ServerConfig here = valid();
        ServerConfig there = new ServerConfig("pod1", "az-b", "cluster-a", "bins/cluster-a",
                new StoreConfig("memory", Optional.empty()),
                Duration.ofSeconds(10), Duration.ofSeconds(3), "http://pod1:8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", java.util.Set.of("logs"));

        assertThat(here).isEqualTo(valid()).hasSameHashCodeAs(valid());
        assertThat(here).isNotEqualTo(there);
        assertThat(here.hashCode()).isNotEqualTo(there.hashCode());
        assertThat(here.toString())
                .as("⚠️ AN OPERATOR READING A CONFIG IN A LOG SEES THE ZONE, or cannot "
                        + "tell which pod's numbers they are looking at")
                .contains("az-a")
                .contains("pod1");
    }

    @Test
    void aVALIDConfigurationIsACCEPTED() {
        assertThatCode(ServerConfigTest::valid).doesNotThrowAnyException();
        assertThat(valid().podId()).isEqualTo("pod1");
    }

    @Test
    void aBLANKPodIdIsREFUSED() {
        assertThatThrownBy(() -> config("  ", "cluster-a", "bins/c", "http://p:1",
                IngestConfig.defaults("cluster-a")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("podId");
    }

    @Test
    void aBLANKPrefixIsREFUSED() {
        // ⚠️ AN UNSET PREFIX PUTS THE LEASE AT THE BUCKET ROOT, which
        // `LeaseConfig`'s own javadoc records as two clusters contending for one
        // lease object and each fencing the other out of a chain it has no
        // relationship with.
        assertThatThrownBy(() -> config("pod1", "cluster-a", "", "http://p:1",
                IngestConfig.defaults("cluster-a")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("prefix");
    }

    @Test
    void aBLANKEndpointIsREFUSED() {
        // ⚠️ A LEADER MUST BE REACHABLE AT ITS OWN ENDPOINT or the pods
        // forwarding to it have nowhere to send -- and a blank one is written
        // into the lease, where every peer reads it.
        assertThatThrownBy(() -> config("pod1", "cluster-a", "bins/c", " ",
                IngestConfig.defaults("cluster-a")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("endpoint");
    }

    @Test
    void aTRUSTDomainThatDISAGREESWithTheIngestConfigIsREFUSED() {
        // ⚠️ THE SHARPEST CASE IN THIS FILE. Both values are individually
        // valid, the node starts, and it then refuses EVERY producer it was
        // configured to accept -- with a message naming the domain the operator
        // did not set.
        assertThatThrownBy(() -> config("pod1", "cluster-a", "bins/c", "http://p:1",
                IngestConfig.defaults("cluster-b")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cluster-a")
                .hasMessageContaining("cluster-b");
    }

    @Test
    void aNULLInAnyFieldIsREFUSEDAndTheMessageNamesTheFIELD() {
        assertThatThrownBy(() -> config(null, "cluster-a", "bins/c", "http://p:1",
                IngestConfig.defaults("cluster-a")))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("podId");
        assertThatThrownBy(() -> config("pod1", null, "bins/c", "http://p:1",
                IngestConfig.defaults("cluster-a")))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("trustDomain");
        assertThatThrownBy(() -> config("pod1", "cluster-a", null, "http://p:1",
                IngestConfig.defaults("cluster-a")))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("prefix");
        assertThatThrownBy(() -> config("pod1", "cluster-a", "bins/c", null,
                IngestConfig.defaults("cluster-a")))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("endpoint");
        assertThatThrownBy(() -> config("pod1", "cluster-a", "bins/c", "http://p:1", null))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("ingest");
        assertThatThrownBy(() -> new ServerConfig("pod1", "az-a", "cluster-a", "bins/c", null,
                Duration.ofSeconds(10), Duration.ofSeconds(3), "http://p:1",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", java.util.Set.of("logs")))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("store");
        assertThatThrownBy(() -> new ServerConfig("pod1", "az-a", "cluster-a", "bins/c",
                new StoreConfig("memory", Optional.empty()), null, Duration.ofSeconds(3),
                "http://p:1", IngestConfig.defaults("cluster-a"), 0, "producer-1",
                java.util.Set.of("logs")))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("leaseTtl");
        assertThatThrownBy(() -> new ServerConfig("pod1", "az-a", "cluster-a", "bins/c",
                new StoreConfig("memory", Optional.empty()), Duration.ofSeconds(10), null,
                "http://p:1", IngestConfig.defaults("cluster-a"), 0, "producer-1",
                java.util.Set.of("logs")))
                .isInstanceOf(NullPointerException.class).hasMessageContaining("leaseRenewInterval");
    }
}
