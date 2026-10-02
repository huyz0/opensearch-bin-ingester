// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Every pod carries its availability zone, and a pod that was not told one is
 * refused (M9.2, NFR-5, M9 criterion 6).
 *
 * <p>⚠️ **THE SILENT FAILURE THIS REFUSAL EXISTS TO PREVENT IS A MEASUREMENT,
 * NOT AN OUTAGE.** Nothing in a running node needs {@code pod.az} to serve a
 * write, so a default would cost nothing visible — and every pod in the fleet
 * would carry the SAME default, which makes every peer same-AZ and NFR-5's
 * cross-AZ total exactly zero on a fleet spread over three zones. A zero
 * nobody can tell from a correct one is the worst answer a cost report has.
 */
class PodAzSettingTest {

    private static Map<String, String> minimal() {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_UID, "uid-pod1");
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "memory");
        settings.put(ServerProperties.ENDPOINT, "http://pod1:8080");
        settings.put(ServerProperties.HTTP_PORT, "8080");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        return settings;
    }

    @Test
    void theZoneIsCARRIEDThroughToTheConfig() {
        assertThat(ServerProperties.parse(minimal()).az()).isEqualTo("az-a");
    }

    @Test
    void aMISSINGZoneIsREFUSEDAndTheMessageNamesTheKey() {
        Map<String, String> settings = minimal();
        settings.remove(ServerProperties.POD_AZ);
        assertThatThrownBy(() -> ServerProperties.parse(settings))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.POD_AZ);
    }

    @Test
    void aBLANKZoneIsREFUSEDLikeEveryOtherRequiredSetting() {
        // ⚠️ `POD_AZ=${ZONE}` WITH `ZONE` UNSET arrives as an empty string, and
        // a parser that only checks for absence takes it -- M8.69's shape, in a
        // setting whose blank value is indistinguishable from a working one
        // until someone reads the cost report.
        Map<String, String> settings = minimal();
        settings.put(ServerProperties.POD_AZ, "   ");
        assertThatThrownBy(() -> ServerProperties.parse(settings))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.POD_AZ)
                .hasMessageContaining("blank");
    }

    @Test
    void theZoneIsTRIMMEDLikeEveryOtherRequiredSetting() {
        Map<String, String> settings = minimal();
        settings.put(ServerProperties.POD_AZ, " az-c ");
        assertThat(ServerProperties.parse(settings).az()).isEqualTo("az-c");
    }

    @Test
    void theKeyIsKNOWNSoAManifestCarryingItIsNotREFUSEDAsATypo() {
        assertThat(ServerProperties.knownKeys()).contains(ServerProperties.POD_AZ);
    }

    @Test
    void aBLANKZoneIsREFUSEDByTheRecordItselfToo() {
        // ⚠️ AT THE RECORD, NOT ONLY AT THE PARSER: a composition root that
        // built a config in code rather than from settings would otherwise
        // start a node whose every peer is cross-AZ.
        assertThatThrownBy(() -> ServerConfigs.withAz("  "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("az");
        assertThatThrownBy(() -> ServerConfigs.withAz(null))
                .isInstanceOf(NullPointerException.class);
    }

    /** ⚠️ One construction site, so the case above pins the RECORD's guard. */
    private static final class ServerConfigs {
        static ServerConfig withAz(String az) {
            return new ServerConfig("pod1", az, "cluster-a", "bins/cluster-a",
                    new StoreConfig("memory", java.util.Optional.empty()),
                    java.time.Duration.ofSeconds(10), java.time.Duration.ofSeconds(3),
                    "http://pod1:8080", io.github.huyz0.os.biningester.ingest.IngestConfig.defaults("cluster-a"),
                    8080, "producer-1", java.util.Set.of("logs"), RetentionConfig.defaults(), java.util.Optional.empty(), "uid-pod1", io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL, io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false);
        }
    }
}
