// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A pod id the writer cannot use is refused AT PARSE, naming the key (M8.47,
 * FR-13).
 *
 * <p>⚠️ **IT WAS REFUSED FROM INSIDE {@code Assembly}**, where the segment
 * publisher rejects a short id containing {@code -} or {@code /}: an uncaught
 * {@code IllegalArgumentException} out of {@code main}, the operator-facing
 * shape M8.4's exit codes exist to prevent. A {@link ConfigurationException}
 * is what {@code Main} turns into one line and {@code EXIT_CONFIG}.
 */
class PodIdSettingTest {

    private static Map<String, String> settings(String podId) {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, podId);
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
    void aDASHInThePodIdIsREFUSEDNamingTheKey() {
        assertThatThrownBy(() -> ServerProperties.parse(settings("ingester-0")))
                .as("⚠️ A STATEFULSET's OWN NAMING, and the likeliest id there is")
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.POD_ID)
                .hasMessageContaining("ingester-0");
    }

    @Test
    void aSLASHInThePodIdIsREFUSEDToo() {
        assertThatThrownBy(() -> ServerProperties.parse(settings("a/b")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.POD_ID);
    }

    @Test
    void anIdTheWriterCanUseIsACCEPTED() {
        assertThat(ServerProperties.parse(settings("pod1")).podId()).isEqualTo("pod1");
    }
}
