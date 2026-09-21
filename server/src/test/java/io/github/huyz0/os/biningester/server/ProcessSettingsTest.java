// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The settings the PROCESS needs, as against the ones the GRAPH needs (M8.4).
 *
 * <p>⚠️ **THREE OF THEM ARE REQUIRED AND NONE OF THE THREE HAS A DEFAULT**,
 * which is the decision this file pins:
 *
 * <ul>
 *   <li>{@code http.port} — a defaulted 0 asks the kernel for a port, so the
 *       {@code endpoint} the lease publishes names a port no peer can reach and
 *       every forwarded commit fails with a connection refused. It only appears
 *       once a SECOND pod exists, which is after the deploy.</li>
 *   <li>{@code producer.subject} — the identity every write is stamped with
 *       until authentication lands.</li>
 *   <li>{@code producer.allowed-indices} — ⚠️ and the default that must never
 *       be written is "all". {@code Principal}'s empty allow-list permits
 *       NOTHING (ADR-0021); a root that widened it would turn the one security
 *       property the write path has into a comment.</li>
 * </ul>
 */
class ProcessSettingsTest {

    private static Map<String, String> minimal() {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
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

    private static Map<String, String> without(String key) {
        Map<String, String> settings = minimal();
        settings.remove(key);
        return settings;
    }

    private static Map<String, String> with(String key, String value) {
        Map<String, String> settings = minimal();
        settings.put(key, value);
        return settings;
    }

    @Test
    void eachOfTheTHREEProcessSettingsIsREQUIREDAndTheMessageNAMESIt() {
        for (String key : new String[] {ServerProperties.HTTP_PORT,
                ServerProperties.PRODUCER_SUBJECT, ServerProperties.PRODUCER_ALLOWED_INDICES}) {
            assertThatThrownBy(() -> ServerProperties.parse(without(key)))
                    .as("%s must be refused rather than defaulted", key)
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining(key);
        }
    }

    @Test
    void aPORTOfZEROIsLEGALBecauseAKernelChosenPortIsWhatATestWants() {
        assertThat(ServerProperties.parse(with(ServerProperties.HTTP_PORT, "0")).httpPort())
                .isEqualTo(0);
    }

    @Test
    void aPORTOutsideTheRangeOrNotANumberIsREFUSED() {
        assertThatThrownBy(() -> ServerProperties.parse(with(ServerProperties.HTTP_PORT, "65536")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("65536");
        assertThatThrownBy(() -> ServerProperties.parse(with(ServerProperties.HTTP_PORT, "-1")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("-1");
        assertThatThrownBy(() -> ServerProperties.parse(with(ServerProperties.HTTP_PORT, "8080x")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("8080x");
    }

    @Test
    void theALLOWListIsSPLITOnCommasAndTRIMMED() {
        assertThat(ServerProperties.parse(
                with(ServerProperties.PRODUCER_ALLOWED_INDICES, " logs , metrics ,traces"))
                .allowedIndices())
                .containsExactlyInAnyOrder("logs", "metrics", "traces");
    }

    @Test
    void anEMPTYEntryInTheALLOWListIsREFUSEDRatherThanDropped() {
        // ⚠️ A TRAILING COMMA IS THE COMMONEST WAY TO WRITE ONE, and dropping
        // it silently means the list an operator reads and the list the node
        // enforces are different lengths.
        assertThatThrownBy(() -> ServerProperties.parse(
                with(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs,")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.PRODUCER_ALLOWED_INDICES);
        assertThatThrownBy(() -> ServerProperties.parse(
                with(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs,,metrics")))
                .isInstanceOf(ConfigurationException.class);
    }

    @Test
    void aREPEATEDIndexIsREFUSEDBecauseASetWouldSwallowIt() {
        assertThatThrownBy(() -> ServerProperties.parse(
                with(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs,metrics,logs")))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining("logs");
    }

    @Test
    void thePRINCIPALCarriesTheCONFIGUREDDomainSubjectAndAllowList() {
        // ⚠️ ONE TRUST DOMAIN. The record's own guard makes the producer's
        // domain and the operator's the same string; this is where that reaches
        // the object `BulkService` actually checks against.
        ServerConfig config = ServerProperties.parse(
                with(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs,metrics"));

        assertThat(config.principal().trustDomain()).isEqualTo("cluster-a");
        assertThat(config.principal().subject()).isEqualTo("producer-1");
        assertThat(config.principal().canWriteTo("logs")).isTrue();
        assertThat(config.principal().canWriteTo("metrics")).isTrue();
        assertThat(config.principal().canWriteTo("secrets")).isFalse();
    }

    @Test
    void theALLOWListCannotBeWIDENEDAfterTheConfigIsBuilt() {
        // ⚠️ A PRIVILEGE ESCALATION WITH NO CODE CHANGE AT THE CALL SITE, which
        // is why both the record and `Principal` copy.
        ServerConfig config = ServerProperties.parse(minimal());
        assertThatThrownBy(() -> config.allowedIndices().add("secrets"))
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(config.principal().canWriteTo("secrets")).isFalse();
    }
}
