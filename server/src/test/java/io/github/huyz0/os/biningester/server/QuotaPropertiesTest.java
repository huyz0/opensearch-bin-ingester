// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.ingest.IndexQuotas;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The quota settings (M11.8, ADR-0078 decision 4): none by default, a default
 * and per-index overrides by name, the in-flight cap, and a bad value refused
 * naming its key.
 */
class QuotaPropertiesTest {

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
    void nothingConfiguredIsNoQuotaAtAll() {
        assertThat(ServerProperties.parse(minimal()).quotas())
                .isEqualTo(IndexQuotas.Config.none());
    }

    @Test
    void aDefaultAPerIndexOverrideWithADottedNameAndTheCapAreRead() {
        Map<String, String> set = minimal();
        set.put(QuotaProperties.DEFAULT_BYTES, "1000");
        set.put(QuotaProperties.DEFAULT_RECORDS, "10");
        set.put("ingest.quota.index.logs.2026.records-per-second", " 3 ");
        set.put(QuotaProperties.MAX_IN_FLIGHT, "4");

        IndexQuotas.Config quotas = ServerProperties.parse(set).quotas();

        assertThat(quotas.defaults()).isEqualTo(new IndexQuotas.Limit(1000, 10));
        assertThat(quotas.perIndex()).as("⚠️ THE NAME KEEPS ITS DOTS; the unset rate inherits")
                .containsExactly(Map.entry("logs.2026", new IndexQuotas.Limit(1000, 3)));
        assertThat(quotas.maxInFlightPerIndex()).isEqualTo(4);
    }

    @Test
    void aBadValueOrAnUnknownQuotaKeyIsAConfigurationErrorNamingTheKey() {
        for (String[] bad : new String[][] {
                {QuotaProperties.DEFAULT_BYTES, "-1"},
                {QuotaProperties.DEFAULT_RECORDS, "lots"},
                {"ingest.quota.index.logs.bytes-per-second", "1.5"},
                {QuotaProperties.MAX_IN_FLIGHT, "0"},
                {"ingest.quota.index.logs.bits-per-second", "1"}}) {
            Map<String, String> set = minimal();
            set.put(bad[0], bad[1]);

            assertThatThrownBy(() -> ServerProperties.parse(set)).as("%s=%s", bad[0], bad[1])
                    .isInstanceOf(ConfigurationException.class)
                    .hasMessageContaining(bad[0]);
        }
    }
}
