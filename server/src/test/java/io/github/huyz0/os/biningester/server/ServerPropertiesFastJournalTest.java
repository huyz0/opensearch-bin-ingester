// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * The fast journal's directory and cap (ADR-0082 §4; M13.27i): absent, the pod
 * is diskless (the glossary's {@code wal=false} ingester); present, its journal
 * and epoch file live there under a cap of a holder's 512 MiB by default
 * (ADR-0081 §5 step 7, §7: twice the leader's 256 MiB).
 */
class ServerPropertiesFastJournalTest {

    private static Map<String, String> minimal() {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_UID, "uid-pod1");
        settings.put(ServerProperties.PEER_TLS, "off");
        settings.put(ServerProperties.PEER_PORT, "0");
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
    void noDIRECTORYMeansADisklessPod() {
        assertThat(ServerProperties.parse(minimal()).fastJournal()).isEmpty();
    }

    @Test
    void aDIRECTORYIsKeptAtTheHoldersCap() {
        Map<String, String> settings = minimal();
        settings.put(ServerProperties.FAST_JOURNAL_DIR, "/var/fast");

        FastJournalConfig journal = ServerProperties.parse(settings).fastJournal().orElseThrow();

        assertThat(journal.directory()).isEqualTo("/var/fast");
        assertThat(journal.capBytes()).isEqualTo(512L << 20);
    }

    @Test
    void aCAPIsReadInBytes() {
        Map<String, String> settings = minimal();
        settings.put(ServerProperties.FAST_JOURNAL_DIR, "/var/fast");
        settings.put(ServerProperties.FAST_JOURNAL_CAP, "1048576");

        assertThat(ServerProperties.parse(settings).fastJournal().orElseThrow().capBytes())
                .isEqualTo(1L << 20);
    }

    @Test
    void aCAPWithoutADirectoryIsRefusedNamingIt() {
        Map<String, String> settings = minimal();
        settings.put(ServerProperties.FAST_JOURNAL_CAP, "1048576");

        assertThatThrownBy(() -> ServerProperties.parse(settings))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.FAST_JOURNAL_CAP);
    }

    @Test
    void aBLANKDirectoryIsRefusedNamingIt() {
        Map<String, String> settings = minimal();
        settings.put(ServerProperties.FAST_JOURNAL_DIR, "  ");

        assertThatThrownBy(() -> ServerProperties.parse(settings))
                .isInstanceOf(ConfigurationException.class)
                .hasMessageContaining(ServerProperties.FAST_JOURNAL_DIR);
    }
}
