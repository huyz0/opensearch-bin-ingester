// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastEpochFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A started node opens its epoch fence, and its fast journal where one is
 * configured, before it serves (M13.27i).
 */
class IngesterNodeFastDiskTest {

    @TempDir
    Path dir;

    private IngesterNode node;

    @AfterEach
    void stop() throws Exception {
        if (node != null) {
            node.close();
        }
    }

    private static Map<String, String> settings() {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_UID, "uid-pod1");
        settings.put(ServerProperties.PEER_TLS, "off");
        settings.put(ServerProperties.PEER_PORT, "0");
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "memory");
        settings.put(ServerProperties.ENDPOINT, "http://localhost:0");
        settings.put(ServerProperties.HTTP_PORT, "0");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        return settings;
    }

    @Test
    void aJOURNALEDNodeKeepsItsJournalAndEpochFileInTheDirectory() throws Exception {
        Map<String, String> settings = settings();
        settings.put(ServerProperties.FAST_JOURNAL_DIR, dir.toString());

        node = IngesterNode.start(ServerProperties.parse(settings), Clock.systemUTC());

        assertThat(node.fastDisk().journal()).isPresent();
        assertThat(node.fastDisk().fence().highest())
                .as("the term the node took at boot").isEqualTo(1);
        assertThat(FastEpochFile.decode(Files.readAllBytes(dir.resolve("epoch")))).isEqualTo(1);
        assertThat(dir.resolve("journal")).exists();
    }

    @Test
    void aDISKLESSNodeWritesNoFile() throws Exception {
        node = IngesterNode.start(ServerProperties.parse(settings()), Clock.systemUTC());

        assertThat(node.fastDisk().journal()).isEmpty();
        assertThat(node.fastDisk().fence().highest()).isEqualTo(1);
        try (var listed = Files.list(dir)) {
            assertThat(listed).isEmpty();
        }
    }
}
