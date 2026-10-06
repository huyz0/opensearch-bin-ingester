// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A node whose fast disk cannot start gives its boot term back, and a stopped
 * node closes its journal (M13.27i review round 1, T1, T3).
 */
class IngesterNodeFastDiskFailureTest {

    @TempDir
    Path dir;

    private IngesterNode node;

    @AfterEach
    void stop() throws Exception {
        if (node != null) {
            node.close();
        }
    }

    /** A pod over a local-fs store at {@code dir/store}, shared by every pod here. */
    private Map<String, String> settings(String pod) {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, pod);
        settings.put(ServerProperties.POD_UID, "uid-" + pod);
        settings.put(ServerProperties.PEER_TLS, "off");
        settings.put(ServerProperties.PEER_PORT, "0");
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "local-fs");
        settings.put(ServerProperties.STORE_ROOT, dir.resolve("store").toString());
        settings.put(ServerProperties.ENDPOINT, "http://localhost:0");
        settings.put(ServerProperties.HTTP_PORT, "0");
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "logs");
        return settings;
    }

    @Test
    void anUNREADABLEEpochFileFailsTheStartAndGivesTheBootTermBack() throws Exception {
        Path broken = dir.resolve("pod1-fast");
        Files.createDirectories(broken);
        Files.write(broken.resolve("epoch"), new byte[] {1, 2, 3});
        Map<String, String> pod1 = settings("pod1");
        pod1.put(ServerProperties.FAST_JOURNAL_DIR, broken.toString());

        assertThatThrownBy(() -> IngesterNode.start(ServerProperties.parse(pod1),
                Clock.systemUTC())).isInstanceOf(IOException.class);

        node = IngesterNode.start(ServerProperties.parse(settings("pod2")), Clock.systemUTC());
        assertThat(node.fastDisk().fence().highest())
                .as("pod2 took the next term at once: pod1 released its boot term")
                .isEqualTo(2);
    }

    @Test
    void aSTOPPEDNodeClosesItsJournal() throws Exception {
        Map<String, String> settings = settings("pod1");
        settings.put(ServerProperties.FAST_JOURNAL_DIR, dir.resolve("fast").toString());
        node = IngesterNode.start(ServerProperties.parse(settings), Clock.systemUTC());
        var journal = node.fastDisk().journal().orElseThrow();

        node.close();
        node = null;

        assertThatThrownBy(() -> journal.append(new FastJournalRecord.Entry(1,
                new RunKey(new UUID(1, 1), 0), 0, 2, 0,
                new FastJournalRecord.IdempotencyKey("pod1", new UUID(2, 2), 1),
                List.of(new SegmentRecord("d0", OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1})))))
                .as("its file was closed at shutdown").isInstanceOf(IOException.class);
    }
}
