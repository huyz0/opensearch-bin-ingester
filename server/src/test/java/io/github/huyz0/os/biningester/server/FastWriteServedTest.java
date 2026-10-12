// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.ingest.AppendResult;
import io.github.huyz0.os.biningester.security.Principal;
import java.io.InputStream;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The fast write on a node, end to end (M13.84; ADR-0081 §2, test-plan row
 * 19): a {@code wal=true} index's append goes through the term the node
 * leads -- its {@code wal_quorum} recorded first by the catalog's tick --
 * journaled, EXPOSED back to the writer and answered at its offsets; a
 * {@code wal=false} index's append never reaches the fast journal.
 */
class FastWriteServedTest {

    private static final String FAST_UUID = "nVzgup36TLqWp7VBBREj1w";
    private static final String DEFAULT_UUID = "Qk1rUnfdRyCrk1L0DLaXEg";
    private static final Principal PRODUCER =
            new Principal("cluster-a", "producer-1", Set.of("orders", "logs"));

    @TempDir
    Path dir;

    private IngesterNode node;

    @AfterEach
    void stop() throws Exception {
        if (node != null) {
            node.close();
        }
    }

    /** ⚠️ The endpoint names the peer port before the node binds it: retried on a taken one. */
    private IngesterNode startOnAFreePort() throws Exception {
        RuntimeException last = null;
        for (int attempt = 0; attempt < 5; attempt++) {
            int port;
            int peerPort;
            try (ServerSocket probe = new ServerSocket(0);
                    ServerSocket peerProbe = new ServerSocket(0)) {
                port = probe.getLocalPort();
                peerPort = peerProbe.getLocalPort();
            }
            try {
                return IngesterNode.start(ServerProperties.parse(settings(port, peerPort)),
                        Clock.systemUTC());
            } catch (RuntimeException taken) {
                last = taken;
            }
        }
        throw last;
    }

    private Map<String, String> settings(int port, int peerPort) {
        Map<String, String> settings = new HashMap<>();
        settings.put(ServerProperties.POD_ID, "pod1");
        settings.put(ServerProperties.POD_UID, "uid-pod1");
        settings.put(ServerProperties.PEER_TLS, "off");
        settings.put(ServerProperties.PEER_PORT, Integer.toString(peerPort));
        settings.put(ServerProperties.POD_AZ, "az-a");
        settings.put(ServerProperties.TRUST_DOMAIN, "cluster-a");
        settings.put(ServerProperties.PREFIX, "bins/cluster-a");
        settings.put(ServerProperties.STORE_KIND, "local-fs");
        settings.put(ServerProperties.STORE_ROOT, dir.resolve("store").toString());
        settings.put(ServerProperties.ENDPOINT, "http://localhost:" + peerPort);
        settings.put(ServerProperties.HTTP_PORT, Integer.toString(port));
        settings.put(ServerProperties.PRODUCER_SUBJECT, "producer-1");
        settings.put(ServerProperties.PRODUCER_ALLOWED_INDICES, "orders,logs");
        settings.put(ServerProperties.FAST_JOURNAL_DIR, dir.resolve("journal").toString());
        return settings;
    }

    private static List<SegmentRecord> docs(int n) {
        List<SegmentRecord> docs = new java.util.ArrayList<>();
        for (int i = 0; i < n; i++) {
            docs.add(new SegmentRecord("doc-" + i, OpType.INDEX, OptionalLong.empty(),
                    new byte[] {(byte) i}));
        }
        return docs;
    }

    /** Whether the term the node leads records {@code index} in its term record. */
    private boolean recorded(java.util.UUID index) throws Exception {
        var store = node.assembly().store();
        long latest;
        try (InputStream in = store.get(Roster.latestKey("bins/cluster-a"))) {
            latest = Roster.decodeLatest(in.readAllBytes());
        }
        try (InputStream in = store.get(Roster.key("bins/cluster-a", latest))) {
            return Roster.decode(in.readAllBytes()).termRecord().stream()
                    .anyMatch(t -> t.walQuorum().containsKey(index));
        }
    }

    @Test
    void aFASTIndexsAppendIsAssignedThroughTheTermTheNodeLeads() throws Exception {
        node = startOnAFreePort();
        node.assembly().catalog().register(new IndexRegistration(FAST_UUID, "orders",
                List.of(), 1, 1, 1, 1).withFastSettings(250, true, 1));
        java.util.UUID index = RunKey.ofIndexUuid(FAST_UUID, 0).indexId();
        await().atMost(Duration.ofSeconds(20)).until(() -> recorded(index));

        AppendResult got = node.assembly().ingest().append(PRODUCER, "orders", 0,
                docs(3)::forEach);

        assertThat(got).isEqualTo(new AppendResult(3, 0, 2));
        assertThat(node.fastDisk().journal().orElseThrow().held())
                .as("the leader holds every entry of its term")
                .singleElement().satisfies(e -> {
                    assertThat(e.key()).isEqualTo(new RunKey(index, 0));
                    assertThat(e.records()).hasSize(3);
                });
    }

    @Test
    void aFASTWriteIsUploadedToTheChainAtItsOffsetsAndReleased() throws Exception {
        // ⚠️ M13.31a: one segment and one delta at the offsets assigned.
        node = startOnAFreePort();
        node.assembly().catalog().register(new IndexRegistration(FAST_UUID, "orders",
                List.of(), 1, 1, 1, 1).withFastSettings(250, true, 1));
        java.util.UUID index = RunKey.ofIndexUuid(FAST_UUID, 0).indexId();
        await().atMost(Duration.ofSeconds(20)).until(() -> recorded(index));
        node.assembly().ingest().append(PRODUCER, "orders", 0, docs(3)::forEach);
        var chain = io.github.huyz0.os.biningester.sequencer.LocalSequencer
                .underneath(node.assembly().heldTerm()).orElseThrow();

        await().atMost(Duration.ofSeconds(20)).until(
                () -> chain.committedNext(new RunKey(index, 0)) == 3);

        assertThat(node.fastDisk().journal().orElseThrow().held())
                .as("the leader's copy released once the chain holds it").isEmpty();
    }

    @Test
    void aDEFAULTIndexsAppendNeverReachesTheFastJournal() throws Exception {
        node = startOnAFreePort();
        node.assembly().catalog().register(new IndexRegistration(DEFAULT_UUID, "logs",
                List.of(), 1, 1, 1, 1));

        AppendResult got = node.assembly().ingest().append(PRODUCER, "logs", 0,
                docs(2)::forEach);

        assertThat(got.recordCount()).isEqualTo(2);
        assertThat(node.fastDisk().journal().orElseThrow().held()).isEmpty();
    }
}
