// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger.Charge;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.security.Principal;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The assembled pod's OWN ledger receives the write side's charges (M11.2,
 * M11.22, ADR-0077 decision 3, M11 criterion 4): every data PUT and every
 * commit-log PUT the node counts -- the CONTINUE that opened its chain
 * included -- is charged to it. The component tests build their stores with a
 * ledger of their own; this is the wiring.
 */
class AssemblyWriteAttributionTest {

    private static final String PREFIX = "bins/cluster-a";
    private static final String INDEX = "logs";
    private static final String INDEX_UUID = base64Url(UUID.randomUUID());
    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of(INDEX));

    private static String base64Url(UUID uuid) {
        ByteBuffer b = ByteBuffer.allocate(16);
        b.putLong(uuid.getMostSignificantBits());
        b.putLong(uuid.getLeastSignificantBits());
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b.array());
    }

    private static ServerConfig config() {
        return new ServerConfig("pod1", "az-a", "cluster-a", PREFIX,
                new StoreConfig("memory", Optional.empty()),
                Duration.ofDays(1), Duration.ofSeconds(3), "http://pod1:8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", Set.of(INDEX),
                new RetentionConfig(Duration.ofMinutes(1), Duration.ofHours(2),
                        Duration.ofSeconds(10), Duration.ofHours(3), Duration.ofDays(1)), java.util.Optional.empty(), "uid-pod1", io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL, io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false);
    }

    private static SequencerTransport noPeers() {
        return new SequencerTransport() {
            @Override
            public CommitDelta send(String endpoint, CommitRequest request) {
                throw new UnsupportedOperationException("no peer expected: " + endpoint);
            }

            @Override
            public void close() {
            }
        };
    }

    @Test
    void theNodesDataAndCommitPutsAreChargedToItsOwnLedger() throws Exception {
        try (BinStore shared = StoreFactory.open(new StoreConfig("memory", Optional.empty()));
                Assembly assembly = Assembly.open(config(), shared, noPeers(),
                        Clock.systemUTC())) {
            assembly.catalog().register(
                    new IndexRegistration(INDEX_UUID, INDEX, List.of(), 4, 4, 1, 1));
            for (int i = 0; i < 2; i++) {
                String id = "doc-" + i;
                assembly.ingest().append(PRINCIPAL, INDEX, 0, sink -> sink.accept(
                        new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                                id.getBytes(StandardCharsets.UTF_8))));
            }

            IndexCostLedger.Snapshot snapshot = assembly.costLedger().snapshot();
            long commitPuts = assembly.putPurposeCounts().commitPuts();
            assertThat(commitPuts).as("the premise: the chain was opened and appended to")
                    .isGreaterThanOrEqualTo(3);
            assertThat(snapshot.totalMicros(Charge.COMMIT_PUT))
                    .as("⚠️ EVERY COMMIT PUT THE NODE COUNTED IS CHARGED TO ITS LEDGER")
                    .isEqualTo(commitPuts * IndexCostLedger.MICROS_PER_REQUEST);
            assertThat(snapshot.indices()).singleElement().satisfies(logs ->
                    assertThat(logs.micros().get(Charge.COMMIT_PUT))
                            .as("the appends' deltas, one request each, all this index's")
                            .isEqualTo(2 * IndexCostLedger.MICROS_PER_REQUEST));
            assertThat(snapshot.totalMicros(Charge.DATA_PUT))
                    .as("and the same ledger holds the node's data PUTs")
                    .isEqualTo(assembly.putPurposeCounts().dataPuts()
                            * IndexCostLedger.MICROS_PER_REQUEST)
                    .isPositive();
        }
    }
}
