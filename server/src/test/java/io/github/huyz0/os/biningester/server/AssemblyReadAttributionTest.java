// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger.Charge;
import io.github.huyz0.os.biningester.binstore.ObjectStat;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.ingest.SegmentSink;
import io.github.huyz0.os.biningester.security.Principal;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The assembled pod's OWN ledger receives the read side's charges (M11.3,
 * ADR-0077 decisions 3 and 4a, M11 criterion 5): a GET through the node's
 * proxy and a catch-up read, over one workload, sum to the node's counted
 * data-segment GETs. The component tests build each reader with a ledger of
 * their own; this is the wiring.
 */
class AssemblyReadAttributionTest {

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

    private static void assertExact(Assembly assembly) {
        assertThat(assembly.costLedger().snapshot().totalMicros(Charge.DATA_GET))
                .as("⚠️ THE POD's LEDGER: Σ index GET shares + unattributed = the node's "
                        + "counted data-segment GETs")
                .isEqualTo(assembly.dataSegmentGets() * IndexCostLedger.MICROS_PER_REQUEST);
    }

    @Test
    void aProxyGetAndACatchUpReadAreChargedToThePodsOwnLedger() throws Exception {
        try (BinStore shared = StoreFactory.open(new StoreConfig("memory", Optional.empty()));
                Assembly assembly = Assembly.open(config(), shared, noPeers(),
                        Clock.systemUTC())) {
            assembly.catalog().register(
                    new IndexRegistration(INDEX_UUID, INDEX, List.of(), 4, 4, 1, 1));
            assembly.ingest().append(PRINCIPAL, INDEX, 0, sink -> sink.accept(
                    new SegmentRecord("doc", OpType.INDEX, OptionalLong.of(1),
                            "doc".getBytes(StandardCharsets.UTF_8))));

            // ⚠️ A SEGMENT THE NODE's CACHE HAS NEVER SEEN, so its proxy must GET it.
            RunKey stream = RunKey.ofIndexUuid(INDEX_UUID, 0);
            SegmentWriter writer = new SegmentWriter();
            writer.add(stream, new SegmentRecord("cold", OpType.INDEX, OptionalLong.empty(),
                    new byte[100]), 0);
            String cold = new SegmentKey(PREFIX, 1_790_000_000_000L, "podz", 1, 48).key();
            shared.put(cold, Body.ofBytes(writer.toByteArray(0)));
            long before = assembly.dataSegmentGets();
            assembly.segmentProxy().streamTo(cold, List.<SegmentSink>of((b, o, l) -> { }));
            assertThat(assembly.dataSegmentGets() - before).as("the premise: one proxy GET")
                    .isEqualTo(1);
            assertThat(assembly.costLedger().snapshot().totalMicros(Charge.DATA_GET))
                    .as("⚠️ THE PROXY's GET REACHED THE POD's LEDGER: a private one reads 0")
                    .isPositive();
            assertExact(assembly);

            long beforeCatchUp = assembly.dataSegmentGets();
            List<byte[]> frames = new ArrayList<>();
            assembly.respondCatchUp(new CatchUpRequestFrame(UUID.randomUUID(),
                    List.of(new CatchUpRequestFrame.Stream(stream, 0))), frames::add);
            assertThat(frames).as("the premise: the written record was replayed").hasSize(2);
            assertThat(assembly.dataSegmentGets() - beforeCatchUp)
                    .as("the premise: the catch-up read its segment from the store")
                    .isEqualTo(1);
            assertThat(assembly.costLedger().snapshot().totalMicros(Charge.DATA_GET))
                    .as("⚠️ AND THE CATCH-UP's GET TOO: two GETs, two requests' worth")
                    .isEqualTo(2 * IndexCostLedger.MICROS_PER_REQUEST);
            assertExact(assembly);
            List<String> written = shared.list(PREFIX + "/data/", null, 100).objects().stream()
                    .map(ObjectStat::key).toList();
            assertThat(written).as("the flushed segment and the cold one").hasSize(2);
        }
    }
}
