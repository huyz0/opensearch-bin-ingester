// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import io.github.huyz0.os.biningester.binstore.IndexCostLedger.Charge;
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
 * ONE read workload on ONE assembled pod exercises all three data-segment GET
 * kinds (M12.18; M11.3 T2): a GET the pod's cache HOLDS, split by the
 * segment's directory; a GET STREAMED PAST THE HOLD -- a segment larger than
 * the cache -- charged whole to unattributed; and a CATCH-UP read. After each,
 * the sum of index GET shares plus unattributed equals the pod's counted
 * data-segment GETs times 10^6. M11.3 pinned the streamed-past case at the
 * component only.
 */
class AssemblyReadWorkloadAttributionTest {

    private static final String PREFIX = "bins/cluster-a";
    private static final String HEAVY = "logs";
    private static final String LIGHT = "metrics";
    private static final UUID HEAVY_UUID = UUID.randomUUID();
    private static final UUID LIGHT_UUID = UUID.randomUUID();
    private static final Principal PRINCIPAL =
            new Principal("cluster-a", "producer-1", Set.of(HEAVY, LIGHT));

    /** 4 KiB segments, so the pod's cache holds four of them: 16 KiB. */
    private static final long SEGMENT_BYTES = 4 * 1024;

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
                new IngestConfig(IngestConfig.DEFAULT_INTERVAL_FLOOR, SEGMENT_BYTES, "cluster-a",
                        IngestConfig.DEFAULT_MAX_QUEUED_PUSH_BYTES),
                0, "producer-1", Set.of(HEAVY, LIGHT),
                new RetentionConfig(Duration.ofMinutes(1), Duration.ofHours(2),
                        Duration.ofSeconds(10), Duration.ofHours(3), Duration.ofDays(1)), java.util.Optional.empty(), "", io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL, io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false);
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

    private static byte[] segment(int heavyBytes, int lightBytes) throws Exception {
        SegmentWriter writer = new SegmentWriter();
        writer.add(RunKey.ofIndexUuid(base64Url(HEAVY_UUID), 0), new SegmentRecord("h",
                OpType.INDEX, OptionalLong.empty(), new byte[heavyBytes]), 0);
        writer.add(RunKey.ofIndexUuid(base64Url(LIGHT_UUID), 0), new SegmentRecord("l",
                OpType.INDEX, OptionalLong.empty(), new byte[lightBytes]), 0);
        return writer.toByteArray(0);
    }

    private static long share(Assembly assembly, UUID index) {
        return assembly.costLedger().snapshot().indices().stream()
                .filter(cost -> cost.index().equals(index))
                .mapToLong(cost -> cost.micros().getOrDefault(Charge.DATA_GET, 0L)).sum();
    }

    private static long unattributed(Assembly assembly) {
        return assembly.costLedger().snapshot().unattributed().getOrDefault(Charge.DATA_GET, 0L);
    }

    private static void assertExact(Assembly assembly, String after) {
        assertThat(assembly.costLedger().snapshot().totalMicros(Charge.DATA_GET))
                .as("⚠️ EXACT after %s: index GET shares + unattributed = the pod's "
                        + "counted data-segment GETs x 10^6", after)
                .isEqualTo(assembly.dataSegmentGets() * IndexCostLedger.MICROS_PER_REQUEST);
    }

    @Test
    void heldStreamedPastTheHoldAndCatchUpGetsAllSumToTheCountedGets() throws Exception {
        try (BinStore shared = StoreFactory.open(new StoreConfig("memory", Optional.empty()));
                Assembly assembly = Assembly.open(config(), shared, noPeers(),
                        Clock.systemUTC())) {
            assembly.catalog().register(new IndexRegistration(base64Url(HEAVY_UUID), HEAVY,
                    List.of(), 4, 4, 1, 1));
            assembly.catalog().register(new IndexRegistration(base64Url(LIGHT_UUID), LIGHT,
                    List.of(), 4, 4, 1, 1));
            assembly.ingest().append(PRINCIPAL, HEAVY, 0, sink -> sink.accept(
                    new SegmentRecord("doc", OpType.INDEX, OptionalLong.of(1),
                            "doc".getBytes(StandardCharsets.UTF_8))));

            // 1. HELD: a cold two-index segment under the cache's 16 KiB.
            String held = new SegmentKey(PREFIX, 1_790_000_000_000L, "podz", 1, 48).key();
            shared.put(held, Body.ofBytes(segment(2_000, 20)));
            long gets = assembly.dataSegmentGets();
            assembly.segmentProxy().streamTo(held, List.<SegmentSink>of((b, o, l) -> { }));
            assertThat(assembly.dataSegmentGets() - gets).as("the premise: one GET").isEqualTo(1);
            assertThat(share(assembly, HEAVY_UUID)).as("⚠️ HELD: split, the heavy index's share")
                    .isGreaterThan(share(assembly, LIGHT_UUID));
            assertThat(share(assembly, LIGHT_UUID)).as("and the light index's").isPositive();
            assertThat(unattributed(assembly)).as("a held GET is split, not unattributed")
                    .isZero();
            assertExact(assembly, "the held GET");

            // 2. STREAMED PAST THE HOLD: larger than the whole cache.
            String oversized = new SegmentKey(PREFIX, 1_790_000_000_001L, "podz", 2, 48).key();
            byte[] big = segment(20_000, 20);
            assertThat((long) big.length).as("the premise: past the 16 KiB hold")
                    .isGreaterThan(4 * SEGMENT_BYTES);
            shared.put(oversized, Body.ofBytes(big));
            gets = assembly.dataSegmentGets();
            long heavyBefore = share(assembly, HEAVY_UUID);
            assembly.segmentProxy().streamTo(oversized, List.<SegmentSink>of((b, o, l) -> { }));
            assertThat(assembly.dataSegmentGets() - gets).as("the premise: one GET").isEqualTo(1);
            assertThat(unattributed(assembly))
                    .as("⚠️ STREAMED PAST THE HOLD: charged whole to unattributed, never dropped")
                    .isEqualTo(IndexCostLedger.MICROS_PER_REQUEST);
            assertThat(share(assembly, HEAVY_UUID)).as("and not split").isEqualTo(heavyBefore);
            assertExact(assembly, "the GET streamed past the hold");

            // 3. CATCH-UP: the appended record, replayed from its flushed segment.
            gets = assembly.dataSegmentGets();
            long heavyBeforeCatchUp = share(assembly, HEAVY_UUID);
            long unattributedBeforeCatchUp = unattributed(assembly);
            List<byte[]> frames = new ArrayList<>();
            assembly.respondCatchUp(new CatchUpRequestFrame(UUID.randomUUID(),
                    List.of(new CatchUpRequestFrame.Stream(
                            RunKey.ofIndexUuid(base64Url(HEAVY_UUID), 0), 0))), frames::add);
            assertThat(frames).as("the premise: the written record was replayed").isNotEmpty();
            assertThat(assembly.dataSegmentGets() - gets)
                    .as("the premise: the catch-up read its segment from the store")
                    .isPositive();
            assertExact(assembly, "the catch-up read");
            // ⚠️ AND WHERE IT WENT, not only that it summed (M13.7, M12.18 review
            // T1): the flushed segment holds only the heavy index's run, so its
            // GETs are the heavy index's, whole -- a catch-up GET charged whole
            // to unattributed kept the sum exact and passed.
            assertThat(share(assembly, HEAVY_UUID) - heavyBeforeCatchUp)
                    .as("⚠️ THE CATCH-UP GET's SHARE is the heavy index's, whole")
                    .isEqualTo((assembly.dataSegmentGets() - gets)
                            * IndexCostLedger.MICROS_PER_REQUEST);
            assertThat(unattributed(assembly)).as("and none of it unattributed")
                    .isEqualTo(unattributedBeforeCatchUp);
        }
    }
}
