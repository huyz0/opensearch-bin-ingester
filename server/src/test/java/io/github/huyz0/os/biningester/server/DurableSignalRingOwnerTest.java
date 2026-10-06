// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.StoreCounts;
import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.DurableSegmentSignalFrame;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentFormat;
import io.github.huyz0.os.biningester.format.SegmentKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import io.github.huyz0.os.biningester.http.DurableSegmentSignalService;
import io.github.huyz0.os.biningester.http.EndpointSliceView;
import io.github.huyz0.os.biningester.ingest.AzPeers;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.ingest.Peer;
import io.github.huyz0.os.biningester.ingest.PeerRing;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Inbox;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DurableSignalRingOwnerTest {
    private static final String PREFIX = "bins/cluster-a";
    private static final String WRITER = "writera";
    private static final String WRITER_AZ = "az-a";
    private static final String OWNER_AZ = "az-b";

    private record StoredSegment(String key, int byteCount) {
    }

    @Test
    void multipleReadyCandidatesInOneAzWarmOnlyTheDeterministicRingOwner() throws Exception {
        try (var raw = new ObservedStore(Inbox.prefixFor(PREFIX))) {
            var store = new CountingBinStore(raw);
            StoredSegment segment = storeSegment(store);
            String segmentKey = segment.key();
            EndpointSliceView view = peers();
            var config = new StoreConfig("memory", Optional.empty(), Optional.empty(),
                    Optional.empty(), Optional.empty(), true);
            try (Assembly first = Assembly.open(config("owner1", OWNER_AZ, config), store,
                    noPeers(), Clock.systemUTC(), view, new CrossAzBytes(OWNER_AZ));
                    Assembly second = Assembly.open(config("owner2", OWNER_AZ, config), store,
                            noPeers(), Clock.systemUTC(), view, new CrossAzBytes(OWNER_AZ))) {
                var firstReceiver = new DurableSegmentSignalService(view,
                        first::prefetchDurableSegment);
                var secondReceiver = new DurableSegmentSignalService(view,
                        second::prefetchDurableSegment);
                byte[] signal = new DurableSegmentSignalFrame(WRITER, WRITER_AZ, segmentKey)
                        .encode();
                // ⚠️ M10.31: THE BASELINE WAITS FOR THE STARTUP INBOX DRAIN's LIST.
                // The node that takes the term drains the inbox once on its own
                // virtual thread (ADR-0058), and nothing in `open` waits for it; a
                // total taken before that LIST lands counts it inside the window.
                // MEASURED: holding that LIST until the baseline was taken failed
                // the signal assertion below with lists=2 every time. One node holds the
                // term, so there is exactly one such LIST.
                raw.awaitInboxListed();
                StoreCounts before = store.counts();

                assertThat(firstReceiver.accept(WRITER, signal)).isTrue();
                assertThat(secondReceiver.accept(WRITER, signal)).isTrue();

                List<Peer> candidates = List.of(
                        new Peer("owner1", "http://owner1:8080", OWNER_AZ),
                        new Peer("owner2", "http://owner2:8080", OWNER_AZ));
                String ownerId = PeerRing.ownerOf(segmentKey,
                        new AzPeers(OWNER_AZ, candidates)).orElseThrow().podId();
                Assembly owner = ownerId.equals("owner1") ? first : second;
                StoreCounts afterSignals = store.counts();
                assertThat(afterSignals).as("both signal recipients cause one GET and no other "
                                + "object-store request")
                        .isEqualTo(new StoreCounts(before.puts(), before.gets() + 1,
                                before.lists(), before.stats(), before.deletes()));

                var rereadBytes = new java.util.concurrent.atomic.AtomicInteger();
                owner.segmentProxy().streamTo(segmentKey, List.of(
                        (bytes, offset, length) -> rereadBytes.addAndGet(length)));

                assertThat(rereadBytes).as("the warm cache returns the complete segment")
                        .hasValue(segment.byteCount());
                assertThat(store.counts())
                        .as("the deterministic ring owner's first shared-cache read is warm")
                        .isEqualTo(afterSignals);
            }
        }
    }

    private static StoredSegment storeSegment(CountingBinStore store) throws Exception {
        var stream = new RunKey(UUID.fromString("00000000-0000-0000-0000-000000000001"), 0);
        var writer = new SegmentWriter();
        writer.add(stream, new SegmentRecord("doc-1", OpType.INDEX, OptionalLong.empty(),
                "payload".getBytes(java.nio.charset.StandardCharsets.UTF_8)), 1L);
        String key = new SegmentKey(PREFIX, 1L, WRITER, 0L,
                SegmentFormat.DIRECTORY_ENTRY_BYTES).key();
        byte[] bytes = writer.toByteArray(1L);
        store.put(key, Body.ofBytes(bytes));
        return new StoredSegment(key, bytes.length);
    }

    private static EndpointSliceView peers() {
        var view = new EndpointSliceView();
        view.apply("{\"type\":\"ADDED\",\"object\":{\"metadata\":{\"name\":\"ingesters\"},"
                + "\"endpoints\":["
                + endpoint("writera", WRITER, WRITER_AZ) + ","
                + endpoint("owner1", "owner1", OWNER_AZ) + ","
                + endpoint("owner2", "owner2", OWNER_AZ) + "]}} ");
        return view;
    }

    private static String endpoint(String address, String pod, String az) {
        return "{\"addresses\":[\"" + address + "\"],\"zone\":\"" + az
                + "\",\"conditions\":{\"ready\":true},\"targetRef\":{\"name\":\""
                + pod + "\"}}";
    }

    /**
     * ⚠️ A LEASE NO RENEWAL OF WHICH FALLS INSIDE THE TEST (M11.15, H10): the
     * windows above compare STORE-WIDE counts, and a renewal every 3 s could
     * land its PUT inside one. MEASURED that renewals do reach them: at a 20 ms
     * renewal the signal window read puts=15 against 6, three runs of three.
     */
    private static ServerConfig config(String podId, String az, StoreConfig store) {
        return new ServerConfig(podId, az, "cluster-a", PREFIX, store, Duration.ofHours(1),
                Duration.ofMinutes(20), "http://" + podId + ":8080", IngestConfig.defaults(
                        "cluster-a"), 8080, "producer", Set.of("logs"), RetentionConfig.defaults(), java.util.Optional.empty(), "uid-" + podId, io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL, io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false, java.util.Optional.empty());
    }

    private static SequencerTransport noPeers() {
        return new SequencerTransport() {
            @Override
            public io.github.huyz0.os.biningester.format.CommitDelta send(
                    String endpoint, CommitRequest request) {
                throw new AssertionError("unexpected peer commit");
            }

            @Override
            public void close() {
            }
        };
    }
}
