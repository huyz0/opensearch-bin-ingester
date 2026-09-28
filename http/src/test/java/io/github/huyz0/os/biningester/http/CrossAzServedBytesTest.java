// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.binstore.CrossAzBytes.Transport;
import io.github.huyz0.os.biningester.client.Delivery;
import io.github.huyz0.os.biningester.client.HttpSubscriptionTransport;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import io.github.huyz0.os.biningester.ingest.WatermarkTable;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import io.helidon.webserver.WebServer;
import io.helidon.webserver.http.HttpRouting;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Cross-AZ bytes counted at the peer sockets, over real ones (M9.2, NFR-5,
 * M9 criterion 6).
 *
 * <p>⚠️ **AT LEAST ONE CASE SERVES A CONSUMER ACROSS AN AZ BOUNDARY**, which
 * criterion 6 demands by name: a steady-state write-only run has near-zero
 * cross-AZ bytes BY CONSTRUCTION, so an entirely uninstrumented build passes
 * one. The cases below make a consumer in {@code az-b} poll an ingester in
 * {@code az-a} and ask that the bytes it was served appear; the same poll from
 * {@code az-a} must appear on the other side of the ledger, which is what
 * makes the first case a measurement rather than a tautology.
 */
class CrossAzServedBytesTest {

    private static final UUID LOGS = UUID.randomUUID();
    private static final RunKey STREAM = new RunKey(LOGS, 3);
    private static final String HERE = "az-a";

    private WebServer server;
    private HttpSubscriptionTransport transport;

    @AfterEach
    void stop() {
        if (transport != null) {
            transport.close();
        }
        if (server != null) {
            server.stop();
        }
    }

    private String startIngester(SubscriptionHub hub, CrossAzBytes counter) {
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder()
                        .register(new SubscriptionService(hub, new IndexCatalog(), table(),
                                Clock.systemUTC(), io.github.huyz0.os.biningester.ingest.RetainedFloors.unknown(),
                                new DrainGate(), counter)))
                .build().start();
        return "http://localhost:" + server.port();
    }

    private static WatermarkTable table() {
        return new WatermarkTable(Clock.systemUTC(), Duration.ofMinutes(1), Duration.ofHours(2),
                Duration.ofMinutes(30));
    }

    private HttpSubscriptionTransport connect(String endpoint, String consumerAz) {
        transport = new HttpSubscriptionTransport(endpoint, () -> { }, Duration.ofMillis(20),
                Duration.ofMillis(200), Duration.ofSeconds(5), Duration.ofMillis(500),
                32 * 1024 * 1024, consumerAz);
        return transport;
    }

    @Test
    void aConsumerInAnotherZoneIsServedCrossAzBytes() throws Exception {
        CrossAzBytes counter = new CrossAzBytes(HERE);
        SubscriptionHub hub = new SubscriptionHub();
        String endpoint = startIngester(hub, counter);
        List<Delivery> got = new CopyOnWriteArrayList<>();

        try (var ignored = connect(endpoint, "az-b").subscribe(STREAM, got::add)) {
            await(() -> transport.reconnects() > 0, "the stream to open");
            publish(hub, "seg-1", 5000, 100);
            await(() -> !got.isEmpty(), "a delivery");
        }

        assertThat(counter.crossAzBytes())
                .as("⚠️ THE SEGMENT WAS SERVED ACROSS A ZONE BOUNDARY, so NFR-5's "
                        + "numerator is not zero and a build that never counted would "
                        + "report that it was")
                .isGreaterThan(0);
        assertThat(counter.sameAzBytes()).isZero();
        assertThat(counter.unknownPeerBytes())
                .as("the consumer named its zone, so nothing here is assumed")
                .isZero();
        assertThat(counter.crossAzBytes(Transport.INLINE_PUSH))
                .as("an inline push's payload is attributed to the inline transport")
                .isGreaterThanOrEqualTo(3);
    }

    @Test
    void theSameConsumerInThisZoneIsServedNoCrossAzBytes() throws Exception {
        // ⚠️ THE FALSIFIER FOR THE CASE ABOVE. Identical traffic, one label
        // changed: a counter keyed by the POD, or by "is this me", would count
        // these bytes cross-AZ too and both cases would still pass alone.
        CrossAzBytes counter = new CrossAzBytes(HERE);
        SubscriptionHub hub = new SubscriptionHub();
        String endpoint = startIngester(hub, counter);
        List<Delivery> got = new CopyOnWriteArrayList<>();

        try (var ignored = connect(endpoint, HERE).subscribe(STREAM, got::add)) {
            await(() -> transport.reconnects() > 0, "the stream to open");
            publish(hub, "seg-1", 5000, 100);
            await(() -> !got.isEmpty(), "a delivery");
        }

        assertThat(counter.crossAzBytes()).isZero();
        assertThat(counter.sameAzBytes()).isGreaterThan(0);
    }

    @Test
    void aConsumerThatNamesNoZoneIsCountedCrossAzAndReportedUnknown() throws Exception {
        // ⚠️ THE SAFE SIDE, LOUDLY. Every consumer built before M9.2 lands
        // here, and a build that dropped these bytes would report a fleet-wide
        // zero for the largest term in NFR-5.
        CrossAzBytes counter = new CrossAzBytes(HERE);
        SubscriptionHub hub = new SubscriptionHub();
        String endpoint = startIngester(hub, counter);
        List<Delivery> got = new CopyOnWriteArrayList<>();

        try (var ignored = connect(endpoint, "").subscribe(STREAM, got::add)) {
            await(() -> transport.reconnects() > 0, "the stream to open");
            publish(hub, "seg-1", 5000, 100);
            await(() -> !got.isEmpty(), "a delivery");
        }

        assertThat(counter.unknownPeerBytes()).isGreaterThan(0);
        assertThat(counter.crossAzBytes()).isEqualTo(counter.unknownPeerBytes());
        assertThat(counter.sameAzBytes()).isZero();
    }

    @Test
    void everyByteOfAPollAnswerIsCountedEXACTLYOnce() throws Exception {
        // ⚠️ THE INVARIANT THAT TIES THE SPLIT TO THE WIRE: the per-transport
        // columns must add up to the bytes the consumer actually received, so
        // no arithmetic in the attribution can be off by a frame and stay
        // plausible. ⚠️ TWO PUSHES, DELIBERATELY: with one, the offset of the
        // first frame is 0 and a subtraction mutated into an addition gives
        // the same answer -- which review MEASURED surviving.
        CrossAzBytes counter = new CrossAzBytes(HERE);
        SubscriptionHub hub = new SubscriptionHub();
        String endpoint = startIngester(hub, counter);
        String subscriber = java.util.UUID.randomUUID().toString();
        io.helidon.webclient.api.WebClient client =
                io.helidon.webclient.api.WebClient.builder().baseUri(endpoint).build();

        // ⚠️ THE FIRST POLL OPENS THE SESSION; the hub delivers to sessions
        // that exist, so a push published before one does reaches nobody.
        byte[] opening = poll(client, subscriber);
        assertThat(opening).isEmpty();

        publish(hub, "seg-1", 5000, 100);
        publish(hub, "seg-2", 5100, 7);
        byte[] answer = poll(client, subscriber);

        assertThat(answer.length)
                .as("both pushes drain into ONE answer, which is what makes the "
                        + "subtraction observable at all")
                .isGreaterThan(0);
        assertThat(counter.crossAzBytes())
                .as("⚠️ EVERY BYTE THE CONSUMER RECEIVED, COUNTED ONCE -- no more, "
                        + "which would inflate NFR-5's numerator, and no less, which "
                        + "would hide it")
                .isEqualTo(answer.length);
        assertThat(counter.crossAzBytes(Transport.INLINE_PUSH)
                + counter.crossAzBytes(Transport.PROXY_READ)
                + counter.crossAzBytes(Transport.CONSUMER_POLL))
                .isEqualTo(answer.length);
    }

    private static byte[] poll(io.helidon.webclient.api.WebClient client, String subscriber) {
        try (var response = client.get(HttpSubscriptionTransport.SUBSCRIBE_PREFIX
                        + STREAM.indexId() + "/" + STREAM.partitionId())
                .queryParam("wait", "50")
                .queryParam("sub", subscriber)
                .queryParam(SubscriptionService.AZ_PARAM, "az-b")
                .request()) {
            // ⚠️ AN EMPTY 200 CARRIES NO ENTITY AT ALL and `as` throws on it;
            // the quiet-stream answer is zero bytes, not an error.
            return response.entity().hasEntity() ? response.as(byte[].class) : new byte[0];
        }
    }

    @Test
    void aCommitForwardedToAPeerInAnotherZoneCountsItsBytes() throws Exception {
        CrossAzBytes counter = new CrossAzBytes(HERE, endpoint -> Optional.of("az-b"));
        String peer = startSequencer();
        try (var forwarding = new HttpSequencerTransport(Duration.ofSeconds(5), counter)) {
            forwarding.send(peer, request());
        }
        assertThat(counter.crossAzBytes(Transport.COMMIT_FORWARD)).isGreaterThan(0);
        assertThat(counter.sameAzBytes()).isZero();
    }

    @Test
    void aCommitForwardedWithinThisZoneCountsNoCrossAzBytes() throws Exception {
        CrossAzBytes counter = new CrossAzBytes(HERE, endpoint -> Optional.of(HERE));
        String peer = startSequencer();
        try (var forwarding = new HttpSequencerTransport(Duration.ofSeconds(5), counter)) {
            forwarding.send(peer, request());
        }
        assertThat(counter.crossAzBytes()).isZero();
        assertThat(counter.sameAzBytes(Transport.COMMIT_FORWARD)).isGreaterThan(0);
    }

    @Test
    void anInboxDrainAskedOfAPeerInAnotherZoneCountsItsBytes() throws Exception {
        CrossAzBytes counter = new CrossAzBytes(HERE, endpoint -> Optional.of("az-b"));
        String peer = startSequencer();
        try (var forwarding = new HttpSequencerTransport(Duration.ofSeconds(5), counter)) {
            try {
                forwarding.drain(peer, "pod9");
            } catch (java.io.IOException refused) {
                // ⚠️ THE ASK IS COUNTED WHATEVER THE PEER ANSWERS: the bytes
                // left this pod either way, and ADR-0058 puts this path in
                // exactly the partition where the answer is least likely.
            }
        }
        assertThat(counter.crossAzBytes(Transport.INBOX_DRAIN)).isGreaterThan(0);
    }

    private String startSequencer() {
        server = WebServer.builder().port(0)
                .routing(HttpRouting.builder().register(new CommitService(CrossAzServedBytesTest::sequencer)))
                .build().start();
        return "http://localhost:" + server.port();
    }

    private static Sequencer sequencer() {
        return new Sequencer() {
            @Override
            public CommitDelta commitAll(List<CommitRequest> requests) {
                return new CommitDelta(11, List.of(new SegmentCommit("bins/c/data/seg-1",
                        List.of(new RunCommit(STREAM, 100, 5000)),
                        new SegmentCommit.Attribution("poda", "inc-1", 7))));
            }

            @Override
            public void close() {
            }
        };
    }

    private static CommitRequest request() {
        return new CommitRequest("poda", "inc-1", 7, "bins/c/data/seg-1", Map.of(STREAM, 100));
    }

    private static void await(java.util.function.BooleanSupplier condition, String what) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.onSpinWait();
        }
    }

    private static void publish(SubscriptionHub hub, String segmentKey, long firstOffset,
            int records) throws Exception {
        CommitDelta delta = new CommitDelta(1, List.of(new SegmentCommit(segmentKey,
                List.of(new RunCommit(STREAM, records, firstOffset)),
                new SegmentCommit.Attribution("pod1", "inc-1", 0))));
        try (var store = new io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore()) {
            store.put(segmentKey, io.github.huyz0.os.biningester.binstore.Body.ofBytes(new byte[] {1, 2, 3}));
            hub.publish(delta, segmentKey, new byte[] {1, 2, 3},
                    new io.github.huyz0.os.biningester.ingest.SegmentServing(
                            new io.github.huyz0.os.biningester.ingest.FetchPolicy(new io.github.huyz0.os.biningester.ingest.FetchPolicyConfig(
                                    Long.MAX_VALUE, Long.MAX_VALUE, 1, false)),
                            store.capabilities(), new io.github.huyz0.os.biningester.ingest.SegmentProxy(store,
                        io.github.huyz0.os.biningester.ingest.SegmentProxy.DEFAULT_CHUNK_BYTES,
                        new io.github.huyz0.os.biningester.ingest.SegmentCache(0),
                        new io.github.huyz0.os.biningester.binstore.IndexCostLedger())));
        }
    }
}
