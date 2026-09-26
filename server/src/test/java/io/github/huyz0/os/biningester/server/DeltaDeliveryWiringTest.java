// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.http.EndpointSliceView;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import io.github.huyz0.os.biningester.security.Principal;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * An assembled leaseholder publishes each durable delta itself and pushes it
 * to a same-AZ pod, whose subscribers receive a write that pod never saw
 * (M10.20a, ADR-0075).
 *
 * <p>⚠️ ONE STORE, TWO ROOTS, ONE PORT, the shape {@code SegmentPrefetchAssemblyIT}
 * uses: only the follower's front door is started, on the port both configs
 * name, so the leaseholder's push reaches it at the address the membership
 * lists.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DeltaDeliveryWiringTest {

    private static final String PREFIX = "bins/cluster-a";

    private static final SequencerTransport NO_PEERS = new SequencerTransport() {
        @Override
        public io.github.huyz0.os.biningester.format.CommitDelta send(
                String endpoint, CommitRequest request) {
            throw new AssertionError("unexpected peer commit");
        }

        @Override
        public void close() {
        }
    };

    private static ServerConfig config(String pod, int port) {
        return config(pod, "az-a", port);
    }

    private static ServerConfig config(String pod, String az, int port) {
        return new ServerConfig(pod, az, "cluster-a", PREFIX,
                new StoreConfig("memory", Optional.empty()), Duration.ofSeconds(10),
                Duration.ofSeconds(3), "http://" + pod + ":" + port,
                IngestConfig.defaults("cluster-a"), port, "producer", Set.of("logs"));
    }

    /** The leaseholder at 127.0.0.1 and the follower at 127.0.0.2, both in az-a. */
    private static EndpointSliceView members() {
        return members("az-a");
    }

    /** The leaseholder at 127.0.0.1 in az-a; the follower at 127.0.0.2 in {@code followerAz}. */
    private static EndpointSliceView members(String followerAz) {
        EndpointSliceView view = new EndpointSliceView();
        view.apply("{\"type\":\"ADDED\",\"object\":{\"metadata\":{\"name\":\"ingesters\"},"
                + "\"endpoints\":[" + endpoint("127.0.0.1", "leader", "az-a") + ","
                + endpoint("127.0.0.2", "follower", followerAz) + "]}}");
        return view;
    }

    private static String endpoint(String address, String pod, String az) {
        return "{\"addresses\":[\"" + address + "\"],\"zone\":\"" + az + "\","
                + "\"conditions\":{\"ready\":true},\"targetRef\":{\"name\":\"" + pod
                + "\",\"uid\":\"uid-" + pod + "\"}}";
    }

    private static String indexUuid(UUID uuid) {
        ByteBuffer buffer = ByteBuffer.allocate(16);
        buffer.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
        return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(buffer.array());
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    void aSameAzFollowersSubscriberReceivesTheLeaseholdersWrite() throws Exception {
        int port = freePort();
        UUID index = UUID.randomUUID();
        RunKey stream = new RunKey(index, 0);
        List<SubscriptionHub.Push> atLeader = new CopyOnWriteArrayList<>();
        List<SubscriptionHub.Push> atFollower = new CopyOnWriteArrayList<>();
        CrossAzBytes leaderBytes = new CrossAzBytes("az-a");
        try (var store = new MemoryBinStore();
                Assembly leader = Assembly.open(config("leader", port), store, NO_PEERS,
                        Clock.systemUTC(), members(), leaderBytes);
                Assembly follower = Assembly.open(config("follower", port), store, NO_PEERS,
                        Clock.systemUTC(), members(), new CrossAzBytes("az-a"));
                FrontDoor followerDoor = FrontDoor.start(follower, Clock.systemUTC());
                var leaderSub = leader.hub().subscribe(stream,
                        SubscriptionHub.assembling(atLeader::add));
                var followerSub = follower.hub().subscribe(stream,
                        SubscriptionHub.assembling(atFollower::add))) {
            assertThat(followerDoor.port()).isEqualTo(port);
            leader.catalog().register(new IndexRegistration(indexUuid(index), "logs", List.of(),
                    1, 1, 1, 1));
            Principal principal = new Principal("cluster-a", "producer", Set.of("logs"));
            leader.ingest().append(principal, "logs", 0, sink -> sink.accept(
                    new SegmentRecord("doc-1", OpType.INDEX, OptionalLong.of(1),
                            "payload".getBytes(StandardCharsets.UTF_8))));

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while ((atLeader.isEmpty() || atFollower.isEmpty())
                    && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(atLeader).as("the leaseholder publishes its own commit").hasSize(1);
            assertThat(atFollower).as("and pushes it to its AZ").hasSize(1);
            assertThat(atFollower.get(0).segmentKey()).isEqualTo(atLeader.get(0).segmentKey());
            assertThat(atFollower.get(0).firstOffset()).isZero();
            assertThat(leaderBytes.sameAzBytes(CrossAzBytes.Transport.DELTA_PUSH)).isPositive();
            assertThat(leaderBytes.crossAzBytes(CrossAzBytes.Transport.DELTA_PUSH)).isZero();
        }
    }

    @Test
    void aRemoteAzsRelayReadsTheHintedDeltaAndItsSubscriberReceivesIt() throws Exception {
        int port = freePort();
        UUID index = UUID.randomUUID();
        RunKey stream = new RunKey(index, 0);
        List<SubscriptionHub.Push> atRelay = new CopyOnWriteArrayList<>();
        CrossAzBytes leaderBytes = new CrossAzBytes("az-a");
        try (var store = new MemoryBinStore();
                Assembly leader = Assembly.open(config("leader", "az-a", port), store, NO_PEERS,
                        Clock.systemUTC(), members("az-b"), leaderBytes);
                Assembly relay = Assembly.open(config("follower", "az-b", port), store, NO_PEERS,
                        Clock.systemUTC(), members("az-b"), new CrossAzBytes("az-b"));
                FrontDoor relayDoor = FrontDoor.start(relay, Clock.systemUTC());
                var relaySub = relay.hub().subscribe(stream,
                        SubscriptionHub.assembling(atRelay::add))) {
            assertThat(relayDoor.port()).isEqualTo(port);
            leader.catalog().register(new IndexRegistration(indexUuid(index), "logs", List.of(),
                    1, 1, 1, 1));
            Principal principal = new Principal("cluster-a", "producer", Set.of("logs"));
            leader.ingest().append(principal, "logs", 0, sink -> sink.accept(
                    new SegmentRecord("doc-1", OpType.INDEX, OptionalLong.of(1),
                            "payload".getBytes(StandardCharsets.UTF_8))));

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (atRelay.isEmpty() && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(atRelay).as("the remote AZ's relay read it and published it").hasSize(1);
            assertThat(atRelay.get(0).firstOffset()).isZero();
            assertThat(leaderBytes.crossAzBytes(CrossAzBytes.Transport.DELTA_HINT))
                    .as("one 24-byte hint crossed the AZ boundary")
                    .isEqualTo(io.github.huyz0.os.biningester.format.DeltaHintFrame.BYTES);
            assertThat(leaderBytes.crossAzBytes(CrossAzBytes.Transport.DELTA_PUSH))
                    .as("no whole delta did").isZero();
        }
    }

    @Test
    void theFinalFlushAtCloseStillReachesSubscribersBecauseDeliveryClosesAfterTheIngest()
            throws Exception {
        UUID index = UUID.randomUUID();
        RunKey stream = new RunKey(index, 0);
        List<SubscriptionHub.Push> got = new CopyOnWriteArrayList<>();
        // An interval nobody reaches: the record waits for the flush close makes.
        IngestConfig never = new IngestConfig(Duration.ofDays(1), 8L << 20, "cluster-a",
                IngestConfig.DEFAULT_MAX_QUEUED_PUSH_BYTES, Duration.ofDays(1),
                IngestConfig.DEFAULT_FILL_RATIO_LOW_THRESHOLD,
                IngestConfig.DEFAULT_FILL_RATIO_HIGH_THRESHOLD,
                IngestConfig.DEFAULT_INTERVAL_LENGTHEN_DELAY,
                IngestConfig.DEFAULT_INTERVAL_SHORTEN_DELAY);
        ServerConfig config = new ServerConfig("solo", "az-a", "cluster-a", PREFIX,
                new StoreConfig("memory", Optional.empty()), Duration.ofSeconds(10),
                Duration.ofSeconds(3), "http://solo:8080", never, 0, "producer", Set.of("logs"));
        try (var store = new MemoryBinStore()) {
            Assembly node = Assembly.open(config, store, NO_PEERS, Clock.systemUTC());
            var sub = node.hub().subscribe(stream, SubscriptionHub.assembling(got::add));
            node.catalog().register(new IndexRegistration(indexUuid(index), "logs", List.of(),
                    1, 1, 1, 1));
            Principal principal = new Principal("cluster-a", "producer", Set.of("logs"));
            Thread producer = Thread.ofPlatform().start(() -> {
                try {
                    node.ingest().append(principal, "logs", 0, sink -> sink.accept(
                            new SegmentRecord("doc-1", OpType.INDEX, OptionalLong.of(1),
                                    "payload".getBytes(StandardCharsets.UTF_8))));
                } catch (Exception ignored) {
                    // the assertion below is on what subscribers received
                }
            });
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (producer.getState() != Thread.State.WAITING && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(producer.getState()).as("the premise: the record is buffered")
                    .isEqualTo(Thread.State.WAITING);

            node.close();
            producer.join(TimeUnit.SECONDS.toMillis(20));
            sub.close();

            assertThat(got).as("published by the chain publisher the final flush reached")
                    .hasSize(1);
        }
    }
}
