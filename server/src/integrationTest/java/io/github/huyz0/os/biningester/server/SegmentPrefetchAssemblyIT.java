// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.binstore.backend.S3BinStore;
import io.github.huyz0.os.biningester.binstore.backend.S3Fixture;
import io.github.huyz0.os.biningester.binstore.backend.S3Settings;
import io.github.huyz0.os.biningester.server.chaos.ChaosBucket;
import io.github.huyz0.os.biningester.format.DurableSegmentSignalFrame;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.http.EndpointSliceView;
import io.github.huyz0.os.biningester.security.Principal;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * M8.56: assembled RustFS writes warm the selected remote-AZ cache owner before first read.
 *
 * The writer uses the resolved localhost address and the owner uses the localhost hostname, so
 * both reach this machine while remaining distinct EndpointSlice addresses for source auth.
 * Windows loopback aliases cannot model distinct pod IPs; the production sender still makes a real
 * HTTP request to the owner's FrontDoor and the receiver authorizes its socket peer.
 */
@Timeout(300)
class SegmentPrefetchAssemblyIT {
    private static final String PREFIX = "bins/cluster-a";
    private static final String INDEX_UUID = "AAAAAAAAAAAAAAAAAAAAAA";

    @Test
    void oneRemoteAzOwnerWarmsTheSharedCacheBeforeTheFirstRead() throws Exception {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a T3 suite");
        try (ChaosBucket bucket = ChaosBucket.create()) {
            var settings = bucket.nodeSettings();
            var storeConfig = new StoreConfig("s3", Optional.empty(),
                    Optional.of(settings.get("store.endpoint")),
                    Optional.of(settings.get("store.region")),
                    Optional.of(settings.get("store.bucket")), true);
            var store = new CountingBinStore(S3BinStore.open(
                    new S3Settings(settings.get("store.endpoint"), settings.get("store.region"),
                            settings.get("store.bucket"), true),
                    StaticCredentialsProvider.create(AwsBasicCredentials.create(
                            S3Fixture.ACCESS_KEY, S3Fixture.SECRET_KEY))));
            int ownerPort = freePort();
            var writerConfig = config("writera", "az-a", "127.0.0.1", ownerPort, storeConfig);
            var ownerConfig = config("ownerb", "az-b", "127.0.0.1", ownerPort, storeConfig);
            String writerAddress = java.net.InetAddress.getByName("localhost").getHostAddress();
            EndpointSliceView writerView = peers(writerAddress, "writera", "az-a",
                    "localhost", "ownerb", "az-b");
            EndpointSliceView ownerView = peers(writerAddress, "writera", "az-a",
                    "localhost", "ownerb", "az-b");
            CrossAzBytes writerBytes = new CrossAzBytes("az-a");
            CrossAzBytes ownerBytes = new CrossAzBytes("az-b");
            var writerStore = new CountingBinStore(S3BinStore.open(
                    new S3Settings(settings.get("store.endpoint"), settings.get("store.region"),
                            settings.get("store.bucket"), true),
                    StaticCredentialsProvider.create(AwsBasicCredentials.create(
                            S3Fixture.ACCESS_KEY, S3Fixture.SECRET_KEY))));
            var ownerStore = new CountingBinStore(S3BinStore.open(
                    new S3Settings(settings.get("store.endpoint"), settings.get("store.region"),
                            settings.get("store.bucket"), true),
                    StaticCredentialsProvider.create(AwsBasicCredentials.create(
                            S3Fixture.ACCESS_KEY, S3Fixture.SECRET_KEY))));
            try (writerStore; ownerStore;
                    Assembly writer = Assembly.open(writerConfig, writerStore, noPeers(),
                            Clock.systemUTC(), writerView, writerBytes);
                    Assembly owner = Assembly.open(ownerConfig, ownerStore, noPeers(),
                            Clock.systemUTC(), ownerView, ownerBytes);
                    FrontDoor ownerDoor = FrontDoor.start(owner, Clock.systemUTC())) {
                assertThat(ownerDoor.port()).isEqualTo(ownerPort);
                writer.catalog().register(new IndexRegistration(INDEX_UUID, "logs", List.of(),
                        1, 1, 1, 1));
                Principal principal = new Principal("cluster-a", "producer", Set.of("logs"));
                long getsBefore = owner.storeCounts().gets();
                writer.ingest().append(principal, "logs", 0, sink -> sink.accept(
                        new SegmentRecord("doc-1", OpType.INDEX, OptionalLong.of(1),
                                "payload".getBytes(StandardCharsets.UTF_8))));
                // Append promises commit durability; flushNow additionally joins the
                // serialized flush after its best-effort HTTP hint has returned.
                writer.flush();

                List<String> segments = bucket.segments();
                assertThat(segments).hasSize(1);
                String segmentKey = segments.getFirst();
                byte[] frame = new DurableSegmentSignalFrame("writera", "az-a", segmentKey)
                        .encode();
                assertThat(writerBytes.crossAzBytes(CrossAzBytes.Transport.DURABLE_SEGMENT_SIGNAL))
                        .isEqualTo(frame.length)
                        .isLessThanOrEqualTo(DurableSegmentSignalFrame.MAX_FRAME_BYTES);
                long warmedGets = owner.storeCounts().gets();
                assertThat(warmedGets).as("one selected remote-AZ owner warms the segment")
                        .isEqualTo(getsBefore + 1);

                AtomicInteger bytesRead = new AtomicInteger();
                owner.segmentProxy().streamTo(segmentKey, List.of(
                        (bytes, offset, length) -> bytesRead.addAndGet(length)));

                assertThat(bytesRead.get()).isGreaterThan(0);
                assertThat(owner.storeCounts().gets())
                        .as("the first consumer read reuses the prefetch owner's cache")
                        .isEqualTo(warmedGets);
            }
        }
    }

    private static EndpointSliceView peers(String writerAddress, String writerId, String writerAz,
            String ownerAddress, String ownerId, String ownerAz) {
        EndpointSliceView view = new EndpointSliceView();
        view.apply("{\"type\":\"ADDED\",\"object\":{\"metadata\":{\"name\":\"ingesters\"},"
                + "\"endpoints\":["
                + endpoint(writerAddress, writerId, writerAz) + ","
                + endpoint(ownerAddress, ownerId, ownerAz) + "]}} ");
        return view;
    }

    private static String endpoint(String address, String pod, String az) {
        return "{\"addresses\":[\"" + address + "\"],\"zone\":\"" + az
                + "\",\"conditions\":{\"ready\":true},\"targetRef\":{\"name\":\""
                + pod + "\"}}";
    }

    private static ServerConfig config(String pod, String az, String host, int port,
            StoreConfig store) {
        return new ServerConfig(pod, az, "cluster-a", PREFIX, store,
                Duration.ofSeconds(10), Duration.ofSeconds(3),
                "http://" + host + ":" + port, IngestConfig.defaults("cluster-a"), port,
                "producer", Set.of("logs"), RetentionConfig.defaults(), java.util.Optional.empty(), "uid-" + pod, io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL, io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false, java.util.Optional.empty(), PeerConfig.off(0));
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

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
