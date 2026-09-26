// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.binstore.backend.S3BinStore;
import io.github.huyz0.os.biningester.binstore.backend.S3Fixture;
import io.github.huyz0.os.biningester.binstore.backend.S3Settings;
import io.github.huyz0.os.biningester.format.DeltaHintFrame;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.http.EndpointSliceView;
import io.github.huyz0.os.biningester.http.HttpSequencerTransport;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import io.github.huyz0.os.biningester.security.Principal;
import io.github.huyz0.os.biningester.server.chaos.ChaosBucket;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

/**
 * SPEC criterion 13 (M10.21, ADR-0075): on RustFS, five pods in three AZs,
 * writes spread across all of them, and every pod's subscriber receives every
 * record, in offset order.
 *
 * <p>⚠️ **ONE JVM, FIVE LOOPBACK ADDRESSES, ONE PORT.** Each pod's front door
 * binds its own 127.0.0.x on the port every config names, the way pods of one
 * Deployment share a container port; a push or hint is a real HTTP request to
 * that address, and each receiver authorizes its socket peer against the same
 * EndpointSlice membership every pod reads. ⚠️ Every pod's push and hint
 * sender is bound to its own address as the SOURCE: loopback aliases would
 * otherwise all send from 127.0.0.1, and a relay's onward push would be
 * refused as coming from another AZ -- the check this test must not bypass.
 * Pods are {@code a1} and {@code a2} in az-a, {@code b1} and {@code b2} in az-b, {@code c1} in az-c; {@code a1}
 * opens first and holds the lease. ⚠️ {@code b2} IS WHY THERE ARE FIVE: it
 * reaches every delta only through its AZ's relay {@code b1} pushing onward,
 * ADR-0075's second hop.
 */
@Timeout(300)
class EveryPodReceivesEveryRecordIT {

    private static final String PREFIX = "bins/cluster-a";
    private static final int ROUNDS = 3;
    private static final int RECORDS_PER_APPEND = 5;

    private record Pod(String id, String az, String address) {
    }

    private static final List<Pod> PODS = List.of(
            new Pod("a1", "az-a", "127.0.0.1"),
            new Pod("a2", "az-a", "127.0.0.2"),
            new Pod("b1", "az-b", "127.0.0.3"),
            new Pod("b2", "az-b", "127.0.0.4"),
            new Pod("c1", "az-c", "127.0.0.5"));

    private static final int B1 = 2;
    private static final int B2 = 3;
    private static final int C1 = 4;

    /** M9's idle bound (IdlePodCostSoakTest): only lease renewals, one per interval plus two. */
    private static final Duration IDLE = Duration.ofSeconds(6);

    @Test
    void everyPodsSubscriberReceivesEveryRecordInOffsetOrder() throws Exception {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a T3 suite");
        try (ChaosBucket bucket = ChaosBucket.create()) {
            Map<String, String> settings = bucket.nodeSettings();
            StoreConfig storeConfig = new StoreConfig("s3", Optional.empty(),
                    Optional.of(settings.get("store.endpoint")),
                    Optional.of(settings.get("store.region")),
                    Optional.of(settings.get("store.bucket")), true);
            int port = freePort();
            UUID index = UUID.randomUUID();
            RunKey stream = new RunKey(index, 0);
            List<AutoCloseable> closing = new ArrayList<>();
            List<Assembly> nodes = new ArrayList<>();
            List<CrossAzBytes> bytes = new ArrayList<>();
            List<List<SubscriptionHub.Push>> received = new ArrayList<>();
            List<ChainLogGets> logGets = new ArrayList<>();
            try {
                for (Pod pod : PODS) {
                    ChainLogGets chainLog = new ChainLogGets(S3BinStore.open(
                            new S3Settings(settings.get("store.endpoint"),
                                    settings.get("store.region"), settings.get("store.bucket"),
                                    true),
                            StaticCredentialsProvider.create(AwsBasicCredentials.create(
                                    S3Fixture.ACCESS_KEY, S3Fixture.SECRET_KEY))));
                    logGets.add(chainLog);
                    CountingBinStore store = new CountingBinStore(chainLog);
                    closing.add(store);
                    CrossAzBytes crossAz = new CrossAzBytes(pod.az());
                    HttpSequencerTransport transport =
                            new HttpSequencerTransport(Duration.ofSeconds(10), crossAz);
                    closing.add(transport);
                    Assembly node = Assembly.openForTest(config(pod, port, storeConfig), store,
                            transport, Clock.systemUTC(), members(), crossAz,
                            new PeerPosts(null, sendingFrom(pod.address())));
                    closing.add(node);
                    closing.add(FrontDoor.start(node, Clock.systemUTC(), event -> { }, crossAz,
                            pod.address()));
                    List<SubscriptionHub.Push> got = new CopyOnWriteArrayList<>();
                    closing.add(node.hub().subscribe(stream, SubscriptionHub.assembling(got::add)));
                    node.catalog().register(new IndexRegistration(indexUuid(index), "logs",
                            List.of(), 1, 1, 1, 1));
                    nodes.add(node);
                    bytes.add(crossAz);
                    received.add(got);
                }
                assertThat(bucket.lease().orElseThrow().holderPodId())
                        .as("the premise: a1 leads").isEqualTo("a1");

                long logGetsBefore = logGets.stream().mapToLong(ChainLogGets::gets).sum();
                Principal principal = new Principal("cluster-a", "producer", Set.of("logs"));
                int written = 0;
                for (int round = 0; round < ROUNDS; round++) {
                    for (int i = 0; i < nodes.size(); i++) {
                        int base = written;
                        nodes.get(i).ingest().append(principal, "logs", 0, sink -> {
                            for (int r = 0; r < RECORDS_PER_APPEND; r++) {
                                sink.accept(new SegmentRecord("doc-" + (base + r), OpType.INDEX,
                                        OptionalLong.of(1),
                                        ("payload-" + (base + r)).getBytes(StandardCharsets.UTF_8)));
                            }
                        });
                        written += RECORDS_PER_APPEND;
                    }
                }
                int total = written;

                await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200))
                        .until(() -> received.stream().allMatch(got -> records(got) == total));
                List<String> leaderKeys = received.get(0).stream()
                        .map(SubscriptionHub.Push::segmentKey).toList();
                for (int i = 0; i < PODS.size(); i++) {
                    assertContiguousFromZero(PODS.get(i).id(), received.get(i));
                    assertThat(decodedIds(received.get(i), stream))
                            .as("%s decodes every record written", PODS.get(i).id())
                            .hasSize(total)
                            .containsExactlyInAnyOrderElementsOf(java.util.stream.IntStream
                                    .range(0, total).mapToObj(n -> "doc-" + n).toList());
                    assertThat(received.get(i)).extracting(SubscriptionHub.Push::segmentKey)
                            .as("%s received the same segments, in the same order",
                                    PODS.get(i).id())
                            .isEqualTo(leaderKeys);
                }

                long deltas = received.get(0).stream()
                        .map(SubscriptionHub.Push::chainSequence).distinct().count();
                CrossAzBytes leader = bytes.get(0);
                assertThat(leader.crossAzBytes(CrossAzBytes.Transport.DELTA_PUSH))
                        .as("a whole delta never crosses an AZ").isZero();
                assertThat(leader.sameAzBytes(CrossAzBytes.Transport.DELTA_PUSH))
                        .as("a2 is pushed every delta").isPositive();
                assertThat(leader.crossAzBytes(CrossAzBytes.Transport.DELTA_HINT))
                        .as("exactly 24 bytes per (delta, remote AZ)")
                        .isEqualTo(DeltaHintFrame.BYTES * deltas * 2);
                for (int relay : new int[] {B1, C1}) {
                    assertThat(nodes.get(relay).delivery().relayReads())
                            .as("%s, its AZ's relay, reads each delta exactly once",
                                    PODS.get(relay).id())
                            .isEqualTo(deltas);
                }
                assertThat(logGets.stream().mapToLong(ChainLogGets::gets).sum() - logGetsBefore)
                        .as("store GETs for deltas, across every pod: at most one per "
                                + "(delta, remote AZ)")
                        .isLessThanOrEqualTo(deltas * 2);
                assertThat(nodes.get(B2).delivery().relayReads())
                        .as("b2 is not az-b's relay: it reads no delta").isZero();
                assertThat(bytes.get(B1).sameAzBytes(CrossAzBytes.Transport.DELTA_PUSH))
                        .as("b1 pushes each relayed delta on to b2").isPositive();
                assertThat(bytes.get(B1).crossAzBytes(CrossAzBytes.Transport.DELTA_PUSH)
                        + bytes.get(B1).crossAzBytes(CrossAzBytes.Transport.DELTA_HINT))
                        .as("a relay sends nothing across AZs").isZero();

                // Idle: nothing more is pushed or hinted, and every pod's own
                // requests stay within M9's idle bound -- lease renewals only.
                List<Long> pushesAndHints = bytes.stream().map(
                        EveryPodReceivesEveryRecordIT::pushAndHintBytes).toList();
                List<long[]> before = nodes.stream().map(EveryPodReceivesEveryRecordIT::requests)
                        .toList();
                await().during(IDLE).atMost(IDLE.plusSeconds(2))
                        .pollInterval(Duration.ofMillis(500)).untilAsserted(() -> {
                            assertThat(bytes.stream().map(
                                    EveryPodReceivesEveryRecordIT::pushAndHintBytes).toList())
                                    .as("no pod pushes or hints while idle")
                                    .isEqualTo(pushesAndHints);
                        });
                long renewCeiling = IDLE.toSeconds() / 3 + 2; // 3 s renew interval
                for (int i = 0; i < nodes.size(); i++) {
                    long[] after = requests(nodes.get(i));
                    long[] was = before.get(i);
                    assertThat(new long[] {after[0] - was[0], after[1] - was[1],
                            after[2] - was[2], after[3] - was[3], after[4] - was[4]})
                            .as("%s idle: no GET, LIST, stat, delete or non-lease PUT",
                                    PODS.get(i).id())
                            .containsOnly(0L);
                    assertThat(after[5] - was[5]).as("%s idle: lease renewals within M9's bound",
                            PODS.get(i).id()).isLessThanOrEqualTo(renewCeiling);
                }
            } finally {
                for (int i = closing.size() - 1; i >= 0; i--) {
                    try {
                        closing.get(i).close();
                    } catch (Exception ignored) {
                        // best-effort teardown; the assertions above decide the test
                    }
                }
            }
        }
    }

    private static long pushAndHintBytes(CrossAzBytes counts) {
        return counts.sameAzBytes(CrossAzBytes.Transport.DELTA_PUSH)
                + counts.crossAzBytes(CrossAzBytes.Transport.DELTA_PUSH)
                + counts.sameAzBytes(CrossAzBytes.Transport.DELTA_HINT)
                + counts.crossAzBytes(CrossAzBytes.Transport.DELTA_HINT);
    }

    /** Every record id this pod's subscriber can decode from the segments it was handed. */
    private static List<String> decodedIds(List<SubscriptionHub.Push> got, RunKey stream)
            throws java.io.IOException {
        List<String> ids = new ArrayList<>();
        for (SubscriptionHub.Push push : got) {
            var reader = io.github.huyz0.os.biningester.format.SegmentReader.open(push.segment());
            var entry = reader.find(stream).orElseThrow();
            for (SegmentRecord record : reader.read(entry)) {
                ids.add(record.id());
            }
        }
        return ids;
    }

    /** Counts GETs of chain-log objects -- the deltas a relay reads. */
    private static final class ChainLogGets
            implements io.github.huyz0.os.biningester.binstore.BinStore {
        private final io.github.huyz0.os.biningester.binstore.BinStore delegate;
        private final java.util.concurrent.atomic.AtomicLong gets =
                new java.util.concurrent.atomic.AtomicLong();

        ChainLogGets(io.github.huyz0.os.biningester.binstore.BinStore delegate) {
            this.delegate = delegate;
        }

        long gets() {
            return gets.get();
        }

        private void count(String key) {
            if (key.contains("/ctl/log/")) {
                gets.incrementAndGet();
            }
        }

        @Override
        public java.io.InputStream get(String key) throws java.io.IOException {
            count(key);
            return delegate.get(key);
        }

        @Override
        public java.io.InputStream getRange(String key, long start, long endIncl)
                throws java.io.IOException {
            count(key);
            return delegate.getRange(key, start, endIncl);
        }

        @Override
        public Optional<io.github.huyz0.os.biningester.binstore.ObjectStat> stat(String key)
                throws java.io.IOException {
            return delegate.stat(key);
        }

        @Override
        public io.github.huyz0.os.biningester.binstore.Version put(String key,
                io.github.huyz0.os.biningester.binstore.Body body) throws java.io.IOException {
            return delegate.put(key, body);
        }

        @Override
        public Optional<io.github.huyz0.os.biningester.binstore.Version> putIfAbsent(String key,
                io.github.huyz0.os.biningester.binstore.Body body) throws java.io.IOException {
            return delegate.putIfAbsent(key, body);
        }

        @Override
        public Optional<io.github.huyz0.os.biningester.binstore.Version> putIfMatch(String key,
                io.github.huyz0.os.biningester.binstore.Body body,
                io.github.huyz0.os.biningester.binstore.Version expected)
                throws java.io.IOException {
            return delegate.putIfMatch(key, body, expected);
        }

        @Override
        public io.github.huyz0.os.biningester.binstore.MultipartWriter multipart(String key)
                throws java.io.IOException {
            return delegate.multipart(key);
        }

        @Override
        public io.github.huyz0.os.biningester.binstore.ListPage list(String prefix,
                String startAfter, int maxKeys) throws java.io.IOException {
            return delegate.list(prefix, startAfter, maxKeys);
        }

        @Override
        public void delete(List<String> keys) throws java.io.IOException {
            delegate.delete(keys);
        }

        @Override
        public io.github.huyz0.os.biningester.binstore.SignedUrl presign(String key,
                Duration ttl) throws java.io.IOException {
            return delegate.presign(key, ttl);
        }

        @Override
        public io.github.huyz0.os.biningester.binstore.Capabilities capabilities() {
            return delegate.capabilities();
        }

        @Override
        public void close() throws java.io.IOException {
            delegate.close();
        }
    }

    /** gets, lists, stats, deletes, non-lease puts, lease puts. */
    private static long[] requests(Assembly node) {
        var counts = node.storeCounts();
        var puts = node.putPurposeCounts();
        return new long[] {counts.gets(), counts.lists(), counts.stats(), counts.deletes(),
            puts.total() - puts.leasePuts(), puts.leasePuts()};
    }

    /** A push and hint sender whose connections leave from {@code address}. */
    private static io.github.huyz0.os.biningester.http.DeltaFanOut.PeerPost sendingFrom(
            String address) throws Exception {
        java.net.http.HttpClient client = java.net.http.HttpClient.newBuilder()
                .localAddress(java.net.InetAddress.getByName(address))
                .connectTimeout(Duration.ofMillis(500)).build();
        return (endpoint, path, body) -> {
            java.net.http.HttpResponse<Void> response;
            try {
                response = client.send(java.net.http.HttpRequest
                        .newBuilder(java.net.URI.create(endpoint + path))
                        .timeout(Duration.ofSeconds(2))
                        .POST(java.net.http.HttpRequest.BodyPublishers.ofByteArray(body)).build(),
                        java.net.http.HttpResponse.BodyHandlers.discarding());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new java.io.IOException(e);
            }
            if (response.statusCode() / 100 != 2) {
                throw new java.io.IOException("peer answered " + response.statusCode());
            }
        };
    }

    private static long records(List<SubscriptionHub.Push> got) {
        return got.stream().mapToLong(SubscriptionHub.Push::recordCount).sum();
    }

    private static void assertContiguousFromZero(String pod, List<SubscriptionHub.Push> got) {
        long next = 0;
        for (SubscriptionHub.Push push : got) {
            assertThat(push.firstOffset()).as("%s receives in offset order, with no gap", pod)
                    .isEqualTo(next);
            next += push.recordCount();
        }
    }

    private static ServerConfig config(Pod pod, int port, StoreConfig store) {
        return new ServerConfig(pod.id(), pod.az(), "cluster-a", PREFIX, store,
                Duration.ofSeconds(10), Duration.ofSeconds(3),
                "http://" + pod.address() + ":" + port, IngestConfig.defaults("cluster-a"), port,
                "producer", Set.of("logs"));
    }

    private static EndpointSliceView members() {
        StringBuilder endpoints = new StringBuilder();
        for (Pod pod : PODS) {
            if (endpoints.length() > 0) {
                endpoints.append(',');
            }
            endpoints.append("{\"addresses\":[\"").append(pod.address()).append("\"],\"zone\":\"")
                    .append(pod.az()).append("\",\"conditions\":{\"ready\":true},")
                    .append("\"targetRef\":{\"name\":\"").append(pod.id())
                    .append("\",\"uid\":\"uid-").append(pod.id()).append("\"}}");
        }
        EndpointSliceView view = new EndpointSliceView();
        view.apply("{\"type\":\"ADDED\",\"object\":{\"metadata\":{\"name\":\"ingesters\"},"
                + "\"endpoints\":[" + endpoints + "]}}");
        return view;
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
}
