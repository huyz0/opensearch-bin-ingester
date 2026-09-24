// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.huyz0.os.biningester.binstore.backend.S3Fixture;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.server.IngesterNode;
import io.github.huyz0.os.biningester.server.Main;
import io.github.huyz0.os.biningester.server.chaos.ChaosBucket;
import io.github.huyz0.os.biningester.security.Principal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** M9.13: a live gap is replayed from RustFS through the reachable ingester. */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class GapRereadIT {

    private static final String INDEX_NAME = "logs";

    @TempDir
    Path dir;

    @Test
    void gapReplayIsContiguousAndCostsOneGetPerUnindexedSegment() throws Exception {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a T3 suite");
        System.setProperty("aws.accessKeyId", S3Fixture.ACCESS_KEY);
        System.setProperty("aws.secretAccessKey", S3Fixture.SECRET_KEY);
        try (ChaosBucket bucket = ChaosBucket.create()) {
            UUID indexId = UUID.randomUUID();
            RunKey key = new RunKey(indexId, 0);
            String encodedId = Base64Id.encode(indexId);
            Path config = dir.resolve("gap-reread.properties");
            var store = bucket.nodeSettings();
            Files.writeString(config, String.join("\n",
                    "pod.id=gapreread",
                    "pod.az=az-a",
                    "trust.domain=cluster-a",
                    "store.prefix=bins/cluster-a",
                    "store.kind=" + store.get("store.kind"),
                    "store.endpoint=" + store.get("store.endpoint"),
                    "store.region=" + store.get("store.region"),
                    "store.bucket=" + store.get("store.bucket"),
                    "store.path-style=true",
                    "endpoint=http://gapreread:8080",
                    "http.port=0",
                    "ingest.interval-floor=PT5S",
                    "producer.subject=producer-1",
                    "producer.allowed-indices=logs",
                    ""), StandardCharsets.UTF_8);

            try (IngesterNode node = Main.run(config.toString())) {
                node.assembly().catalog().register(new IndexRegistration(encodedId, INDEX_NAME,
                        List.of(), 1, 1, 1, 1));
                try (NodeChannel channel = NodeChannel.open("http://localhost:" + node.port(),
                        Duration.ofMillis(10), Duration.ofMillis(50), Duration.ofSeconds(5));
                        NodeSubscriptions clients = new NodeSubscriptions(
                                new DropOffsetTransport(channel.transport(), 1), 16)) {
                    var client = clients.clientFor(key);
                    NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(
                            clients.transport(), clients,
                            Optional::<List<CatchUpRequestFrame.Stream>>empty);
                    Principal producer = new Principal("cluster-a", "producer-1", Set.of(INDEX_NAME));

                    appendAndFlush(node, producer, key, "doc-0");
                    assertThat(client.readNext(Duration.ofSeconds(10)).orElseThrow().offset())
                            .isZero();
                    appendAndFlush(node, producer, key, "doc-1"); // deliberately dropped live
                    appendAndFlush(node, producer, key, "doc-2");

                    assertThat(client.readNext(Duration.ofSeconds(10)))
                            .as("offset 2 is held rather than indexed ahead of missing offset 1")
                            .isEmpty();
                    // Store counters on the running assembly include the actual replay GETs.
                    long beforeGets = node.assembly().storeCounts().gets();
                    long beforeLists = node.assembly().storeCounts().lists();
                    coordinator.attempt();

                    var rereadMissing = client.readNext(Duration.ZERO).orElseThrow();
                    var rereadHeldTail = client.readNext(Duration.ZERO).orElseThrow();
                    assertThat(rereadMissing.offset()).isEqualTo(1);
                    assertThat(rereadMissing.record().id()).isEqualTo("doc-1");
                    assertThat(rereadHeldTail.offset()).isEqualTo(2);
                    assertThat(rereadHeldTail.record().id()).isEqualTo("doc-2");
                    assertThat(client.readNext(Duration.ZERO)).isEmpty();
                    long getDelta = node.assembly().storeCounts().gets() - beforeGets;
                    long listDelta = node.assembly().storeCounts().lists() - beforeLists;
                    assertThat(getDelta)
                            .as("two not-yet-indexed committed segments cost at most one GET each")
                            .isEqualTo(2);
                    assertThat(listDelta).isZero();
                    assertThat(clients.fetches())
                            .as("the consumer node has no object-store fetcher")
                            .isFalse();
                }
            }
        } finally {
            System.clearProperty("aws.accessKeyId");
            System.clearProperty("aws.secretAccessKey");
        }
    }

    private static void appendAndFlush(IngesterNode node, Principal principal, RunKey key,
            String id) throws Exception {
        node.assembly().ingest().append(principal, INDEX_NAME, key.partitionId(), sink ->
                sink.accept(new SegmentRecord(id, OpType.INDEX, OptionalLong.of(1),
                        id.getBytes(StandardCharsets.UTF_8))));
        node.assembly().flush();
    }

    private static final class DropOffsetTransport implements SubscriptionTransport {
        private final SubscriptionTransport delegate;
        private final long droppedOffset;

        DropOffsetTransport(SubscriptionTransport delegate, long droppedOffset) {
            this.delegate = delegate;
            this.droppedOffset = droppedOffset;
        }

        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            return delegate.subscribe(key, filtering(listener));
        }

        @Override
        public CatchUpResult requestCatchUp(CatchUpRequestFrame request,
                Consumer<io.github.huyz0.os.biningester.format.SubscriptionEvent> lane)
                throws java.io.IOException {
            return delegate.requestCatchUp(request, lane);
        }

        private Listener filtering(Listener listener) {
            return delivery -> {
                if (delivery.firstOffset() != droppedOffset) {
                    listener.onDelivery(delivery);
                }
            };
        }
    }

    private static final class Base64Id {
        static String encode(UUID id) {
            return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
                    ByteBuffer.allocate(16).putLong(id.getMostSignificantBits())
                            .putLong(id.getLeastSignificantBits()).array());
        }
    }
}
