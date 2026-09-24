// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.client.Delivery;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.client.InstallationSecret;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opensearch.common.settings.Settings;

class TierTwoChainPollTest {
    @TempDir
    Path directory;

    private static final int NODE_SUBSCRIBERS = 16;

    private static final class CountingTransport implements SubscriptionTransport {
        record Registration(RunKey key, Listener listener) {
        }

        final List<Registration> registrations = new ArrayList<>();
        final AtomicBoolean answers = new AtomicBoolean();

        @Override
        public boolean ingesterAnswers() {
            return answers.get();
        }

        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            Registration registration = new Registration(key, listener);
            registrations.add(registration);
            return () -> registrations.remove(registration);
        }
    }

    @Test
    void pollsOneNextDeltaForSixteenSubscribersAndRemembers404UntilNextInterval()
            throws Exception {
        AtomicBoolean reachable = new AtomicBoolean(false);
        AtomicBoolean readerClosed = new AtomicBoolean(false);
        AtomicInteger gets = new AtomicInteger();
        List<Long> requested = new ArrayList<>();
        AtomicInteger consumed = new AtomicInteger();
        AtomicInteger availableAt = new AtomicInteger(Integer.MAX_VALUE);
        TierTwoChainPoller.DeltaReader reader = (epoch, sequence) -> {
            gets.incrementAndGet();
            requested.add(sequence);
            return gets.get() >= availableAt.get()
                    ? Optional.of(new CommitDelta(sequence, "segment/key",
                            List.of(new RunCommit(new RunKey(
                                    java.util.UUID.fromString("00000000-0000-0000-0000-000000000001"),
                                    0), 1, 0))))
                    : Optional.empty();
        };

        CountingTransport transport = new CountingTransport();
        try (NodeSubscriptions node = new NodeSubscriptions(transport, 32)) {
            node.enableTierTwo(reader, reachable::get,
                    delta -> {
                        node.offerTierTwoDelta(delta);
                        consumed.incrementAndGet();
                    }, () -> readerClosed.set(true));
            for (int shard = 0; shard < NODE_SUBSCRIBERS; shard++) {
                RunKey run = new RunKey(java.util.UUID.nameUUIDFromBytes(
                        ("stream-" + shard).getBytes(java.nio.charset.StandardCharsets.UTF_8)), shard);
                node.clientFor(run);
            }
            // Every live subscription reports the same node cursor.
            for (CountingTransport.Registration registration : transport.registrations) {
                registration.listener().onDelivery(new Delivery(registration.key(),
                        "segment/key", 1, 0, FetchMode.INLINE, new byte[0], null, 0, 40));
            }
            // A delayed event may not move the node cursor backwards.
            CountingTransport.Registration last = transport.registrations.getLast();
            last.listener().onDelivery(new Delivery(last.key(), "segment/key", 1, 0,
                    FetchMode.INLINE, new byte[0], null, 0, 39));

            // Even if all 16 shard callbacks reach the node scheduler in one interval,
            // the shared poller makes exactly one next-slot GET.
            for (int shard = 0; shard < NODE_SUBSCRIBERS; shard++) {
                node.pollTierTwo(1);
            }
            assertThat(gets).hasValue(1);
            assertThat(requested).containsExactly(41L);
            assertThat(consumed).hasValue(0);

        // The next interval may retry the same key once, including after a 404.
            node.pollTierTwo(2);
            assertThat(gets).hasValue(2);
            assertThat(requested).containsExactly(41L, 41L);

        // An available delta is consumed once, and advances the cursor.
            availableAt.set(3);
            node.pollTierTwo(3);
            node.pollTierTwo(3);
            assertThat(gets).hasValue(3);
            assertThat(requested).containsExactly(41L, 41L, 41L);
            assertThat(consumed).hasValue(1);
            node.pollTierTwo(4);
            assertThat(gets).hasValue(3);
            assertThat(node.takeTierTwoDelta().sequence()).isEqualTo(41L);

        // Next sequence is then read once on the next interval.
            node.pollTierTwo(4);
            assertThat(requested).containsExactly(41L, 41L, 41L, 42L);
            assertThat(consumed).hasValue(2);
            assertThat(node.takeTierTwoDelta().sequence()).isEqualTo(42L);

        // When an ingester answers, no node-local read is made.
            reachable.set(true);
            node.pollTierTwo(5);
        assertThat(gets).hasValue(4);
        // DeltaReader exposes only GET; Tier 2 has no LIST or STAT capability.
        }
        assertThat(readerClosed).isTrue();
    }

    @Test
    void issuesNoTierTwoReadAfterTheLastSubscriptionCloses() {
        AtomicInteger gets = new AtomicInteger();
        CountingTransport transport = new CountingTransport();
        RunKey run = new RunKey(java.util.UUID.fromString(
                "00000000-0000-0000-0000-000000000002"), 0);
        try (NodeSubscriptions node = new NodeSubscriptions(transport, 32)) {
            node.enableTierTwo((epoch, sequence) -> {
                gets.incrementAndGet();
                return Optional.empty();
            }, () -> false, ignored -> { }, () -> { });
            node.clientFor(run);
            transport.registrations.getFirst().listener().onDelivery(new Delivery(run,
                    "segment/key", 1, 0, FetchMode.INLINE, new byte[0], null, 0, 40));
            node.release(run);

            node.pollTierTwo(1);

            assertThat(node.openClients()).isZero();
            assertThat(gets).hasValue(0);
        }
    }

    @Test
    void configuredPluginReadsOnlyTheCanonicalNextDeltaFromItsLoopbackReader() throws Exception {
        Path secret = directory.resolve("reader.secret");
        InstallationSecret.create(secret);
        AtomicInteger gets = new AtomicInteger();
        java.util.concurrent.atomic.AtomicReference<String> key =
                new java.util.concurrent.atomic.AtomicReference<>();
        RunKey run = new RunKey(java.util.UUID.fromString(
                "00000000-0000-0000-0000-000000000099"), 0);
        byte[] deltaBody = new CommitDelta(41, "segment/key",
                List.of(new RunCommit(run, 1, 0))).encode();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/object", exchange -> {
            gets.incrementAndGet();
            key.set(exchange.getRequestHeaders().getFirst("X-Bin-Key"));
            exchange.sendResponseHeaders(200, deltaBody.length);
            exchange.getResponseBody().write(deltaBody);
            exchange.close();
        });
        server.start();
        CountingTransport transport = new CountingTransport();
        NodeSubscriptions node = new NodeSubscriptions(transport, 16);
        BinStorePlugin.install(ignored -> node);
        BinStorePlugin plugin = null;
        try {
            assertThatThrownBy(() -> BinStorePlugin.enableTierTwo(Settings.builder()
                    .put(BinStorePlugin.READER_ENDPOINT.getKey(), "http://127.0.0.1:9000")
                    .build(), node))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("must all be configured together");
            Settings settings = Settings.builder()
                    .put(BinStorePlugin.READER_ENDPOINT.getKey(),
                            "http://127.0.0.1:" + server.getAddress().getPort())
                    .put(BinStorePlugin.READER_SECRET_FILE.getKey(), secret.toString())
                    .put(BinStorePlugin.STORE_BUCKET.getKey(), "archive")
                    .put(BinStorePlugin.STORE_PREFIX.getKey(), "cluster")
                    .put("node.name", "tier-two-reader-test")
                    .build();
            plugin = new BinStorePlugin(settings);
            assertThat(plugin.subscriptions()).isSameAs(node);
            AtomicInteger duplicateReaderClosed = new AtomicInteger();
            node.enableTierTwo((epoch, sequence) -> Optional.empty(), () -> false,
                    ignored -> { }, duplicateReaderClosed::incrementAndGet);
            assertThat(duplicateReaderClosed).hasValue(1);
            node.clientFor(run);
            transport.registrations.getFirst().listener().onDelivery(new Delivery(run,
                    "segment/key", 1, 0, FetchMode.INLINE, new byte[0], null, 7, 40));

            node.pollTierTwo(1);

            assertThat(gets).hasValue(1);
            assertThat(key).hasValue("cluster/ctl/log/0/0000000000000007/"
                    + "0000000000000029.delta");
            assertThat(node.takeTierTwoDelta().sequence()).isEqualTo(41L);

            class TrackedBody extends java.io.ByteArrayInputStream {
                boolean closed;
                TrackedBody(byte[] bytes) { super(bytes); }
                @Override public void close() throws IOException {
                    closed = true;
                    super.close();
                }
            }
            TrackedBody tracked = new TrackedBody(deltaBody);
            assertThat(BinStorePlugin.decodeDelta(Optional.of(tracked)).orElseThrow().sequence())
                    .isEqualTo(41L);
            assertThat(tracked.closed).isTrue();
            class FailingBody extends java.io.InputStream {
                boolean closed;
                @Override public int read() throws IOException {
                    throw new IOException("injected read failure");
                }
                @Override public void close() {
                    closed = true;
                }
            }
            FailingBody failed = new FailingBody();
            assertThatThrownBy(() -> BinStorePlugin.decodeDelta(Optional.of(failed)))
                    .isInstanceOf(IOException.class);
            assertThat(failed.closed).isTrue();
            transport.answers.set(true);
            node.pollTierTwo(2);
            assertThat(gets).hasValue(1);
        } finally {
            if (plugin != null) {
                plugin.close();
            } else {
                node.close();
            }
            BinStorePlugin.uninstall();
            server.stop(0);
        }
    }

    @Test
    void refusesInvalidAndInconsistentChainCursors() {
        TierTwoChainPoller.DeltaReader reader = (epoch, sequence) -> Optional.empty();
        assertThatThrownBy(() -> new TierTwoChainPoller(-2, -1, reader, () -> false,
                ignored -> { })).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new TierTwoChainPoller(-1, 0, reader, () -> false,
                ignored -> { })).isInstanceOf(IllegalArgumentException.class);
        List<Long> requested = new ArrayList<>();
        TierTwoChainPoller poller = new TierTwoChainPoller(0, 40,
                (epoch, sequence) -> {
                    requested.add(sequence);
                    return Optional.empty();
                }, () -> false, ignored -> { });
        poller.observeCursor(0, 41);
        poller.poll(1);
        assertThat(requested).containsExactly(42L);

        List<Long> zeroCursorRequest = new ArrayList<>();
        TierTwoChainPoller zeroCursor = new TierTwoChainPoller(-1, -1,
                (epoch, sequence) -> {
                    zeroCursorRequest.add(sequence);
                    return Optional.empty();
                }, () -> false, ignored -> { });
        zeroCursor.observeCursor(0, 0);
        zeroCursor.poll(1);
        assertThat(zeroCursorRequest).containsExactly(1L);
    }

    @Test
    void rejectsDeltaWhoseSequenceDoesNotMatchTheRequestedSlot() {
        AtomicInteger consumed = new AtomicInteger();
        TierTwoChainPoller poller = new TierTwoChainPoller(0, 40,
                (epoch, sequence) -> Optional.of(new CommitDelta(sequence + 1,
                        "segment/key", List.of(new RunCommit(new RunKey(
                                java.util.UUID.fromString(
                                        "00000000-0000-0000-0000-000000000003"), 0), 1, 0)))),
                () -> false,
                ignored -> consumed.incrementAndGet());

        assertThatThrownBy(() -> poller.poll(1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("node-local reader returned the wrong chain delta");
        assertThat(consumed).hasValue(0);
    }
}
