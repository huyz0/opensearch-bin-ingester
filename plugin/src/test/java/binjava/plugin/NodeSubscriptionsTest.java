// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.client.Delivery;
import binjava.client.SubscriptionTransport;
import binjava.format.RunKey;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * Criterion 6: ONE subscription per NODE, not per shard.
 *
 * <p>WARNING: the factory is called once per shard, so anything built there is
 * multiplied by the shard count. With 100 shards that is 100 subscriptions and
 * the same segment fetched 100 times -- a request rate scaling with SHARDS,
 * which non-negotiable 6 forbids outright.
 */
class NodeSubscriptionsTest {

    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");

    private static final class CountingTransport implements SubscriptionTransport {
        final AtomicInteger subscribeCalls = new AtomicInteger();
        final List<Listener> listeners = new CopyOnWriteArrayList<>();

        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            subscribeCalls.incrementAndGet();
            listeners.add(listener);
            return () -> listeners.remove(listener);
        }
    }

    @Test
    void oneHundredShardsOfOneStreamOpenASingleSubscription() {
        CountingTransport transport = new CountingTransport();
        try (NodeSubscriptions node = new NodeSubscriptions(transport, 16)) {
            RunKey key = new RunKey(A, 0);
            for (int shard = 0; shard < 100; shard++) {
                node.clientFor(key);
            }
            // WARNING: the counter, not the map size -- a factory that built a
            // client and then discarded it would leave the map at 1 while having
            // opened 100 subscriptions.
            assertThat(node.clientsCreated()).isEqualTo(1);
            assertThat(transport.subscribeCalls.get()).isEqualTo(1);
            assertThat(node.openClients()).isEqualTo(1);
        }
    }

    @Test
    void distinctStreamsStillGetTheirOwnSubscriptions() {
        CountingTransport transport = new CountingTransport();
        try (NodeSubscriptions node = new NodeSubscriptions(transport, 16)) {
            for (int shard = 0; shard < 100; shard++) {
                node.clientFor(new RunKey(A, shard));
            }
            // WARNING: sharing must be by STREAM, not global. One client for
            // everything would deliver every partition's records to every shard.
            assertThat(node.clientsCreated()).isEqualTo(100);
        }
    }

    @Test
    void theSameClientInstanceIsHandedOutEachTime() {
        CountingTransport transport = new CountingTransport();
        try (NodeSubscriptions node = new NodeSubscriptions(transport, 16)) {
            RunKey key = new RunKey(A, 3);
            assertThat(node.clientFor(key)).isSameAs(node.clientFor(key));
        }
    }

    @Test
    void closingTheNodeClosesEverySubscription() {
        CountingTransport transport = new CountingTransport();
        NodeSubscriptions node = new NodeSubscriptions(transport, 16);
        node.clientFor(new RunKey(A, 0));
        node.clientFor(new RunKey(A, 1));
        assertThat(transport.listeners).hasSize(2);
        node.close();
        // WARNING: a node that shuts down without unsubscribing leaves the
        // ingester pushing into dead consumers for the life of the cluster.
        assertThat(transport.listeners).isEmpty();
        assertThat(node.openClients()).isZero();
    }

    @Test
    void aNonPositiveQueueCapacityIsRefused() {
        assertThatThrownBy(() -> new NodeSubscriptions(new CountingTransport(), 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void thePluginRegistersItselfUnderItsOwnType() {
        CountingTransport transport = new CountingTransport();
        try (NodeSubscriptions node = new NodeSubscriptions(transport, 16)) {
            BinStorePlugin plugin = new BinStorePlugin(node);
            assertThat(plugin.getType()).isEqualTo("BINSTORE");
            // WARNING: the key must equal getType(), or index.ingestion_source.type
            // resolves to nothing and the index silently never ingests.
            assertThat(plugin.getIngestionConsumerFactories())
                    .containsOnlyKeys(plugin.getType());
            assertThat(plugin.getIngestionConsumerFactories().get("BINSTORE"))
                    .isInstanceOf(BinStoreConsumerFactory.class);
        }
    }

    @Test
    void theFactoryParsesPointersItsConsumersProduce() {
        CountingTransport transport = new CountingTransport();
        try (NodeSubscriptions node = new NodeSubscriptions(transport, 16)) {
            BinStoreConsumerFactory factory = new BinStoreConsumerFactory(node);
            assertThat(factory.parsePointerFromString(new BinStoreOffset(4_242).asString()))
                    .isEqualTo(new BinStoreOffset(4_242));
        }
    }
}
