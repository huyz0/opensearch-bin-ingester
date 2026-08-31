// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import binjava.client.ConsumerClient;
import binjava.client.SubscriptionTransport;
import binjava.format.RunKey;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The node-level state every shard on this node shares.
 *
 * <p>WARNING: ONE PER NODE, NOT ONE PER SHARD -- criterion 6, and cost rule R5.
 * {@code createShardConsumer} is called once per shard, so anything constructed
 * there is multiplied by the shard count: 100 shards would open 100 subscriptions
 * and fetch the same segment 100 times. The request rate would then scale with
 * SHARDS, which is precisely what non-negotiable 6 forbids.
 *
 * <p>WARNING: this is why the plugin implements {@code Plugin} as well as
 * {@code IngestionConsumerPlugin} -- {@code createComponents} is the only hook
 * that runs once per node, and the factory has to be handed the result rather
 * than building its own.
 */
public final class NodeSubscriptions implements AutoCloseable {

    private final SubscriptionTransport transport;
    private final Map<RunKey, ConsumerClient> clients = new ConcurrentHashMap<>();
    private final AtomicInteger clientsCreated = new AtomicInteger();
    private final int queueCapacity;

    public NodeSubscriptions(SubscriptionTransport transport, int queueCapacity) {
        this.transport = Objects.requireNonNull(transport, "transport");
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queue capacity must be positive");
        }
        this.queueCapacity = queueCapacity;
    }

    /**
     * The client for one stream, created once and shared.
     *
     * <p>WARNING: computeIfAbsent, so two shards of the same stream on one node
     * share ONE subscription rather than opening two.
     */
    public ConsumerClient clientFor(RunKey key) {
        return clients.computeIfAbsent(key, k -> {
            clientsCreated.incrementAndGet();
            return new ConsumerClient(transport, k, queueCapacity);
        });
    }

    /** How many clients were actually constructed -- criterion 6's counter. */
    public int clientsCreated() {
        return clientsCreated.get();
    }

    public int openClients() {
        return clients.size();
    }

    @Override
    public void close() {
        clients.values().forEach(ConsumerClient::close);
        clients.clear();
    }
}
