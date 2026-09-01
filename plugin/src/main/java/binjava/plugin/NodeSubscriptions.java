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
    private final Map<RunKey, Entry> clients = new ConcurrentHashMap<>();
    private final AtomicInteger clientsCreated = new AtomicInteger();
    private final int queueCapacity;

    /**
     * ⚠️ REFERENCE-COUNTED. Found by a real node-restart test (M1.17b): closing
     * a shard closes its {@code BinStoreShardConsumer}, which closed the SHARED
     * client directly -- unsubscribing the node's only subscription for that
     * stream. Nothing removed the now-dead entry from {@code clients}, so the
     * next shard (e.g. the SAME shard, recreated when its index reopens) got
     * the identical closed, unsubscribed client back out of {@code clientFor}
     * and never received another delivery. The count is how many shards on
     * this node currently hold this stream's client; it reaches zero only when
     * all of them have released it.
     */
    private static final class Entry {
        final ConsumerClient client;
        int refCount;

        Entry(ConsumerClient client) {
            this.client = client;
        }
    }

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
     * <p>WARNING: `compute`, not `computeIfAbsent` -- an earlier version of
     * this comment described the pre-fix behavior. Every call increments the
     * share count, not just the first, or a second sharer's hold is invisible
     * and {@link #release} can close the client out from under it while that
     * sharer still expects it to be open.
     */
    public ConsumerClient clientFor(RunKey key) {
        // ⚠️ compute, not computeIfAbsent: a caller must ALSO increment the
        // count on every hit, not just on the first, or a second shard's share
        // is invisible and release() closes the client out from under it.
        Entry entry = clients.compute(key, (k, existing) -> {
            Entry e = existing;
            if (e == null) {
                clientsCreated.incrementAndGet();
                e = new Entry(new ConsumerClient(transport, k, queueCapacity));
            }
            e.refCount++;
            return e;
        });
        return entry.client;
    }

    /**
     * One shard's hold on {@code key}'s shared client is released. The
     * underlying subscription is torn down and the entry forgotten only once
     * NOTHING on this node still holds it -- so the next {@link #clientFor}
     * call for the same key opens a FRESH subscription rather than handing
     * back one that is already dead.
     */
    public void release(RunKey key) {
        clients.computeIfPresent(key, (k, entry) -> {
            entry.refCount--;
            if (entry.refCount > 0) {
                return entry;
            }
            entry.client.close();
            return null;
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
        // ⚠️ Node shutdown closes everything regardless of ref count -- there
        // is no "later" for anything still open to be released into.
        clients.values().forEach(e -> e.client.close());
        clients.clear();
    }
}
