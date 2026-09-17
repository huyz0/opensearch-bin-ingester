// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import binjava.client.ConsumerClient;
import binjava.client.SubscriptionTransport;
import binjava.format.RunKey;
import java.util.List;
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

    private final Map<RunKey, Entry> clients = new ConcurrentHashMap<>();
    private final AtomicInteger clientsCreated = new AtomicInteger();
    private final int queueCapacity;
    private final SubscriptionTransport transport;

    /**
     * ⚠️ ONE SEGMENT SOURCE FOR THE WHOLE NODE (M5.45h), or none.
     * {@code direct} hands every run of a segment its OWN delivery carrying
     * the same grant, so a source owned per client fetches once per RUN --
     * ~400 whole-object GETs for one 8 MiB segment on a catch-up node, which
     * non-negotiable 6 forbids and which no meter in the ingester can see.
     * Sharing the instance is what makes the fetch per (node, segment), and
     * {@link NodeSegmentSource} is the wrapper that does the sharing.
     *
     * <p>⚠️ NULL IS THE DEPLOYMENT THAT DOES NOT ENABLE {@code direct}, which
     * is every one that ships in M5 (ADR-0044 (a)). A `direct` delivery then
     * reaches {@code ConsumerClient} with no source and throws where an
     * operator sees it, rather than emptying a window quietly.
     */
    private final binjava.client.SegmentSource nodeSegmentSource;

    /**
     * ⚠️ ONE SUBSCRIBER FOR THE WHOLE NODE (M5.62), and the identity is the
     * point. {@code SubscriptionHub} groups by {@code Subscriber} IDENTITY, so
     * a node registering one listener for every key it holds is handed a
     * segment ONCE however many runs of it this node holds; registering per
     * key made a node holding 178 runs 178 consumers to the hub and 178 copies
     * of the bytes. M5.40a bought that property at the hub and nothing opted
     * into it until this field.
     *
     * <p>⚠️ IT ROUTES BY {@code Delivery.key()} rather than by which
     * subscription delivered it, because with one subscription there is no
     * "which". A delivery for a key this node no longer holds is dropped: a
     * shard can close between the push leaving the ingester and the listener
     * running, and the alternative -- resurrecting the entry -- would create a
     * client nobody holds.
     */
    private final SubscriptionTransport.Listener nodeListener = delivery -> {
        Entry entry = clients.get(delivery.key());
        if (entry != null) {
            entry.client.deliver(delivery);
        }
    };

    /**
     * ⚠️ ONE REGISTRATION FOR THE NODE, EXTENDED A KEY AT A TIME. It is built
     * empty in the constructor and never rebuilt: {@code add} and {@code remove}
     * touch one key and leave every other registration untouched, so no window
     * exists in which a stream is registered twice (duplicate deliveries, which
     * nothing on the consumer path dedups) or not at all (dropped ones). An
     * earlier version rebuilt the whole subscription whenever the key set moved
     * and had both windows depending on the order of the two calls.
     */
    private final SubscriptionTransport.MultiSubscription subscription;

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
        this(transport, queueCapacity, null);
    }

    /**
     * The same over a wired channel, which is how a real deployment builds
     * this.
     *
     * <p>⚠️ **THE CHANNEL IS KEPT SO THE REGISTRAR REACHES THE NODE.** The
     * plugin installs a listener from what this object holds; a deployment that
     * handed only the transport would get a registrar built here, with no
     * reconnect wired to it — which is precisely the two-milestone gap M6.15
     * records.
     */
    public NodeSubscriptions(NodeChannel channel, int queueCapacity) {
        this(Objects.requireNonNull(channel, "channel").transport(), queueCapacity, null);
        this.channel = channel;
    }

    private NodeChannel channel;

    /** The channel this was built over, or {@code null} for a bare transport. */
    public NodeChannel channel() {
        return channel;
    }

    /**
     * @param nodeSegmentSource the ONE source every client on this node
     *     fetches through, or {@code null} where {@code direct} is not enabled
     */
    public NodeSubscriptions(SubscriptionTransport transport, int queueCapacity,
            binjava.client.SegmentSource nodeSegmentSource) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.nodeSegmentSource = nodeSegmentSource;
        if (queueCapacity <= 0) {
            throw new IllegalArgumentException("queue capacity must be positive");
        }
        this.queueCapacity = queueCapacity;
        this.subscription = transport.subscribe(List.of(), nodeListener);
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
        //
        // ⚠️ AND THE REGISTRATION HAPPENS INSIDE IT, which is where the
        // subscribe always was before M5.62 moved it out -- it used to be
        // `ConsumerClient`'s constructor, called from this mapping function.
        // Doing it after `compute` returns makes the entry and the registration
        // two steps a release can interleave with: `release` drops the last
        // holder and is descheduled before its `remove`, this method creates a
        // fresh entry and `add`s a key that is still registered (a no-op), and
        // the release then unsubscribes a stream a live client holds. Nothing
        // re-enters this branch afterwards, so that stream is blind for the
        // life of the node -- the index-reopen shape the `Entry` javadoc
        // records from M1.17b, as a race instead of a bug.
        //
        // ⚠️ THE MAPPING FUNCTION THEREFORE CALLS OUT, and the rule that makes
        // that safe is that it must not call back into THIS key: a
        // `ConcurrentHashMap` refuses a recursive update on the same key with
        // an `IllegalStateException` rather than deadlocking, and
        // `nodeListener` reads other keys, which a bin lock does not hold.
        Entry entry = clients.compute(key, (k, existing) -> {
            if (existing != null) {
                existing.refCount++;
                return existing;
            }
            // ⚠️ THE FED CONSTRUCTOR, holding no subscription of its own:
            // `nodeListener` is what this node is subscribed with, and a client
            // that closed a subscription would close every other run's with it.
            ConsumerClient client = new ConsumerClient(k, queueCapacity, nodeSegmentSource);
            try {
                subscription.add(k);
            } catch (RuntimeException failedToSubscribe) {
                // ⚠️ RETURNING NULL LEAVES NO ENTRY, which is the rollback. An
                // entry left behind is a client nobody is subscribed for: the
                // caller never received it so never releases it, and the next
                // call for this key would find it, skip this branch, and that
                // stream would receive nothing for the life of the node.
                client.close();
                throw failedToSubscribe;
            }
            // ⚠️ COUNTED ONLY ONCE THE REGISTRATION STANDS, so criterion 6's
            // counter does not climb on a transport that is failing to
            // subscribe.
            clientsCreated.incrementAndGet();
            Entry e = new Entry(client);
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
            // ⚠️ INSIDE THE COMPUTE, for the reason `clientFor` gives at
            // length: the entry and the registration must move together, or a
            // `clientFor` interleaving between them unsubscribes a stream a
            // live client holds.
            //
            // ⚠️ AND ONLY THE LAST HOLDER GETS HERE. Every release before it
            // changes nothing the subscription can see.
            // ⚠️ NO ROLLBACK, AND THE ABSENCE IS THE DESIGN. A throw here
            // leaves the entry alive with `refCount` at 0, which is the state
            // that RECOVERS: the next `clientFor` raises it to 1 and that
            // shard's own release drops it back to 0 and tries the unsubscribe
            // again. Round 4 added an `entry.refCount++` rollback here and
            // review measured it as the opposite of what its comment claimed --
            // it restores a hold NOBODY owns, so every later open/close pair is
            // balanced around a phantom (1 -> 2 -> 1), the `refCount > 0` early
            // return fires forever, and the unsubscribe is never retried for
            // the life of the node. The hub then counts this node as a consumer
            // of every segment carrying this run and keeps pushing it bytes --
            // the NFR-5 waste this whole row removes.
            subscription.remove(k);
            entry.client.close();
            return null;
        });
    }

    /**
     * The transport this node's subscriptions travel on, so the registration
     * push travels on it too (M6.7, ADR-0015 § 2).
     *
     * <p>⚠️ EXPOSED RATHER THAN DUPLICATED. Handing {@link IndexRegistrar} its
     * own transport would be a second connection and a second credential per
     * node, for one message per index per cluster-state change -- the second
     * endpoint ADR-0015 exists to avoid.
     */
    public SubscriptionTransport transport() {
        return transport;
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
        // ⚠️ THE SUBSCRIPTION GOES LAST AND IS THE ONLY THING THAT UNSUBSCRIBES.
        // The clients hold none of their own since M5.62, so closing them frees
        // their queues and leaves this node registered until this line runs.
        subscription.close();
    }
}
