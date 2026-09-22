// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.core.index.Index;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Tells the ingester the shape of every index this node ingests (M6.7, FR-16,
 * ADR-0015 § 2).
 *
 * <p>⚠️ THE INGESTER CANNOT ASK. It holds no cluster state and no OpenSearch
 * client, and ADR-0015 records why it must not grow one: a service that queried
 * the cluster for an index's shard count would need an endpoint, a credential
 * and a retry policy per cluster, and would ask once per RECORD on the path
 * that matters. So the node pushes, over the subscription it already holds --
 * no new endpoint, no new credential, no unauthenticated surface.
 *
 * <p>⚠️ NODE-SCOPED, NOT SHARD-SCOPED, which is the same reason
 * {@link NodeSubscriptions} is. {@code createShardConsumer} runs once per
 * SHARD, so a listener built there pushes once per shard per change: eight
 * copies of one index's shape for an eight-shard node, and every one of them
 * identical. Counted at K = 1 and K = 8 by {@code RegistrationPushTest},
 * because no other criterion in M6 observes the push at all.
 *
 * <p>⚠️ ON CHANGE, NEVER ON A TIMER. A poll puts a message rate on an idle
 * cluster, which is NFR-2's shape one layer up even though no object-store
 * request is involved. What makes that safe is that the push is IDEMPOTENT and
 * DIFFED: the registration for an index is computed from the state and sent
 * only when it differs from what was last accepted, so a cluster-state change
 * that touches nothing this node ingests costs nothing.
 *
 * <p>⚠️ A FAILED PUSH IS RETRIED, and this is the one case that satisfies every
 * count above while being badly wrong: one throw, never retried, and every
 * index on that node falls out of the ingester's pending pool at
 * {@code pendingTimeout} -- a sustained stream of refused writes for a fault
 * that lasted one message. A failure is retried {@value #ATTEMPTS} times
 * immediately and then CARRIED, so the next cluster-state change tries it
 * again.
 *
 * <p>⚠️ IT HOLDS NO CLOCK AND NO SOCKET: the transport is the seam
 * (non-negotiable 7), and a `plugin` class may not name {@code io.github.huyz0.os.biningester.binstore}
 * at all (ADR-0023).
 */
public final class IndexRegistrar implements ClusterStateListener {

    /**
     * How many times one push is attempted before it is carried to the next
     * cluster-state change.
     *
     * <p>⚠️ IMMEDIATE RATHER THAN SCHEDULED, because a scheduler is a timer and
     * a timer is what this class exists not to have. Three attempts covers the
     * fault this is for -- one message lost to a connection that is already
     * being re-established -- and a longer outage is covered by the carry,
     * which costs nothing while the cluster is quiet.
     */
    static final int ATTEMPTS = 3;

    private static final Logger LOG = LogManager.getLogger(IndexRegistrar.class);

    /** The setting that says an index is ingested by THIS plugin. */
    static final String SOURCE_TYPE = "index.ingestion_source.type";

    private final SubscriptionTransport transport;

    /**
     * Where the pushes actually run.
     *
     * <p>⚠️ NOT THE CLUSTER APPLIER THREAD, which is what
     * {@code clusterService.addListener} hands this class. That thread applies
     * every cluster-state update on the node -- allocation, mappings, the ack
     * to the cluster manager -- and {@link #push} makes up to
     * {@value #ATTEMPTS} network calls per due index with no timeout of its
     * own. Forty indices due in one event against an unreachable ingester is
     * 120 serial attempts with the whole node's cluster-state application
     * stopped behind them. In production this is the node's generic thread
     * pool; a test passes a direct executor and keeps every case
     * deterministic.
     */
    private final Executor pusher;

    /**
     * The last state seen, so a RECONNECT can re-push without waiting for the
     * cluster to change.
     *
     * <p>⚠️ THE INGESTER'S CATALOG IS IN MEMORY AND DIES WITH IT. After it
     * restarts, this node's {@code accepted} still holds every shape it ever
     * pushed, so the diff finds nothing due and a steady cluster never sends
     * another message -- every routed write on this node then pends and is
     * refused at {@code pendingTimeout}, indefinitely, against an ingester that
     * is perfectly healthy. {@link #onReconnect()} is how that is recovered.
     */
    private ClusterState lastState;

    /**
     * Serialises the pushes, and is NOT the lock that guards {@link #accepted}.
     *
     * <p>⚠️ TWO LOCKS, FOR TWO DIFFERENT REASONS, and round 2 measured what one
     * costs. Holding the state monitor across {@code transport.register} blocks
     * the applier thread on the MONITOR instead of on the socket -- a probe
     * with a parked {@code register} showed a second {@code clusterChanged}
     * failing to return within two seconds -- so every cluster-state update on
     * the node stops behind a hanging connect just the same.
     *
     * <p>⚠️ AND {@code threadPool.generic()} IS MULTI-THREADED WITH NO ORDERING
     * BETWEEN TASKS. Two events during a rollover -- one where
     * {@code logs-000001} claims the alias and one where it does not -- can be
     * pushed out of order, and the ingester's catalog takes whichever arrived
     * LAST. This lock is what makes the newest state the one that wins, with
     * {@link #drain} re-deriving from {@link #lastState} rather than from the
     * event it was submitted for.
     */
    private final ReentrantLock pushing = new ReentrantLock();

    /**
     * Whether a push task is queued or running, and whether the state moved
     * again while it ran -- the whole of the coalescing.
     *
     * <p>⚠️ WITHOUT IT, A FAULT MULTIPLIES. Nothing is recorded as accepted
     * until a push succeeds, so while the ingester is unreachable EVERY index
     * is due on EVERY cluster-state publication -- a few a second on a busy
     * cluster. Each publication then queues a task on {@code generic()}, an
     * unbounded scaling pool shared with peer recovery, shard-store fetch and
     * snapshots, and each queued task repeats every attempt for every index.
     * The message rate at the transport would scale with publication rate x
     * indices, on exactly the path where the far end is already in trouble.
     * With it, one task runs at a time and the state it re-derives from is
     * whatever is newest when it gets there.
     */
    private boolean pushQueued;

    private boolean stateMovedAgain;

    /**
     * What the transport TOOK without throwing, by index UUID -- never what
     * was merely computed. ⚠️ "Accepted" here means the push returned, not
     * that the ingester acknowledged anything: {@code register} carries no ack.
     *
     * <p>⚠️ A REGISTRATION IS RECORDED ONLY AFTER THE PUSH SUCCEEDS. Recording
     * it first makes a failed push permanent: the diff below then finds the
     * shape unchanged forever and never sends it again, which is the
     * never-retried failure one step more subtle.
     */
    private final Map<String, IndexRegistration> accepted = new LinkedHashMap<>();

    private final AtomicInteger pushes = new AtomicInteger();
    private final AtomicInteger failures = new AtomicInteger();

    public IndexRegistrar(SubscriptionTransport transport, Executor pusher) {
        this.transport = Objects.requireNonNull(transport, "transport");
        this.pusher = Objects.requireNonNull(pusher, "pusher");
    }

    /**
     * Re-pushes every index this node hosts, because the ingester may have
     * forgotten them (M6.7, ADR-0015 § 2's "on connect").
     *
     * <p>⚠️ THE CONNECT HALF OF THE CRITERION, and it cannot be served by the
     * change half: a reconnect is not a cluster-state change, and on a steady
     * cluster no change may follow it for hours. What is FORGOTTEN here is
     * this node's memory of what the ingester accepted -- after which the
     * ordinary diff finds every hosted index due and sends it.
     *
     * <p>⚠️ NOTHING CALLS THIS IN PRODUCTION YET, and saying so is the point:
     * the call site is the transport's reconnect, and no production transport
     * exists (M5.6e, owned by M8). M6.15 is the row that wires it. A node that
     * has seen no cluster state yet re-pushes nothing, which is correct -- it
     * hosts nothing to push.
     */
    public void onReconnect() {
        boolean anything;
        synchronized (this) {
            accepted.clear();
            anything = lastState != null;
        }
        if (!anything) {
            return;
        }
        synchronized (this) {
            if (pushQueued) {
                stateMovedAgain = true;
                return;
            }
            pushQueued = true;
        }
        pusher.execute(this::drain);
    }

    /**
     * ⚠️ THE MONITOR GUARDS THE MEMORY AND NOTHING ELSE. {@code accepted} is
     * touched from three threads for real -- the applier diffs, the pool
     * records what was accepted, and {@link #onReconnect()} clears -- and two
     * interleaved diffs against a half-updated map send an index's shape twice
     * or not at all. What the monitor must NOT cover is
     * {@code transport.register}; see {@link #pushing}.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        Objects.requireNonNull(event, "event");
        synchronized (this) {
            lastState = event.state();
        }
        // ⚠️ FORGETTING HAPPENS HERE, on the applier thread, because it is
        // pure memory and because an index that moved AWAY makes nothing due:
        // done in the push task instead, a shard leaving and coming back is
        // never forgotten at all, and the return diffs as unchanged against a
        // stale memory.
        forgetWhatThisNodeNoLongerHosts(event.state());
        // ⚠️ NOTHING DUE MEANS NO TASK AT ALL, not an empty one. Cluster state
        // is published constantly on a busy cluster, and a task per
        // publication is a poll wearing an event's clothes -- the shape this
        // class exists not to have. The check reads memory and touches no
        // socket, so it is safe on the applier thread; the push is not, and is
        // not done here.
        if (due(event.state()).isEmpty()) {
            return;
        }
        synchronized (this) {
            if (pushQueued) {
                // ⚠️ NO SECOND TASK. The one already queued re-derives from
                // `lastState`, which this event has just updated, so it will
                // push what this event wanted -- and this flag is what makes
                // it look again after it finishes.
                stateMovedAgain = true;
                return;
            }
            pushQueued = true;
        }
        pusher.execute(this::drain);
    }

    /**
     * ⚠️ IT RE-DERIVES FROM {@link #lastState}, never from the event that
     * submitted it, and holds {@link #pushing} while it does. Two tasks on a
     * multi-threaded pool would otherwise race, and the LOSER's shape is the
     * one the ingester keeps -- M6.3's catalog takes whichever push arrived
     * last. Re-deriving also collapses a burst of changes into one push of the
     * newest shape.
     */
    private void drain() {
        pushing.lock();
        try {
            while (true) {
                ClusterState state;
                synchronized (this) {
                    stateMovedAgain = false;
                    state = lastState;
                }
                if (state != null) {
                    for (IndexRegistration registration : due(state)) {
                        // ⚠️ OUTSIDE EVERY LOCK THIS CLASS HOLDS. `push` is up
                        // to three network calls with no timeout of its own;
                        // what is guarded is the memory of what was accepted,
                        // not the call.
                        push(registration);
                    }
                }
                synchronized (this) {
                    // ⚠️ THE FLAG IS CLEARED ONLY HERE, under the same monitor
                    // a new event sets it under. Clearing it before the last
                    // push would let an event that arrived DURING the push
                    // queue a second task; clearing it after, without the
                    // re-check, would drop that event's change until the next
                    // one.
                    if (!stateMovedAgain) {
                        pushQueued = false;
                        return;
                    }
                }
            }
        } finally {
            pushing.unlock();
        }
    }

    /**
     * ⚠️ A STATE WITH NO ROUTING NODE FOR THIS NODE FORGETS NOTHING. Treating
     * it as "hosts no index" empties {@link #accepted}, and the next ordinary
     * state re-pushes every index at once -- a burst caused by a transient
     * view, on the node least able to absorb it.
     *
     * <p>⚠️ AND AN INDEX THIS NODE NO LONGER HOSTS IS FORGOTTEN, not
     * unregistered: there is no unregister frame and there must not be one --
     * another node may still host the index, and a "forget this" message
     * racing that node's push empties the ingester's catalog for an index that
     * is very much alive. Forgetting locally is what makes a re-addition push
     * again rather than diff as unchanged.
     */
    private synchronized void forgetWhatThisNodeNoLongerHosts(ClusterState state) {
        RoutingNode node = localRoutingNode(state);
        if (node == null) {
            return;
        }
        accepted.keySet().retainAll(hostedRegistrations(state, node).keySet());
    }

    /** What this node hosts and the ingester has not accepted in this shape. */
    private synchronized List<IndexRegistration> due(ClusterState state) {
        RoutingNode node = localRoutingNode(state);
        if (node == null) {
            return List.of();
        }
        List<IndexRegistration> due = new ArrayList<>();
        for (Map.Entry<String, IndexRegistration> entry
                : hostedRegistrations(state, node).entrySet()) {
            if (entry.getValue().equals(accepted.get(entry.getKey()))) {
                continue;
            }
            due.add(entry.getValue());
        }
        return due;
    }

    private static RoutingNode localRoutingNode(ClusterState state) {
        String localNodeId = state.nodes().getLocalNodeId();
        return localNodeId == null ? null : state.getRoutingNodes().node(localNodeId);
    }

    private void push(IndexRegistration registration) {
        RuntimeException last = null;
        for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
            try {
                transport.register(registration);
                pushes.incrementAndGet();
                // ⚠️ RECORDED ONLY AFTER THE PUSH SUCCEEDS, which is also
                // the whole retry mechanism: a registration that never
                // succeeded is not in `accepted`, so the next cluster-state
                // change diffs it as new and sends it again. A separate
                // carry-forward set would be a second copy of that fact.
                synchronized (this) {
                    accepted.put(registration.indexUuid(), registration);
                }
                return;
            } catch (RuntimeException e) {
                failures.incrementAndGet();
                last = e;
            }
        }
        // ⚠️ NOT RECORDED AS ACCEPTED, which is what carries it: the next
        // cluster-state change finds no accepted shape for this index and
        // sends it again. Until one arrives the ingester's pending pool is
        // what holds this index's writes, which is the window ADR-0015 § 3
        // buys.
        if (last != null) {
            LOG.warn("could not push the registration for index " + registration.indexName()
                    + " after " + ATTEMPTS + " attempts; it will be retried on the next "
                    + "cluster-state change. Until then this index's routed writes wait "
                    + "in the ingester's pending pool and are refused when it expires", last);
        }
    }

    /**
     * ⚠️ THE STATE SAYS WHICH NODE IS LOCAL, so this class is handed no node
     * id. Taking one at construction means reading
     * {@code clusterService.localNode()} from {@code createComponents}, which
     * runs before the node has joined anything -- a lifecycle question this
     * does not have to have, since every event carries the answer.
     */
    private Map<String, IndexRegistration> hostedRegistrations(ClusterState state,
            RoutingNode node) {
        Map<String, IndexRegistration> wanted = new LinkedHashMap<>();
        for (ShardRouting shard : node) {
            Index index = shard.index();
            // ⚠️ ONE REGISTRATION PER INDEX, HOWEVER MANY SHARDS OF IT THIS
            // NODE HOLDS -- the K = 8 half of criterion 12. It is the KEY that
            // buys it: this loop walks SHARDS, and keying what it collects by
            // the index's UUID is what turns eight shards into one message.
            // The skip below only avoids re-deriving a shape already derived;
            // removing it changes no count.
            if (wanted.containsKey(index.getUUID())) {
                continue;
            }
            IndexMetadata metadata = state.metadata().index(index);
            if (metadata == null || !ours(metadata)) {
                continue;
            }
            wanted.put(index.getUUID(), registrationOf(metadata));
        }
        return wanted;
    }

    /**
     * ⚠️ ONLY THE INDICES THIS PLUGIN INGESTS. A node also hosts ordinary
     * indices, and pushing their shapes would put indices the ingester will
     * never be written to into its catalog -- and would make the push count
     * scale with a number that has nothing to do with us.
     */
    private static boolean ours(IndexMetadata metadata) {
        String type = metadata.getSettings().get(SOURCE_TYPE);
        return type != null && type.equalsIgnoreCase(BinStorePlugin.TYPE);
    }

    private static IndexRegistration registrationOf(IndexMetadata metadata) {
        // ⚠️ SORTED, because the registration is DIFFED by equality and its
        // aliases are a List: two states listing the same aliases in a
        // different order would otherwise read as a change and re-push. The
        // order OpenSearch happens to give them is not a contract.
        Set<String> aliases = metadata.getAliases().keySet();
        return new IndexRegistration(
                metadata.getIndexUUID(),
                metadata.getIndex().getName(),
                aliases.stream().sorted().toList(),
                metadata.getNumberOfShards(),
                metadata.getRoutingNumShards(),
                metadata.getRoutingFactor(),
                metadata.getRoutingPartitionSize());
    }

    /**
     * How many registrations this node has handed to the transport without it
     * throwing.
     *
     * <p>⚠️ DELIVERED, NOT ACKNOWLEDGED. {@code register} returns void and
     * carries no ack, so a transport that accepted the bytes and lost them
     * counts here exactly like one that delivered them. What recovers that is
     * {@link #onReconnect()}, not this number.
     */
    public int pushes() {
        return pushes.get();
    }

    /** How many push ATTEMPTS have failed, including retried ones. */
    public int pushFailures() {
        return failures.get();
    }

    /** How many indices this node has a live registration for. */
    public synchronized int registeredIndices() {
        return accepted.size();
    }
}
