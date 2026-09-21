// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import io.github.huyz0.os.biningester.client.HttpSubscriptionTransport;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executor;

/**
 * The node's end of the subscription channel, with its reconnect wired to the
 * registrar (M8.21, M6.15, FR-16).
 *
 * <p>⚠️ **THIS CLASS EXISTS BECAUSE THE TWO HALVES REFER TO EACH OTHER.**
 * {@link IndexRegistrar} is built over a transport, and the transport's
 * reconnect must reach the registrar — so neither can be constructed with the
 * other already in hand. Every earlier attempt to state this as "the caller
 * wires it" left {@code IndexRegistrar.onReconnect()} called by nothing, which
 * is M6.15: the registration is state the INGESTER holds in memory, so a
 * restarted ingester knows no index's shape and every routed write to this
 * node's indices is refused when its wait expires, with nothing naming the
 * cause.
 *
 * <p>⚠️ **THE CYCLE IS CLOSED WITH A HOLDER, AND THE WINDOW IS ARGUED RATHER
 * THAN GUARDED.** The transport is started first — its reader thread polls
 * immediately — so a reconnect can fire before the registrar exists, and the
 * holder answers null for that instant. Dropping it is CORRECT and not merely
 * tolerable: what {@code onReconnect} does is forget what the ingester was
 * believed to have accepted, and a registrar built one statement ago has been
 * handed no cluster state, hosts no index, and has nothing to forget. It is
 * also not yet a listener on anything — {@link #registrar(Executor)} is what a caller
 * hands to the cluster service, after this constructor returns.
 */
public final class NodeChannel implements AutoCloseable {

    /** Builds a transport whose reconnect runs {@code onReconnect}. */
    @FunctionalInterface
    public interface TransportFactory {
        SubscriptionTransport open(Runnable onReconnect);
    }

    private final SubscriptionTransport transport;

    /**
     * Wires a transport this node speaks HTTP over.
     *
     * @param endpoint the ingester this node subscribes to
     */
    public static NodeChannel open(String endpoint, Duration retryFloor,
            Duration retryCeiling, Duration timeout) {
        Objects.requireNonNull(endpoint, "endpoint");
        return new NodeChannel(reconnect -> new HttpSubscriptionTransport(endpoint, reconnect,
                retryFloor, retryCeiling, timeout));
    }

    /**
     * The same over any transport, which is what lets the wiring be asserted
     * without a socket: the factory is handed the reconnect callback, and a
     * test that runs it is asking exactly "does a reconnect reach the
     * registrar".
     */
    public NodeChannel(TransportFactory factory) {
        Objects.requireNonNull(factory, "factory");
        this.transport = Objects.requireNonNull(factory.open(holder::onReconnect), "transport");
    }

    private final Holder holder = new Holder();

    /**
     * The one lock the cycle is closed under.
     *
     * <p>⚠️ **`volatile`, NOT A PLAIN FIELD.** The reconnect runs on the
     * transport's reader thread and the write happens on whichever thread built
     * the channel; without it that thread could go on reading null for ever and
     * the node would never re-push.
     */
    private static final class Holder {
        private volatile IndexRegistrar registrar;

        void onReconnect() {
            IndexRegistrar wired = registrar;
            if (wired != null) {
                wired.onReconnect();
            }
        }

        void wire(IndexRegistrar wired) {
            this.registrar = wired;
        }
    }

    public SubscriptionTransport transport() {
        return transport;
    }

    /**
     * The registrar to hand {@code clusterService.addListener}, created on the
     * first call and wired to this channel's reconnect.
     *
     * <p>⚠️ **CREATED LATE, AND THE EXECUTOR IS WHY.** The registrar's pushes
     * must not run on the cluster applier thread, so it needs the node's
     * generic pool — which does not exist when a deployment builds its
     * transport. A channel built at configuration time and a registrar built at
     * {@code createComponents} time is the real order, and pretending otherwise
     * would mean the plugin building its own executor.
     *
     * <p>⚠️ **IT IS NOT REGISTERED HERE.** A cluster service is the node's and
     * this class holds no reference to one; what this owns is the cycle between
     * the two objects, and owning more would make it a second composition root.
     *
     * <p>⚠️ **ONCE.** A second registrar over the same transport would push
     * every index's shape twice per cluster-state change, and only one of them
     * would be reached by a reconnect.
     */
    public synchronized IndexRegistrar registrar(Executor pusher) {
        if (registrar == null) {
            registrar = new IndexRegistrar(transport, pusher);
            holder.wire(registrar);
        }
        return registrar;
    }

    private IndexRegistrar registrar;

    /**
     * ⚠️ **CLOSES THE TRANSPORT ONLY WHERE IT HAS A CLOSE.**
     * {@code SubscriptionTransport} has none — a consumer closes its
     * subscription handle — so this reaches the concrete one, which is the
     * process's handle on the reader threads.
     */
    @Override
    public void close() {
        if (transport instanceof HttpSubscriptionTransport http) {
            http.close();
        }
    }
}
