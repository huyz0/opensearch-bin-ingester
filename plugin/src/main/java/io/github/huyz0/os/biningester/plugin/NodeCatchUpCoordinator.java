// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Coordinates one node-wide replay after every assigned local shard is readable. */
final class NodeCatchUpCoordinator {

    private static final Logger LOG = LogManager.getLogger(NodeCatchUpCoordinator.class);
    private final SubscriptionTransport transport;
    private final NodeSubscriptions clients;
    private final Supplier<Optional<List<CatchUpRequestFrame.Stream>>> snapshot;
    private final Map<io.github.huyz0.os.biningester.format.RunKey, Long> deliveredUpTo =
            new java.util.HashMap<>();
    private final Set<io.github.huyz0.os.biningester.format.RunKey> catchUpClients =
            new HashSet<>();
    private CatchUpRequestFrame request;
    private boolean done;

    NodeCatchUpCoordinator(SubscriptionTransport transport, NodeSubscriptions clients,
            Supplier<Optional<List<CatchUpRequestFrame.Stream>>> snapshot) {
        this.transport = java.util.Objects.requireNonNull(transport, "transport");
        this.clients = java.util.Objects.requireNonNull(clients, "clients");
        this.snapshot = java.util.Objects.requireNonNull(snapshot, "snapshot");
    }

    /** Runs on the generic pool; a pending position read is retried by the caller's schedule. */
    synchronized void attempt() {
        if (done) {
            return;
        }
        try {
            if (request == null) {
                Optional<List<CatchUpRequestFrame.Stream>> ready = snapshot.get();
                if (ready.isEmpty() || ready.get().isEmpty()) {
                    return;
                }
                List<CatchUpRequestFrame.Stream> streams = List.copyOf(ready.get());
                if (streams.size() > CatchUpRequestFrame.MAX_STREAMS) {
                    throw new CatchUpRequestFrame.StreamLimitException(streams.size(),
                            CatchUpRequestFrame.MAX_STREAMS);
                }
                if (!clients.holdsAll(streams.stream()
                        .map(CatchUpRequestFrame.Stream::key).toList())) {
                    return;
                }
                request = new CatchUpRequestFrame(UUID.randomUUID(), streams);
                streams.forEach(stream -> deliveredUpTo.put(stream.key(), stream.batchStart()));
            }
            Set<io.github.huyz0.os.biningester.format.RunKey> requested = new HashSet<>();
            request.streams().forEach(stream -> requested.add(stream.key()));
            SubscriptionTransport.CatchUpResult result = transport.requestCatchUp(request,
                    event -> accept(event, requested));
            if (result == SubscriptionTransport.CatchUpResult.UNSUPPORTED) {
                // Old peers continue on the already-open live subscriptions.
                done = true;
            } else {
                clients.completeCatchUp(request.requestId(), catchUpClients);
                done = true;
            }
        } catch (CatchUpRequestFrame.StreamLimitException unsupportedSize) {
            LOG.error("node catch-up snapshot exceeds the supported stream limit; "
                    + "catch-up cannot proceed", unsupportedSize);
            done = true;
        } catch (IOException | RuntimeException failed) {
            // Keep the exact snapshot and request id. A partial response may
            // already be in bounded client lanes; a matching retry remains one
            // logical catch-up exchange.
            LOG.warn("node catch-up exchange failed; it will retry at the progress interval", failed);
        }
    }

    private void accept(SubscriptionEvent event,
            Set<io.github.huyz0.os.biningester.format.RunKey> requested) {
        if (event.via() != io.github.huyz0.os.biningester.format.FetchMode.INLINE) {
            throw new IllegalStateException("catch-up events must carry inline bytes");
        }
        if (!requested.contains(event.key())) {
            return;
        }
        long next = deliveredUpTo.get(event.key());
        long exclusive = Math.addExact(event.firstOffset(), event.recordCount());
        if (exclusive <= next) {
            return;
        }
        if (event.firstOffset() < next) {
            throw new IllegalStateException("catch-up retry overlapped a previously delivered range");
        }
        if (event.firstOffset() > next) {
            throw new IllegalStateException("catch-up response skipped records after offset " + next);
        }
        if (clients.deliverCatchUp(request.requestId(), event)) {
            deliveredUpTo.put(event.key(), exclusive);
            catchUpClients.add(event.key());
        }
    }
}
