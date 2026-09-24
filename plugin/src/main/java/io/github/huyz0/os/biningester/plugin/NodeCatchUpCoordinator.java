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
    private record GapRange(long start, long through) { }

    private final SubscriptionTransport transport;
    private final NodeSubscriptions clients;
    private final Supplier<Optional<List<CatchUpRequestFrame.Stream>>> snapshot;
    private final Map<io.github.huyz0.os.biningester.format.RunKey, Long> deliveredUpTo =
            new java.util.HashMap<>();
    private final Set<io.github.huyz0.os.biningester.format.RunKey> catchUpClients =
            new HashSet<>();
    private final Map<io.github.huyz0.os.biningester.format.RunKey, GapRange> pendingGaps =
            new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<io.github.huyz0.os.biningester.format.RunKey, Long> gapTargets =
            new java.util.HashMap<>();
    private CatchUpRequestFrame request;
    private boolean gapRequest;
    private boolean done;

    NodeCatchUpCoordinator(SubscriptionTransport transport, NodeSubscriptions clients,
            Supplier<Optional<List<CatchUpRequestFrame.Stream>>> snapshot) {
        this.transport = java.util.Objects.requireNonNull(transport, "transport");
        this.clients = java.util.Objects.requireNonNull(clients, "clients");
        this.snapshot = java.util.Objects.requireNonNull(snapshot, "snapshot");
        clients.onGap(this::requestGap);
    }

    /** Runs on the generic pool; a pending position read is retried by the caller's schedule. */
    synchronized void attempt() {
        if (request == null && !done) {
            try {
                Optional<List<CatchUpRequestFrame.Stream>> ready = snapshot.get();
                if (ready.isPresent() && !ready.get().isEmpty()) {
                    List<CatchUpRequestFrame.Stream> streams = List.copyOf(ready.get());
                    if (streams.size() > CatchUpRequestFrame.MAX_STREAMS) {
                        throw new CatchUpRequestFrame.StreamLimitException(streams.size(),
                                CatchUpRequestFrame.MAX_STREAMS);
                    }
                    if (clients.holdsAll(streams.stream()
                            .map(CatchUpRequestFrame.Stream::key).toList())) {
                        request = new CatchUpRequestFrame(UUID.randomUUID(), streams);
                        streams.forEach(stream ->
                                deliveredUpTo.put(stream.key(), stream.batchStart()));
                    }
                }
            } catch (CatchUpRequestFrame.StreamLimitException unsupportedSize) {
                LOG.error("node catch-up snapshot exceeds the supported stream limit; "
                        + "catch-up cannot proceed", unsupportedSize);
                done = true;
            } catch (RuntimeException failed) {
                LOG.warn("node catch-up position read failed; it will retry at the progress interval",
                        failed);
            }
        }
        if (request == null && !pendingGaps.isEmpty()) {
            List<CatchUpRequestFrame.Stream> streams = pendingGaps.entrySet().stream()
                    .map(entry -> new CatchUpRequestFrame.Stream(
                            entry.getKey(), entry.getValue().start()))
                    .limit(CatchUpRequestFrame.MAX_STREAMS)
                    .toList();
            request = new CatchUpRequestFrame(UUID.randomUUID(), streams);
            gapRequest = true;
            streams.forEach(stream -> {
                GapRange range = pendingGaps.get(stream.key());
                deliveredUpTo.put(stream.key(), stream.batchStart());
                gapTargets.put(stream.key(), range.through());
            });
        }
        if (request == null) {
            return;
        }
        Set<io.github.huyz0.os.biningester.format.RunKey> requested = new HashSet<>();
        request.streams().forEach(stream -> requested.add(stream.key()));
        try {
            SubscriptionTransport.CatchUpResult result = transport.requestCatchUp(request,
                    event -> accept(event, requested));
            if (result == SubscriptionTransport.CatchUpResult.UNSUPPORTED) {
                if (!gapRequest) {
                    done = true;
                    request = null;
                } else {
                    LOG.error("reachable ingester does not support gap catch-up; "
                            + "the affected stream remains held");
                }
                return;
            }
            if (gapRequest && !catchUpClients.containsAll(requested)) {
                LOG.error("gap catch-up completed without replay for every held stream; "
                        + "the affected streams remain held");
                return;
            }
            if (gapRequest && request.streams().stream().anyMatch(stream ->
                    deliveredUpTo.get(stream.key()) < gapTargets.get(stream.key()))) {
                LOG.error("gap catch-up ended before reaching the held live range; "
                        + "the affected streams remain held for retry");
                return;
            }
            clients.completeCatchUp(request.requestId(), catchUpClients);
            if (gapRequest) {
                for (CatchUpRequestFrame.Stream stream : request.streams()) {
                    clients.completeGapRepair(stream.key());
                    pendingGaps.remove(stream.key(), new GapRange(stream.batchStart(),
                            gapTargets.get(stream.key())));
                }
            } else {
                done = true;
            }
            request = null;
            gapRequest = false;
            deliveredUpTo.clear();
            catchUpClients.clear();
            gapTargets.clear();
        } catch (IOException | RuntimeException failed) {
            LOG.warn("node catch-up exchange failed; it will retry at the progress interval", failed);
        }
    }

    private void requestGap(io.github.huyz0.os.biningester.format.RunKey key,
            io.github.huyz0.os.biningester.client.DeliveryGapException gap) {
        pendingGaps.merge(key, new GapRange(gap.expectedOffset(), gap.receivedOffset()),
                (previous, next) -> new GapRange(Math.min(previous.start(), next.start()),
                        Math.max(previous.through(), next.through())));
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
