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
    private record GapRange(long start, long through, long epoch, long sequence) {
        private static GapRange merge(GapRange previous, GapRange current) {
            boolean hasPrevious = previous != null;
            boolean sameEpoch = hasPrevious && previous.epoch() == current.epoch();
            long start = hasPrevious ? Math.min(previous.start(), current.start()) : current.start();
            long through = hasPrevious
                    ? Math.max(previous.through(), current.through()) : current.through();
            long epoch = !hasPrevious ? current.epoch() : sameEpoch ? previous.epoch() : -1;
            long sequence = !hasPrevious ? current.sequence()
                    : sameEpoch ? Math.max(previous.sequence(), current.sequence()) : -1;
            return new GapRange(start, through, epoch, sequence);
        }
    }

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
    private final Map<io.github.huyz0.os.biningester.format.RunKey, GapRange> requestedGaps =
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
                requestedGaps.put(stream.key(), range);
                deliveredUpTo.put(stream.key(), stream.batchStart());
                gapTargets.put(stream.key(), range.through());
            });
        }
        if (request == null) {
            return;
        }
        Set<io.github.huyz0.os.biningester.format.RunKey> requested = new HashSet<>();
        request.streams().forEach(stream -> requested.add(stream.key()));
        boolean[] refusedEvent = {false};
        try {
            SubscriptionTransport.CatchUpResult result = transport.requestCatchUp(request,
                    event -> {
                        if (!accept(event, requested)) {
                            refusedEvent[0] = true;
                        }
                    });
            if (refusedEvent[0]) {
                LOG.warn("catch-up lane refused a replay event; the same request will retry");
                return;
            }
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
                    pendingGaps.remove(stream.key(), requestedGaps.get(stream.key()));
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
            if (gapRequest && !clients.ingesterAnswers()) {
                try {
                    if (recoverFromNodeStore(requested)) {
                        clients.completeCatchUp(request.requestId(), catchUpClients);
                        for (CatchUpRequestFrame.Stream stream : request.streams()) {
                            clients.completeGapRepair(stream.key());
                            pendingGaps.remove(stream.key(), requestedGaps.get(stream.key()));
                        }
                        clearRequest();
                        return;
                    }
                } catch (RuntimeException localRecoveryFailed) {
                    LOG.warn("node-local gap recovery failed; the affected streams remain held",
                            localRecoveryFailed);
                }
            }
            LOG.warn("node catch-up exchange failed; it will retry at the progress interval", failed);
        }
    }

    private boolean recoverFromNodeStore(Set<io.github.huyz0.os.biningester.format.RunKey> requested) {
        Map<io.github.huyz0.os.biningester.format.RunKey, TierThreeRecovery.Gap> gaps =
                new java.util.HashMap<>();
        long epoch = -1;
        long sequence = -1;
        for (CatchUpRequestFrame.Stream stream : request.streams()) {
            GapRange range = requestedGaps.get(stream.key());
            if (range == null || range.epoch() < 0 || range.sequence() < 0
                    || (epoch >= 0 && epoch != range.epoch())) {
                return false;
            }
            epoch = range.epoch();
            sequence = Math.max(sequence, range.sequence());
            gaps.put(stream.key(), new TierThreeRecovery.Gap(range.start(), range.through()));
        }
        if (gaps.isEmpty()) {
            return false;
        }
        return clients.recoverTierThree(epoch, sequence, gaps,
                event -> accept(event, requested));
    }

    private synchronized void requestGap(io.github.huyz0.os.biningester.format.RunKey key,
            io.github.huyz0.os.biningester.client.DeliveryGapException gap) {
        GapRange next = new GapRange(gap.expectedOffset(), gap.receivedOffset(),
                gap.sequencerEpoch(), gap.chainSequence());
        pendingGaps.put(key, GapRange.merge(pendingGaps.get(key), next));
    }

    private void clearRequest() {
        request = null;
        gapRequest = false;
        deliveredUpTo.clear();
        catchUpClients.clear();
        gapTargets.clear();
        requestedGaps.clear();
    }

    private boolean accept(SubscriptionEvent event,
            Set<io.github.huyz0.os.biningester.format.RunKey> requested) {
        if (event.via() != io.github.huyz0.os.biningester.format.FetchMode.INLINE) {
            throw new IllegalStateException("catch-up events must carry inline bytes");
        }
        if (!requested.contains(event.key())) {
            return true;
        }
        long next = deliveredUpTo.get(event.key());
        long exclusive = Math.addExact(event.firstOffset(), event.recordCount());
        if (exclusive <= next) {
            return true;
        }
        if (event.firstOffset() < next) {
            throw new IllegalStateException("catch-up retry overlapped a previously delivered range");
        }
        if (event.firstOffset() > next) {
            throw new IllegalStateException("catch-up response skipped records after offset " + next);
        }
        if (!clients.deliverCatchUp(request.requestId(), event)) {
            return false;
        }
        deliveredUpTo.put(event.key(), exclusive);
        catchUpClients.add(event.key());
        return true;
    }
}
