// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.DeltaHintFrame;
import io.github.huyz0.os.biningester.http.DeltaFanOut;
import io.github.huyz0.os.biningester.http.DeltaRelay;
import io.github.huyz0.os.biningester.http.EndpointSliceView;
import io.github.huyz0.os.biningester.ingest.ChainPublisher;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Where every durable delta enters this pod's publication (M10.20a,
 * ADR-0075): the leaseholder's committed-delta hook, a push from a pod of this
 * AZ, and a hint this pod relays as its AZ's relay.
 *
 * <p>⚠️ **BUILT BEFORE THE SEQUENCER, ATTACHED AFTER THE INGEST.** A term's
 * hook is installed when the lease is taken -- possibly inside the sequencer's
 * constructor, and before that term's inbox drain starts committing -- while
 * the chain publisher only exists once the ingest does. A delta committed
 * before {@link #attach} is HELD, in order, and delivered on attach; past
 * {@link #PENDING_LIMIT} it is dropped and counted rather than growing
 * without bound.
 */
final class DeltaDelivery implements AutoCloseable {

    /** Deltas that may wait for the chain publisher. */
    static final int PENDING_LIMIT = 4096;

    private record Held(CommitDelta delta, long epoch) {
    }

    private final ServerConfig config;
    private final CrossAzBytes crossAz;
    private final EndpointSliceView peers;
    private final DeltaRelay.Reader reader;
    private final List<Held> pending = new ArrayList<>();
    private final AtomicLong droppedBeforeAttach = new AtomicLong();
    private ChainPublisher chain;
    private DeltaFanOut fanOut;
    private DeltaRelay relay;

    DeltaDelivery(ServerConfig config, CrossAzBytes crossAz, EndpointSliceView peers,
            DeltaRelay.Reader reader) {
        this.config = Objects.requireNonNull(config, "config");
        this.crossAz = crossAz == null ? CrossAzBytes.untracked() : crossAz;
        this.peers = Objects.requireNonNull(peers, "peers");
        this.reader = Objects.requireNonNull(reader, "reader");
    }

    /**
     * Starts delivering through {@code publisher}, then delivers what was held;
     * call once. ⚠️ UNDER THE SAME LOCK AS {@link #committed}: a commit landing
     * mid-replay would otherwise be published before the older held deltas,
     * and the chain publisher drops those as stale.
     */
    synchronized void attach(ChainPublisher publisher) {
        Objects.requireNonNull(publisher, "publisher");
        if (chain != null) {
            throw new IllegalStateException("already attached");
        }
        chain = publisher;
        if (config.httpPort() > 0) {
            fanOut = new DeltaFanOut(config.podId(), config.az(), config.httpPort(), crossAz,
                    peers::readyEndpoints, (delta, epoch) -> publisher.offer(epoch, delta));
            relay = new DeltaRelay(reader, fanOut::relayed);
        }
        for (Held h : pending) {
            deliver(h.delta(), h.epoch());
        }
        pending.clear();
    }

    /**
     * The leaseholder's hook: this pod publishes, then every other pod is
     * reached. Everything it calls only enqueues, so holding the lock here
     * never blocks the commit path on a peer.
     */
    synchronized void committed(CommitDelta delta, long epoch) {
        if (chain == null) {
            if (pending.size() < PENDING_LIMIT) {
                pending.add(new Held(delta, epoch));
            } else {
                droppedBeforeAttach.incrementAndGet();
            }
            return;
        }
        deliver(delta, epoch);
    }

    private void deliver(CommitDelta delta, long epoch) {
        if (fanOut != null) {
            fanOut.committed(delta, epoch);
        } else {
            chain.offer(epoch, delta);
        }
    }

    /** A delta pushed by a pod of this AZ. */
    void pushed(long epoch, CommitDelta delta) {
        ChainPublisher publisher;
        synchronized (this) {
            publisher = chain;
        }
        if (publisher != null) {
            publisher.offer(epoch, delta);
        } else {
            droppedBeforeAttach.incrementAndGet();
        }
    }

    /** A hint this pod received as its AZ's relay; false when it was not queued. */
    boolean hinted(DeltaHintFrame hint) {
        DeltaRelay r;
        synchronized (this) {
            r = relay;
        }
        if (r == null) {
            droppedBeforeAttach.incrementAndGet();
            return false;
        }
        return r.offer(hint);
    }

    /**
     * Installs this delivery as {@code term}'s committed-delta hook, THEN
     * starts {@code drain}. ⚠️ ONE METHOD SO THE ORDER CANNOT DRIFT: a drain
     * started first can commit an intent before the hook exists, and that
     * delta reaches no pod.
     */
    void hookThenDrain(io.github.huyz0.os.biningester.sequencer.LocalSequencer term,
            Runnable drain) {
        term.onCommitted(this::committed);
        drain.run();
    }

    /** Deltas and hints that arrived before attach, or with nothing to take them. */
    long droppedBeforeAttach() {
        return droppedBeforeAttach.get();
    }

    @Override
    public void close() {
        DeltaRelay r;
        DeltaFanOut out;
        ChainPublisher publisher;
        synchronized (this) {
            r = relay;
            out = fanOut;
            publisher = chain;
        }
        if (r != null) {
            r.close();
        }
        if (out != null) {
            out.close();
        }
        if (publisher != null) {
            publisher.close();
        }
    }
}
