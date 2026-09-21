// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import io.github.huyz0.os.biningester.format.RunKey;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * How a consumer is told its stream advanced.
 *
 * <p>WARNING: one of the four I/O seams (architecture.md). It exists so the
 * consumer can be tested at T1 against a fake transport -- criterion 3 runs
 * 1,600 consumers that way -- while the real one speaks HTTP from the `plugin`
 * side. A consumer that reached for a socket itself could not be tested at that
 * scale inside L0's budget.
 */
public interface SubscriptionTransport {

    /** Handed each delivery for the subscribed stream. */
    @FunctionalInterface
    interface Listener {
        void onDelivery(Delivery delivery);

        /**
         * Told the lowest offset of {@code key} still retained (ADR-0056).
         *
         * <p>⚠️ **A NO-OP BY DEFAULT, AND THAT IS CORRECT RATHER THAN A HIDDEN
         * GAP**: a listener that ignores the floor refuses nothing, which is
         * the behaviour every consumer had before the floor could travel. What
         * must not be silent is a listener that OWNS a {@code ConsumerClient}
         * and drops it -- the consumer then never refuses a collected position
         * -- and both of those in the tree override this.
         *
         * <p>⚠️ **KEYED**, so a router holding many streams on one subscription
         * can hand it to the right client, exactly as it routes deliveries.
         */
        default void onRetainedFloor(RunKey key, long oldestRetainedOffset) {
        }

        /**
         * Whether the next poll for {@code key} should ask for the floor
         * (ADR-0056).
         *
         * <p>⚠️ **FALSE BY DEFAULT, AND ASKED PER POLL.** A listener asks only
         * while a resume is waiting on a fresh floor, so a subscription whose
         * shards are all tailing never asks and the serving pod never reads
         * the store on its behalf. An earlier version asked until a floor
         * arrived and then never again, and review found both halves wrong: a
         * stream with no floor to give asked for ever, and a stream that had
         * one never refreshed it for a later resume.
         */
        default boolean wantsRetainedFloor(RunKey key) {
            return false;
        }
    }

    /** Registers interest; closing the handle unsubscribes. */
    AutoCloseable subscribe(RunKey key, Listener listener);

    /**
     * Tells the ingester the SHAPE of an index this node ingests (M6.7,
     * FR-16, ADR-0015 § 2, ADR-0047).
     *
     * <p>⚠️ IT TRAVELS ON THE SUBSCRIPTION THE NODE ALREADY HOLDS, which is
     * the whole reason it is on this seam rather than on one of its own: a
     * second channel is a second endpoint, a second credential and a second
     * unauthenticated surface, for one message per index per cluster-state
     * change.
     *
     * <p>⚠️ THE DEFAULT REFUSES RATHER THAN ACCEPTING SILENTLY. A no-op
     * default would make a transport that cannot carry registrations look
     * exactly like one that can: the plugin would count its pushes as
     * delivered, the ingester would never learn any index's shape, and every
     * routed write would be refused at {@code pendingTimeout} with nothing
     * naming the cause. No production transport exists yet -- M5.6e, owned by
     * M8 -- so this is the honest state rather than a gap being papered over.
     *
     * <p>⚠️ IT MAY THROW, AND THE CALLER RETRIES. {@code IndexRegistrar}
     * attempts each push three times and carries a failure to the next
     * cluster-state change, because a registration lost to one dropped message
     * turns into a sustained stream of refused writes for every index on that
     * node.
     */
    default void register(io.github.huyz0.os.biningester.format.IndexRegistration registration) {
        throw new UnsupportedOperationException("this transport cannot carry index "
                + "registrations, so the ingester would never learn " + registration.indexName()
                + "'s shard count and every routed write to it would be refused when its wait "
                + "expired (M5.6e is the production transport)");
    }

    /**
     * Tells the ingester where this node's shard copies have got to (M7.3,
     * FR-9, ADR-0005, ADR-0049).
     *
     * <p>⚠️ IT TRAVELS ON THE SUBSCRIPTION THE NODE ALREADY HOLDS, for the
     * same reason {@link #register} does: metadata-only, same-AZ, and no new
     * endpoint, credential or unauthenticated surface. It costs zero
     * object-store requests, which is what lets an idle node keep reporting
     * forever (NFR-2).
     *
     * <p>⚠️ THE DEFAULT REFUSES RATHER THAN SWALLOWING. A transport that
     * accepted progress silently would leave every watermark on the node
     * frozen at nothing while the deployment looked healthy, and the first
     * anyone would hear of it is data reaching {@code maxRetention} and its
     * alarm — hours later, and reported as a retention incident rather than as
     * a transport that cannot carry a frame.
     */
    default void report(io.github.huyz0.os.biningester.format.ConsumerProgress progress) {
        throw new UnsupportedOperationException("this transport cannot carry consumer "
                + "progress, so the ingester would never learn where this node's "
                + progress.entries().size() + " shard copies have got to, and GC would "
                + "keep their streams until maxRetention (M5.6e is the production "
                + "transport)");
    }

    /**
     * One listener's registration against a CHANGING set of streams, which is
     * what a node holds (M5.62).
     *
     * <p>⚠️ IT IS A TYPE RATHER THAN A RE-SUBSCRIBE, and that is the whole
     * design. The first version of this seam rebuilt the whole registration
     * whenever a stream appeared or went, and review found two majors in the
     * window that opens between the two calls: opening the new one first
     * duplicates every delivery for the keys carried by both -- and nothing on
     * the consumer path dedups a repeated {@code Delivery}, so the records are
     * applied twice -- while closing the old one first drops whatever arrives
     * in the gap. {@link #add} and {@link #remove} touch ONE key and leave
     * every other registration untouched, so there is no window either way.
     *
     * <p>⚠️ WHAT AN IMPLEMENTATION MAY NOT DO, because its only production
     * caller calls {@link #add} and {@link #remove} from INSIDE a
     * {@code ConcurrentHashMap} mapping function, holding that key's bin: it
     * may not block on I/O (every shard opening on the node serialises behind
     * that call), it may not invoke the {@link Listener} synchronously from
     * {@code add} -- the key is not yet in the caller's map, so that delivery
     * is discarded -- and it may not re-enter the caller -- a reconnect handler that
     * re-added keys would deadlock or throw, depending on which bin it lands
     * in. None of these is enforced by anything, which is why it is stated
     * where an implementer reads it rather than only where the caller reasons
     * about it.
     *
     * <p>⚠️ AND IT MAKES THE MERGE A TYPE RATHER THAN A PROMISE. The property
     * this exists for -- one subscriber for K streams, so the segment is handed
     * over once -- cannot be expressed by calling a single-key {@code subscribe}
     * K times with the same listener, because nothing stops an implementation
     * building a fresh subscriber per call. Holding the keys in one object
     * makes "these are one subscriber's" the only reading.
     */
    interface MultiSubscription extends AutoCloseable {

        /** Adds one stream to this listener's registration. */
        void add(RunKey key);

        /**
         * Removes one stream. ⚠️ Removing a key that is not registered is a
         * no-op rather than an error: the caller that drops the last holder of
         * a stream is racing anything that closed the whole subscription, and
         * both orders must be safe.
         *
         * <p>⚠️ IF IT THROWS, THE STREAM MUST STAY REGISTERED. The caller's
         * recovery is to let the next open/close pair for that stream try
         * again, and that recovery re-enters {@code remove} and nothing else --
         * it does NOT call {@link #add}, because the key is still held. An
         * implementation that unsubscribed and then threw would therefore leave
         * a live consumer with no registration, which is the failure this seam
         * exists to prevent. The default keeps its handle until the close
         * succeeds, and {@code MultiSubscriptionDefaultTest} pins it.
         */
        void remove(RunKey key);

        /** Unsubscribes every stream still registered. */
        @Override
        void close();
    }

    /**
     * Registers ONE listener against MANY streams; closing the handle
     * unsubscribes all of them.
     *
     * <p>⚠️ THE POINT IS THE MERGE, NOT THE CONVENIENCE. A node holding K runs
     * of a segment needs the BYTES once and a delivery per run.
     * {@code SubscriptionHub} groups by {@code Subscriber} IDENTITY, so one
     * subscriber registered against K keys is handed the segment once -- and K
     * separate registrations are K consumers to the hub, which is K copies.
     * M5.40a bought that property at the hub and nothing opted into it; this is
     * the expression a caller uses to opt in.
     *
     * <p>⚠️ THE DEFAULT DOES NOT MERGE, and says so rather than pretending. It
     * subscribes each key on its own, which is exactly the K-consumer shape
     * above -- correct, and paying for every copy. It exists so a transport
     * that cannot merge (a fake, or one speaking a protocol with no multi-key
     * frame) is not forced to lie about it; a transport that CAN merge
     * overrides this and registers one subscriber.
     *
     * <p>⚠️ ALL-OR-NONE FAILURE FOLLOWS THE MERGE, which is M5.40a's rule
     * arriving where an operator can see it: one byte stream means one broken
     * stream, so a failure fails every run this listener holds rather than one
     * of them. A caller that needs per-run isolation subscribes per run and
     * pays for the copies.
     *
     * @param keys the streams to start with, possibly empty -- a node that has
     *     opened no shard yet holds none, and {@link MultiSubscription#add} is
     *     how the first one arrives
     */
    default MultiSubscription subscribe(List<RunKey> keys, Listener listener) {
        Objects.requireNonNull(keys, "keys");
        Objects.requireNonNull(listener, "listener");
        Map<RunKey, AutoCloseable> handles = new LinkedHashMap<>();
        try {
            // ⚠️ `computeIfAbsent`, so a duplicate key in `keys` subscribes
            // ONCE. `put` overwrites, and the handle it overwrites is the one
            // nothing can close afterwards.
            keys.forEach(key -> handles.computeIfAbsent(key, k -> subscribe(k, listener)));
        } catch (RuntimeException failed) {
            // ⚠️ THE ONES ALREADY OPEN ARE CLOSED, AND A THROW FROM ONE OF THEM
            // DOES NOT STOP THE REST. `closeQuietly` rethrows despite its name,
            // so a plain forEach over it would leak every handle after the
            // first failure -- the leak this block exists to prevent -- and
            // would replace the original failure with the close's.
            for (AutoCloseable handle : handles.values()) {
                try {
                    closeQuietly(handle);
                } catch (RuntimeException alsoFailed) {
                    failed.addSuppressed(alsoFailed);
                }
            }
            throw failed;
        }
        // ⚠️ SYNCHRONIZED, EVERY METHOD. The only production caller manages
        // shards, which open and close on different threads, and a lost update
        // to this map leaves a handle unrecorded -- so `close` never
        // unsubscribes that stream and the node keeps taking its deliveries.
        // A plain map here was a real defect rather than a theoretical one.
        return new MultiSubscription() {
            @Override
            public synchronized void add(RunKey key) {
                handles.computeIfAbsent(key, k -> subscribe(k, listener));
            }

            @Override
            public synchronized void remove(RunKey key) {
                AutoCloseable handle = handles.get(key);
                if (handle == null) {
                    return;
                }
                // ⚠️ THE MAPPING GOES ONLY IF THE CLOSE SUCCEEDS. Removing it
                // first and then closing loses the handle when the close
                // throws: a retried `remove` is a documented no-op and
                // `close()` no longer knows about it, so that stream stays
                // subscribed with nothing able to unsubscribe it.
                closeQuietly(handle);
                handles.remove(key);
            }

            @Override
            public synchronized void close() {
                // ⚠️ EVERY HANDLE IS CLOSED EVEN IF ONE THROWS. Stopping at the
                // first leaves the rest subscribed and the caller holds nothing
                // to close them with -- a leak with no recovery, where the
                // alternative is one exception that reaches the caller anyway.
                RuntimeException first = null;
                for (AutoCloseable handle : handles.values()) {
                    try {
                        closeQuietly(handle);
                    } catch (RuntimeException failed) {
                        if (first == null) {
                            first = failed;
                        } else {
                            first.addSuppressed(failed);
                        }
                    }
                }
                handles.clear();
                if (first != null) {
                    throw first;
                }
            }
        };
    }

    private static void closeQuietly(AutoCloseable handle) {
        if (handle == null) {
            return;
        }
        try {
            handle.close();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("failed to unsubscribe", e);
        }
    }
}
