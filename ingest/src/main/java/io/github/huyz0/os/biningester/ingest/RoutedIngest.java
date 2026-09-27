// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.security.Principal;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Places records the producer did not place (M6.6, FR-13, FR-19, ADR-0015).
 *
 * <p>⚠️ A DECORATOR, NOT A SECOND INGESTER. Everything about buffering,
 * flushing, committing and pushing stays in the {@link Ingest} it wraps; what
 * this adds is the answer to "which partition", which needs the registered
 * shape and therefore the catalog. Putting it inside {@code DefaultIngest}
 * would also have put it past code-structure.md's 700-line cap, which that file
 * is exactly at.
 *
 * <p>⚠️ ONE ROUTING VALUE PER REQUEST, SO ONE PARTITION PER REQUEST. The value
 * arrives out of band — a query parameter, a frame field — never from inside a
 * document, because parsing the body is the most expensive thing this service
 * could do (research doc 02 § 3) and is forbidden.
 *
 * <p>⚠️ AN EXPLICIT PARTITION IS VALIDATED HERE, which is FR-13's defining
 * clause and has never been served before: {@code ?partition=99} on a 3-shard
 * index returns 202 today and lands in a stream nothing polls. The check needs
 * a registered shard count, which did not exist in the ingester until M6.3.
 *
 * <p>⚠️ AN UNKNOWN INDEX WAITS RATHER THAN BEING REFUSED (ADR-0015's
 * Consequences): "a producer that starts before the plugin connects is not
 * punished for a race it cannot see". A 400 there is a 400 storm on every
 * plugin reconnect. The wait is bounded by {@link PendingPool}'s timeout and
 * ends in a refusal, never in a fold to partition 0.
 */
public final class RoutedIngest implements Ingest {

    private final Ingest delegate;
    private final IndexCatalog catalog;
    private final PendingPool pending;
    private final Duration pendingTimeout;

    /**
     * ⚠️ ONE LOCK AND ONE CONDITION FOR THE WHOLE INGESTER, not one per index.
     * A registration wakes every waiter and each re-checks its own index, which
     * is O(waiters) on a path that runs once per cluster-state change -- against
     * a per-index condition map that would have to be created, found and
     * removed under its own lock on the per-RECORD path.
     */
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition registered = lock.newCondition();

    private final java.time.Clock clock;

    public RoutedIngest(Ingest delegate, IndexCatalog catalog, PendingPool pending,
            Duration pendingTimeout, java.time.Clock clock) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.pending = Objects.requireNonNull(pending, "pending");
        this.pendingTimeout = Objects.requireNonNull(pendingTimeout, "pendingTimeout");
        if (!pendingTimeout.equals(pending.timeout())) {
            // ⚠️ ONE TIMEOUT, NOT TWO THAT DRIFT. The wait below and the pool's
            // own sweep are the two halves of the same bound: a wait longer
            // than the pool's hands the producer a refusal naming a duration
            // that did not decide anything, and a shorter one refuses records
            // the pool is still holding.
            throw new IllegalArgumentException("the wait of " + pendingTimeout
                    + " and the pending pool's own timeout of " + pending.timeout()
                    + " are the two halves of one bound and must agree");
        }
        // ⚠️ SUBSCRIBED TO THE CATALOG, NOT ONLY TO {@link #register}: the
        // assembled server's subscription endpoint registers into the catalog
        // directly, and a write waiting here would sleep out its timeout for an
        // index that had arrived (M8.32).
        catalog.whenRegistered(this::wakeWaiters);
    }

    /** How many requests are currently waiting for a registration. */
    public int pendingBatches() {
        return pending.waitingBatches();
    }

    /** The catalog this ingester places against. */
    public IndexCatalog catalog() {
        return catalog;
    }

    /**
     * Records what a node said about an index, and releases anything waiting
     * for it.
     *
     * <p>⚠️ THE WAITERS ARE WOKEN AFTER THE CATALOG IS UPDATED, never before: a
     * waiter that re-checked between the signal and the update would find the
     * index still unknown and go back to sleep until its timeout, which is the
     * refusal this whole path exists to avoid. The catalog's own listener does
     * the waking, so a registration that bypasses this method wakes them too.
     */
    public void register(IndexRegistration registration) {
        catalog.register(registration);
    }

    private void wakeWaiters() {
        lock.lock();
        try {
            registered.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * ⚠️ VALIDATED AGAINST THE REGISTERED SHARD COUNT WHEN THERE IS ONE, and
     * passed through untouched when there is not. An unknown index is not an
     * invalid partition -- the producer may be ahead of the plugin, and
     * refusing would punish it for a race it cannot see.
     */
    @Override
    public AppendResult append(Principal principal, String index, int partition,
            RecordSource records) throws IOException {
        return append(principal, index, partition, (byte) 0, records);
    }

    @Override
    public boolean acceptsLane(byte lane) {
        return delegate.acceptsLane(lane);
    }

    /**
     * ⚠️ REFUSED BEFORE ANY WORK, and above all before the pending pool: a
     * pooled write of an inactive lane would hold pool space for the whole
     * registration timeout and end in a 503 the producer retries for ever.
     */
    private void requireLane(byte lane) {
        if (!delegate.acceptsLane(lane)) {
            throw new PlacementRefusedException("lane " + lane + " is not active on this "
                    + "ingester");
        }
    }

    /** ⚠️ THE LANE TRAVELS WITH THE RECORDS, on every path (ADR-0074). */
    @Override
    public AppendResult append(Principal principal, String index, int partition, byte lane,
            RecordSource records) throws IOException {
        requireLane(lane);
        Optional<IndexRegistration> known = catalog.resolve(index);
        if (known.isPresent() && partition >= known.get().numShards()) {
            throw new PlacementRefusedException("partition " + partition + " does not exist in "
                    + known.get().indexName() + ", which has " + known.get().numShards()
                    + " shards -- records written there land in a stream no shard polls, and "
                    + "nothing downstream would report it");
        }
        // ⚠️ THE CONCRETE NAME, so an explicit-partition write to an ALIAS
        // lands in the same streams a routed one does. Passing the alias
        // through would make `(indexUUID, partition)` depend on which name the
        // producer happened to use.
        String concrete = known.map(IndexRegistration::indexName).orElse(index);
        return delegate.append(principal, concrete, partition, lane, records);
    }

    @Override
    public AppendResult appendRouted(Principal principal, String indexOrAlias, String routing,
            RecordSource records) throws IOException {
        return appendRouted(principal, indexOrAlias, routing, (byte) 0, records);
    }

    @Override
    public AppendResult appendRouted(Principal principal, String indexOrAlias, String routing,
            byte lane, RecordSource records) throws IOException {
        Objects.requireNonNull(routing, "routing");
        requireLane(lane);
        Optional<IndexRegistration> known = catalog.resolveForRouting(indexOrAlias);
        if (known.isPresent()) {
            return place(principal, known.get(), routing, lane, records);
        }
        return waitForRegistration(principal, indexOrAlias, routing, lane, records);
    }

    private AppendResult place(Principal principal, IndexRegistration index, String routing,
            byte lane, RecordSource records) throws IOException {
        int partition = RoutingPartitioner.partitionFor(index, routing);
        return delegate.append(principal, index.indexName(), partition, lane, records);
    }

    /**
     * Holds the records until the index is registered, or refuses them.
     *
     * <p>⚠️ THE RECORDS ARE MATERIALISED HERE AND ONLY HERE. Everywhere else in
     * this service a {@code RecordSource} is streamed straight into the
     * accumulator (java-style.md rule 8), and this path breaks that
     * deliberately: a record that must WAIT has to be held somewhere, and the
     * pool is where -- bounded, per index, in bytes. The alternative is holding
     * the producer's connection open while the source is un-consumed, which
     * moves the same memory into the HTTP layer where nothing bounds it.
     */
    private AppendResult waitForRegistration(Principal principal, String indexOrAlias,
            String routing, byte lane, RecordSource records) throws IOException {
        // ⚠️ ONE BATCH PER REQUEST, which is the ownership boundary M6.6's
        // round-1 review measured the absence of: with the pool keyed on the
        // index alone, the first waiter to wake drained EVERY request's
        // records and wrote all of them at the one partition its own routing
        // value named -- one producer's records at another's partition, and
        // the owner holding an exception for a write that had happened.
        PendingPool.Batch batch = pending.open(indexOrAlias, routing);
        boolean[] any = {false};
        boolean settled = false;
        try {
            records.forEachRecord(record -> {
                any[0] = true;
                if (!batch.offer(record)) {
                    throw new RegistrationTimeoutException("index " + indexOrAlias
                            + " is not registered and its pending pool is full -- the plugin "
                            + "has not pushed its shape and this ingester will not guess a "
                            + "partition for it");
                }
            });
            if (!any[0]) {
                throw new IllegalArgumentException("a write of no records is not a write");
            }
            AppendResult result = placeWhenRegistered(principal, indexOrAlias, routing, lane,
                    batch);
            settled = true;
            return result;
        } finally {
            // ⚠️ EVERY EXIT DISCARDS THE BATCH, AND A `finally` IS WHAT MAKES
            // THAT TRUE. Round 3 MEASURED the version that caught instead: an
            // index that registered WITH `routing_partition_size > 1` while a
            // write waited threw out of the WAIT, past the catch, and left
            // waitingBatches=1 and bytesHeldFor=42 -- forever, because nothing
            // calls `expire()` in production. The index's bound then erodes by
            // one request per failure until every routed write to it is
            // refused for a pool full of records nobody is waiting for.
            if (!settled) {
                batch.discard();
            }
        }
    }

    private AppendResult placeWhenRegistered(Principal principal, String indexOrAlias,
            String routing, byte lane, PendingPool.Batch batch) throws IOException {
        Optional<IndexRegistration> arrived = awaitRegistration(indexOrAlias);
        List<SegmentRecord> mine = batch.take();
        if (arrived.isEmpty() || mine.isEmpty()) {
            // ⚠️ EMPTY MEANS THE SWEEPER GOT THERE FIRST, which is the same
            // outcome for the producer and must not be reported as a success:
            // appending nothing would return 202 for records that were
            // dropped.
            throw new RegistrationTimeoutException("index " + indexOrAlias + " was still not "
                    + "registered after " + pendingTimeout + " -- refusing rather than folding "
                    + "these records into partition 0, which would funnel the whole index into "
                    + "one shard while telling the producer it succeeded (ADR-0006)");
        }
        IndexRegistration index = arrived.get();
        int partition = RoutingPartitioner.partitionFor(index, routing);
        return delegate.append(principal, index.indexName(), partition, lane, mine::forEach);
    }

    private Optional<IndexRegistration> awaitRegistration(String indexOrAlias) {
        // ⚠️ THE INJECTED CLOCK, not `System.nanoTime` -- non-negotiable 7 and
        // `check-io-seam`, which went red on the first draft. The pool takes
        // one for exactly this reason and reintroducing the wall clock a layer
        // up would make the timeout untestable again.
        long deadlineMillis = clock.millis() + pendingTimeout.toMillis();
        lock.lock();
        try {
            while (true) {
                Optional<IndexRegistration> known = catalog.resolveForRouting(indexOrAlias);
                if (known.isPresent()) {
                    return known;
                }
                long left = deadlineMillis - clock.millis();
                if (left <= 0) {
                    return Optional.empty();
                }
                // ⚠️ `awaitNanos` RATHER THAN A SLEEP LOOP: a registration
                // arriving in a millisecond releases the write in a
                // millisecond, which is the whole reason the pool exists
                // rather than a plain refusal.
                registered.await(left, java.util.concurrent.TimeUnit.MILLISECONDS);
            }
        } catch (InterruptedException interrupted) {
            // ⚠️ THE INTERRUPT IS NAMED, not folded into the timeout. Telling
            // a producer its index "was still not registered after PT5S" when
            // the wait was cut short sends an operator after a plugin that was
            // fine.
            Thread.currentThread().interrupt();
            throw new RegistrationTimeoutException("the wait for index " + indexOrAlias
                    + "'s registration was interrupted before it could be placed");
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
