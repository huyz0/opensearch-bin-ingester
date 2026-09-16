// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import binjava.format.SegmentRecord;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Records waiting for their index's shape to arrive (M6.5, FR-13, ADR-0015 § 3,
 * ADR-0046).
 *
 * <p>⚠️ IT WORKS ONLY BECAUSE ORDERING IS ASSIGNED AT COMMIT.
 * [ADR-0001](../../../../../docs/internal/product/decisions/0001-segments-carry-no-absolute-offsets.md)
 * is what makes a buffered record movable: it has no offset yet, so placing it
 * late costs nothing and needs no key. A design that assigned offsets at write
 * time would have to choose a partition immediately and could only defer the
 * problem to read time, which ADR-0015 prices at S-times serving amplification.
 *
 * <p>⚠️ IT COVERS THE UNKNOWN-INDEX CASE ONLY. ADR-0015 § 3 also named "a
 * rollover the plugin has not yet pushed", and ADR-0046 withdrew that: the
 * previous registration is present and correct FOR THE INDEX IT DESCRIBES, so
 * nothing in the ingester distinguishes a current mapping from one that was
 * current a millisecond ago, and detecting it means asking OpenSearch per
 * RECORD -- 10,000 cluster questions for one {@code _bulk} -- or on a timer.
 *
 * <p>⚠️ THE TIMEOUT REJECTS; IT NEVER FOLDS. ADR-0006's rule is "reject, never
 * fold": placing an unregistered index's records in partition 0 funnels a whole
 * index into one shard while returning 202 to the producer, which is a data
 * shape nobody can undo later.
 *
 * <p>⚠️ THE BOUND IS PER INDEX AND IN BYTES. Per index, because one unknown
 * index flooding a global bound would refuse the records of every OTHER index
 * waiting on an ordinary plugin reconnect -- the blast radius ADR-0010's
 * per-index share cap exists to prevent elsewhere. In bytes, because a bound on
 * the record COUNT holds N times the largest record and an operator cannot size
 * a heap against it.
 *
 * <p>⚠️ THE CLOCK IS INJECTED (non-negotiable 7). A pool that read
 * {@code System.currentTimeMillis} could only be tested by sleeping, and the
 * timeout is the one behaviour here that a test must drive rather than wait
 * for.
 */
public final class PendingPool {

    /** One record waiting to be placed, with what it will be placed BY. */
    public record Pending(String index, String routing, SegmentRecord record,
            long arrivedAtMillis) {

        public Pending {
            Objects.requireNonNull(index, "index");
            Objects.requireNonNull(routing, "routing");
            Objects.requireNonNull(record, "record");
        }

        long bytes() {
            // ⚠️ THE SAME ESTIMATE THE FLUSH TRIGGER USES, so a record's cost
            // is one number across the whole write path rather than two that
            // drift. The routing value is counted too: it is held here and
            // nowhere else.
            return Accumulator.estimatedFramedBytes(record)
                    + routing.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        }
    }

    private static final class Waiting {
        private final Deque<Pending> records = new ArrayDeque<>();
        private long bytes;
    }

    private final Clock clock;
    private final Duration timeout;
    private final long maxBytesPerIndex;
    private final Map<String, Waiting> byIndex = new ConcurrentHashMap<>();

    public PendingPool(Clock clock, Duration timeout, long maxBytesPerIndex) {
        this.clock = Objects.requireNonNull(clock, "clock");
        this.timeout = Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative() || timeout.isZero()) {
            throw new IllegalArgumentException("a pendingTimeout of " + timeout
                    + " rejects every record before its registration could arrive");
        }
        if (maxBytesPerIndex <= 0) {
            throw new IllegalArgumentException("maxBytesPerIndex is never " + maxBytesPerIndex);
        }
        this.maxBytesPerIndex = maxBytesPerIndex;
    }

    /**
     * Takes a record whose index is not yet registered.
     *
     * @return false when this index's pool is full, which the caller answers
     *     with a refusal rather than by placing the record somewhere
     */
    public boolean offer(String index, String routing, SegmentRecord record) {
        Objects.requireNonNull(index, "index");
        Pending pending = new Pending(index, routing, record, clock.millis());
        boolean[] accepted = {false};
        // ⚠️ `compute`, NEVER `computeIfAbsent` FOLLOWED BY A MONITOR, and
        // round-1 review MEASURED the difference: resolving the entry outside
        // the map's own lock lets `take` unmap it between the lookup and the
        // add, so the record lands in an object nothing can reach -- never
        // taken, never expired, and the producer holding a 202. One offer
        // thread against one consumer lost 3,510 of 200,000 records.
        // ⚠️ SO EVERY MUTATION OF A `Waiting` HAPPENS INSIDE A `compute` on
        // this map, which is what makes membership and content atomic
        // together. There is no second lock and there must not be one.
        byIndex.compute(index, (key, waiting) -> {
            Waiting target = waiting == null ? new Waiting() : waiting;
            if (target.bytes + pending.bytes() > maxBytesPerIndex) {
                // ⚠️ REFUSED, NOT EVICTED. Dropping the OLDEST waiting record
                // to make room would lose a write the producer was told
                // nothing about; refusing the newest tells the one producer
                // that can still do something about it.
                return waiting;
            }
            target.records.addLast(pending);
            target.bytes += pending.bytes();
            accepted[0] = true;
            return target;
        });
        return accepted[0];
    }

    /**
     * Everything waiting for {@code index}, removed, in arrival order.
     *
     * <p>⚠️ FIFO, because that is the order the producer sent them and the
     * order their offsets will be assigned in. Nothing here re-orders, and a
     * pool that did would make a replay produce a different log than the
     * original run.
     */
    public List<Pending> take(String index) {
        List<Pending> taken = new ArrayList<>();
        // ⚠️ THE REMOVAL AND THE COPY ARE ONE OPERATION. Removing first and
        // copying after is the window round-1 review measured: an `offer` that
        // resolved the same entry a moment earlier adds to it afterwards and
        // the record is unreachable.
        byIndex.compute(index, (key, waiting) -> {
            if (waiting != null) {
                taken.addAll(waiting.records);
            }
            return null;
        });
        return taken;
    }

    /**
     * Everything that has waited longer than the timeout, removed.
     *
     * <p>⚠️ THE CALLER REFUSES THEM; THIS ONLY HANDS THEM OVER. Which HTTP
     * status a timed-out write gets, and what the producer is told, belongs
     * with the adapter -- ADR-0019 and architecture.md rule 4 keep that
     * decision out of here.
     *
     * <p>⚠️ AND IT SWEEPS EVERY INDEX, not the one a caller happens to ask
     * about: an index nobody writes to again would otherwise hold its records
     * forever, which is the leak the bound exists to prevent arriving by
     * another road.
     */
    public List<Pending> expire() {
        long now = clock.millis();
        List<Pending> expired = new ArrayList<>();
        // ⚠️ THE KEYS ARE SNAPSHOTTED AND EACH IS SWEPT UNDER `compute`, so an
        // index that appears mid-sweep is simply swept next time -- its
        // records cannot be older than the timeout yet. Iterating the entry
        // set and mutating the values outside the map's lock is the window
        // round-1 review measured on the previous version.
        for (String index : List.copyOf(byIndex.keySet())) {
            byIndex.compute(index, (key, waiting) -> {
                if (waiting == null) {
                    return null;
                }
                while (!waiting.records.isEmpty()
                        && now - waiting.records.peekFirst().arrivedAtMillis()
                                >= timeout.toMillis()) {
                    Pending pending = waiting.records.removeFirst();
                    waiting.bytes -= pending.bytes();
                    expired.add(pending);
                }
                // ⚠️ RETURNING null REMOVES IT, and it is safe here for the
                // reason it was NOT safe before: an `offer` for this key can
                // only run before or after this whole function, never inside
                // it.
                return waiting.records.isEmpty() ? null : waiting;
            });
        }
        return expired;
    }

    /** Bytes held for one index, never above the per-index maximum. */
    public long bytesHeldFor(String index) {
        long[] held = {0};
        // ⚠️ READ UNDER THE SAME LOCK EVERY MUTATION TAKES. A plain `get`
        // followed by a field read can observe a count that belongs to neither
        // the before nor the after of a concurrent offer.
        byIndex.computeIfPresent(index, (key, waiting) -> {
            held[0] = waiting.bytes;
            return waiting;
        });
        return held[0];
    }

    /** How many indices currently have records waiting. */
    public int waitingIndices() {
        return byIndex.size();
    }
}
