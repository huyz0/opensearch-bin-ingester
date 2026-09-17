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
 *
 * <p>⚠️ THE UNIT IS A BATCH, NOT A RECORD, AND M6.6's ROUND-1 REVIEW IS WHY.
 * The first version keyed everything on the index alone, so the first waiter to
 * wake drained EVERY request's records for that index and wrote all of them at
 * the one partition ITS OWN routing value named. Measured: producer A's records
 * landed in producer B's partition while A got an exception for a write that
 * had happened, and reversing the wake order made B return 202 having written
 * nothing. A batch is one request's records with one routing value -- the
 * thing that is placed together and reported together.
 */
public final class PendingPool {

    /**
     * One request's records, waiting for its index's shape.
     *
     * <p>⚠️ IT IS THE OWNERSHIP BOUNDARY. Only the caller that opened a batch
     * takes it, so two producers writing to the same unregistered index cannot
     * take each other's records -- which is exactly what the index-keyed
     * version did, placing one producer's records at the other's partition.
     */
    public final class Batch {

        private final String index;
        private final String routing;
        private final List<SegmentRecord> records = new ArrayList<>();
        private final long openedAtMillis = clock.millis();
        private long bytes;
        private boolean settled;
        private boolean listed;

        private Batch(String index, String routing) {
            this.index = index;
            this.routing = routing;
        }

        /** The index this batch was written to -- an alias, as the producer sent it. */
        public String index() {
            return index;
        }

        /** The routing value every record in this batch is placed by. */
        public String routing() {
            return routing;
        }

        /**
         * Adds one record, if this index's pool has room.
         *
         * @return false when the index's pool is full, which the caller
         *     answers with a refusal rather than by placing the record
         *     somewhere
         */
        public boolean offer(SegmentRecord record) {
            Objects.requireNonNull(record, "record");
            long cost = costOf(record, routing);
            boolean[] accepted = {false};
            // ⚠️ THE CHARGE AND THE ADD ARE ONE OPERATION under the map's own
            // lock. Resolving the entry first and mutating it after is the
            // window M6.5's round-1 review measured, where a record landed in
            // an object nothing could reach.
            byIndex.compute(index, (key, waiting) -> {
                Waiting target = waiting == null ? new Waiting() : waiting;
                if (settled || target.bytes + cost > maxBytesPerIndex) {
                    return waiting;
                }
                records.add(record);
                bytes += cost;
                target.bytes += cost;
                // ⚠️ LISTED ONCE, however many records it takes. Adding on
                // every offer put a two-record batch in the list twice, so
                // `expire` handed the same batch to the sweeper twice and
                // `remove` dropped only the first copy -- a settled batch left
                // in the index's list forever, which is the leak the bound
                // exists to prevent arriving by another road.
                if (!listed) {
                    listed = true;
                    target.batches.add(this);
                }
                accepted[0] = true;
                return target;
            });
            return accepted[0];
        }

        /**
         * This batch's records, removed from the pool.
         *
         * <p>⚠️ EMPTY MEANS SOMEBODY ELSE SETTLED IT -- the sweeper expired it
         * while this caller was waiting. The caller refuses the write rather
         * than appending nothing, because appending nothing would return a
         * success for records that were dropped.
         */
        public List<SegmentRecord> take() {
            List<SegmentRecord> taken = new ArrayList<>();
            settle(taken);
            return taken;
        }

        /** Releases this batch without placing it, for a caller that is refusing. */
        public void discard() {
            settle(new ArrayList<>());
        }

        private void settle(List<SegmentRecord> into) {
            byIndex.compute(index, (key, waiting) -> {
                if (waiting == null || settled) {
                    settled = true;
                    return waiting;
                }
                settled = true;
                into.addAll(records);
                waiting.bytes -= bytes;
                waiting.batches.remove(this);
                records.clear();
                bytes = 0;
                return waiting.batches.isEmpty() ? null : waiting;
            });
        }

    }

    private static final class Waiting {
        private final List<Batch> batches = new ArrayList<>();
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

    /** Opens a batch for one request: one index, one routing value. */
    public Batch open(String index, String routing) {
        Objects.requireNonNull(index, "index");
        Objects.requireNonNull(routing, "routing");
        return new Batch(index, routing);
    }

    /**
     * ⚠️ THE SAME ESTIMATE THE FLUSH TRIGGER USES, so a record's cost is one
     * number across the whole write path rather than two that drift. The
     * routing value is counted once per RECORD because that is the granularity
     * the bound is spent at, and it is held here and nowhere else.
     */
    private static long costOf(SegmentRecord record, String routing) {
        return Accumulator.estimatedFramedBytes(record)
                + routing.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
    }

    /**
     * Every batch that has waited longer than the timeout, settled and removed.
     *
     * <p>⚠️ THE RECORDS ARE DROPPED HERE; WHAT COMES BACK IS THE BATCH, EMPTY.
     * A batch whose owner is still waiting learns it was expired by taking
     * nothing, and refuses its write on that. The returned batches are what an
     * eventual sweeper LOGS and METERS -- each names its index and its routing
     * value -- and which HTTP status a timed-out write gets belongs with the
     * adapter (ADR-0019, architecture.md rule 4), never here.
     *
     * <p>⚠️ NOTHING CALLS THIS IN PRODUCTION YET. Every routed write settles
     * its own batch, in a finally, so the pool does not leak without a sweeper;
     * what a sweeper adds is the bound on a wait that outlives its caller, and
     * the row that starts one is M6.6's own follow-up.
     *
     * <p>⚠️ AND IT SWEEPS EVERY INDEX, not the one a caller happens to ask
     * about: an index nobody writes to again would otherwise hold its records
     * forever, which is the leak the bound exists to prevent arriving by
     * another road.
     */
    public List<Batch> expire() {
        long now = clock.millis();
        List<Batch> expired = new ArrayList<>();
        for (String index : List.copyOf(byIndex.keySet())) {
            List<Batch> candidates = new ArrayList<>();
            byIndex.computeIfPresent(index, (key, waiting) -> {
                for (Batch batch : waiting.batches) {
                    if (now - batch.openedAtMillis >= timeout.toMillis()) {
                        candidates.add(batch);
                    }
                }
                return waiting;
            });
            for (Batch batch : candidates) {
                // ⚠️ SETTLED THROUGH THE BATCH ITSELF, so a batch its owner is
                // taking at the same moment is settled exactly once -- the
                // `settled` flag is read and written under the same map lock
                // every other mutation takes.
                List<SegmentRecord> dropped = batch.take();
                if (!dropped.isEmpty()) {
                    expired.add(batch);
                }
            }
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

    /** How long a batch may wait here before a sweeper may hand it back. */
    public Duration timeout() {
        return timeout;
    }

    /** How many indices currently have records waiting. */
    public int waitingIndices() {
        return byIndex.size();
    }

    /**
     * How many BATCHES are waiting, across every index.
     *
     * <p>⚠️ BATCHES, NOT INDICES, because that is the number a test asserting
     * two writes are genuinely IN FLIGHT at once needs: both requests to one
     * index are one index and two batches, and the index count cannot tell
     * them from a single request.
     */
    public int waitingBatches() {
        // ⚠️ READ THROUGH `computeIfPresent`, which is the lock every mutation
        // here takes -- reading `waiting.batches` from the value view would
        // observe an ArrayList mid-add.
        int[] total = {0};
        for (String index : byIndex.keySet()) {
            byIndex.computeIfPresent(index, (key, waiting) -> {
                total[0] += waiting.batches.size();
                return waiting;
            });
        }
        return total[0];
    }
}
