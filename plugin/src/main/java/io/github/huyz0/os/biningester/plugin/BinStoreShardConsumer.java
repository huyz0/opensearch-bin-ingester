// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import io.github.huyz0.os.biningester.client.ConsumerClient;
import io.github.huyz0.os.biningester.client.ConsumerRecord;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;

/**
 * What OpenSearch's ingestion engine polls, one per shard.
 *
 * <p>WARNING: {@code timeoutMillis} IS OURS TO USE, and using it is the single
 * most important observation in the OpenSearch analysis. Nothing forbids
 * blocking inside {@code readNext} for up to that long, so an idle shard parks
 * on a queue instead of returning empty and being called straight back. That is
 * the hook that makes zero idle cost possible AT ALL -- without it, NFR-2 is
 * unreachable no matter how the storage layer behaves.
 *
 * <p>WARNING: {@code getPointerBasedLag} MUST NOT ISSUE A REQUEST. OpenSearch
 * calls it periodically even while ingestion is PAUSED, so a single store read
 * there would make an idle cluster cost money forever -- the exact failure
 * criterion 3 measures. It is served from the tail offset the subscription
 * already pushed.
 */
public final class BinStoreShardConsumer
        implements IngestionShardConsumer<BinStoreOffset, BinStoreMessage> {

    private final int shardId;
    private final ConsumerClient client;
    private final Runnable onClose;
    private volatile long tailOffset = -1;

    /**
     * The position a RESUME asked for, until the retained floor is known and
     * has been checked against it; -1 when there is nothing left to check.
     *
     * <p>⚠️ **KEPT, NOT CHECKED ONCE** (M8.6). The floor arrives on the
     * subscription, asynchronously, and can land after the resume's first
     * read. `DefaultStreamPoller` calls the pointer overload only on a forced
     * or reset pointer and the plain one after that, so a check made only in
     * the pointer overload, before the floor arrived, would refuse nothing for
     * the rest of the session.
     */
    private volatile long pendingResume = -1;

    /**
     * The client's floor-report count when the pending resume asked; only a
     * report past it may clear the resume (ADR-0056).
     */
    private volatile long freshAfter;

    /**
     * A consumer over a client it OWNS exclusively: closing this consumer
     * closes the client, because nothing else could be sharing it.
     */
    public BinStoreShardConsumer(int shardId, ConsumerClient client) {
        this.shardId = shardId;
        this.client = Objects.requireNonNull(client, "client");
        this.onClose = client::close;
    }

    /**
     * A consumer over the node's SHARED client for {@code key}. Closing this
     * consumer RELEASES that share rather than closing the client directly --
     * other shards of the same stream on this node may still hold it. This is
     * the constructor {@link BinStoreConsumerFactory} uses in production.
     */
    public BinStoreShardConsumer(int shardId, RunKey key, NodeSubscriptions subscriptions) {
        this.shardId = shardId;
        this.client = subscriptions.clientFor(key);
        this.onClose = () -> subscriptions.release(key);
    }

    @Override
    public List<ReadResult<BinStoreOffset, BinStoreMessage>> readNext(
            BinStoreOffset pointer, boolean includeStart, long maxMessages, int timeoutMillis) {
        pendingResume = includeStart ? pointer.offset() : pointer.offset() + 1;
        // ⚠️ A FRESH FLOOR, NOT THE ONE HELD. The client may have learned one
        // hours ago -- a node running since morning resets a shard in the
        // afternoon -- and GC has moved since. See `refuseIfCollected`.
        freshAfter = client.requestFreshFloor();
        refuseIfCollected();
        List<ReadResult<BinStoreOffset, BinStoreMessage>> out = new ArrayList<>();
        drain(out, maxMessages, timeoutMillis,
                r -> includeStart ? r.offset() >= pointer.offset() : r.offset() > pointer.offset());
        return out;
    }

    @Override
    public List<ReadResult<BinStoreOffset, BinStoreMessage>> readNext(
            long maxMessages, int timeoutMillis) {
        refuseIfCollected();
        List<ReadResult<BinStoreOffset, BinStoreMessage>> out = new ArrayList<>();
        drain(out, maxMessages, timeoutMillis, r -> true);
        return out;
    }

    /**
     * Refuses a resumed position below the retained floor (M7.16, M7.18, M8.6).
     *
     * <p>⚠️ **THROWN OUT OF {@code readNext}, WHICH PAUSES THE SHARD, AND THAT
     * IS THE POINT.** Research 02 §6: an exception from the poll path pauses
     * that shard until an operator intervenes, readable in
     * {@code _ingestion/_state}. That is exactly wrong for a gap -- whose
     * records may still arrive -- and exactly right here, because records below
     * the floor never will. Raised from consumer CONSTRUCTION instead it would
     * be swallowed: M6.8 measured {@code DefaultStreamPoller} catching whatever
     * {@code createShardConsumer} throws, logging at WARN and retrying for ever,
     * index green and nothing indexed.
     *
     * <p>⚠️ **UNCHECKED, CARRYING THE CHECKED ONE.** The SPI's {@code readNext}
     * declares nothing, so {@link PositionCollectedException} travels as the
     * cause, with the lost range in its message for the operator who reads it.
     *
     * <p>⚠️ **CLEARED ONLY ONCE A FLOOR REPORTED AFTER THE RESUME HAS BEEN
     * CHECKED AND PASSED.** An ingester that predates ADR-0056 never sends a
     * floor, so a shard resumed against one keeps CHECKING -- one field per
     * poll, no request -- while its client's bounded asks run out, and refuses
     * nothing, which is the behaviour before the floor existed.
     */
    private void refuseIfCollected() {
        long resume = pendingResume;
        if (resume < 0) {
            return;
        }
        boolean cleared;
        try {
            cleared = client.checkResume(resume, freshAfter);
        } catch (io.github.huyz0.os.biningester.client.PositionCollectedException collected) {
            throw new java.io.UncheckedIOException("shard " + shardId + " cannot resume: "
                    + collected.getMessage(), collected);
        }
        // ⚠️ CLEARED ONLY BY A FLOOR REPORTED AFTER THE RESUME ASKED. A held
        // floor may REFUSE -- floors only rise, so it is a true lower bound and
        // the check above uses it -- but it may not CLEAR: review found a
        // floor learned hours earlier passing a reset to a position GC had
        // since collected. Until a fresh one arrives the check simply repeats,
        // which is a read of one field per poll.
        // ⚠️ ONE CALL, count read first (M8.44): see `checkResume`.
        if (cleared) {
            pendingResume = -1;
        }
    }

    private void drain(List<ReadResult<BinStoreOffset, BinStoreMessage>> out, long maxMessages,
            int timeoutMillis, java.util.function.Predicate<ConsumerRecord> wanted) {
        // WARNING: the FIRST read blocks for the whole timeout; subsequent ones
        // do not. Blocking again after something arrived would hold a full batch
        // hostage to the timeout and add it to every batch's latency.
        Duration budget = Duration.ofMillis(timeoutMillis);
        while (out.size() < maxMessages) {
            Optional<ConsumerRecord> next;
            try {
                next = client.readNext(budget);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            if (next.isEmpty()) {
                return;
            }
            ConsumerRecord record = next.get();
            tailOffset = Math.max(tailOffset, record.offset());
            if (wanted.test(record)) {
                out.add(new ReadResult<>(new BinStoreOffset(record.offset()),
                        new BinStoreMessage(record.payload(), record.timestampMillis())));
            }
            budget = Duration.ZERO;
        }
    }

    /**
     * Gaps this shard's stream has reported (M6.1).
     *
     * <p>⚠️ IT IS READ, NEVER THROWN. Research doc 02 § 6: "Any exception
     * thrown from {@code readNext} pauses ingestion for that shard ... and
     * requires operator intervention to resume" -- so a dropped delivery, which
     * the records upstream survive, must not become a shard an operator has to
     * restart by hand. What the SPI offers instead is nothing at all: it has no
     * gap concept, so this is the surface an operator and a test read.
     */
    public long gapsDetected() {
        return client.gapsDetected();
    }

    @Override
    public IngestionShardPointer earliestPointer() {
        return new BinStoreOffset(0);
    }

    @Override
    public IngestionShardPointer latestPointer() {
        return new BinStoreOffset(Math.max(tailOffset, 0));
    }

    @Override
    public IngestionShardPointer pointerFromTimestampMillis(long timestampMillis) {
        // SKELETON: replaced by M2. The commit log records a first-timestamp per
        // (stream, object), so this becomes a binary search over checkpoints with
        // NO data reads -- better than the reference FilePartitionConsumer, which
        // simply returns earliestPointer(). M1 does the same thing it does.
        return earliestPointer();
    }

    @Override
    public IngestionShardPointer pointerFromOffset(String offset) {
        return BinStoreOffset.fromString(offset);
    }

    @Override
    public int getShardId() {
        return shardId;
    }

    @Override
    public long getPointerBasedLag(IngestionShardPointer expectedStartPointer) {
        // WARNING: SERVED FROM MEMORY. OpenSearch calls this periodically even
        // while ingestion is paused; one store read here and an idle cluster
        // costs money forever.
        if (!(expectedStartPointer instanceof BinStoreOffset from) || tailOffset < 0) {
            return 0;
        }
        return Math.max(0, tailOffset - from.offset());
    }

    @Override
    public void close() throws IOException {
        onClose.run();
    }
}
