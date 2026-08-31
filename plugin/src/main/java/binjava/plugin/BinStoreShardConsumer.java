// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import binjava.client.ConsumerClient;
import binjava.client.ConsumerRecord;
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
    private volatile long tailOffset = -1;

    public BinStoreShardConsumer(int shardId, ConsumerClient client) {
        this.shardId = shardId;
        this.client = Objects.requireNonNull(client, "client");
    }

    @Override
    public List<ReadResult<BinStoreOffset, BinStoreMessage>> readNext(
            BinStoreOffset pointer, boolean includeStart, long maxMessages, int timeoutMillis) {
        List<ReadResult<BinStoreOffset, BinStoreMessage>> out = new ArrayList<>();
        drain(out, maxMessages, timeoutMillis,
                r -> includeStart ? r.offset() >= pointer.offset() : r.offset() > pointer.offset());
        return out;
    }

    @Override
    public List<ReadResult<BinStoreOffset, BinStoreMessage>> readNext(
            long maxMessages, int timeoutMillis) {
        List<ReadResult<BinStoreOffset, BinStoreMessage>> out = new ArrayList<>();
        drain(out, maxMessages, timeoutMillis, r -> true);
        return out;
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
        client.close();
    }
}
