// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import java.io.IOException;
import java.lang.System.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.indices.pollingingest.StreamPoller;

/**
 * Where this node's shard copies have COMMITTED, for {@link ProgressReporter}
 * (M8.43, M7.17, ADR-0051).
 *
 * <p>⚠️ **THE LAST LUCENE COMMIT's {@code batch_start}, AND NOTHING ELSE.** What
 * the consumer delivered is ahead of it by exactly the window retention exists
 * to protect, and {@code readNext}'s carried pointer freezes at the last reset.
 * A copy restarts from its commit, so the commit is what GC may trust.
 *
 * <p>⚠️ **READ FROM THE COMMIT's METADATA, WHICH PINS NOTHING.**
 * {@code IndexShard.acquireLastIndexCommit} returns a handle that pins the
 * commit's segment files until it is closed, and a reporter that leaked one per
 * interval would grow the node's disk silently. The store's own read of the
 * last committed segments info holds only a store reference, released here.
 *
 * <p>⚠️ **THE HANDLE IS AN INDEX-EVENT LISTENER**, because
 * {@code createComponents} is not given the node's shards: a shard of one of
 * this plugin's indices is added when it starts and dropped when it closes.
 */
final class ShardPositions implements IndexEventListener, ProgressReporter.Positions {

    private static final Logger LOG = System.getLogger(ShardPositions.class.getName());

    private final Map<ShardId, IndexShard> shards = new ConcurrentHashMap<>();

    ShardPositions() {}

    ShardPositions(Map<ShardId, IndexShard> shards) {
        this.shards.putAll(shards);
    }

    @Override
    public void afterIndexShardStarted(IndexShard shard) {
        shards.put(shard.shardId(), shard);
    }

    @Override
    public void beforeIndexShardClosed(ShardId id, IndexShard shard, Settings settings) {
        removeClosed(shards, id, shard);
    }

    static <T> boolean removeClosed(Map<ShardId, T> entries, ShardId id, T shard) {
        return entries.remove(id, shard);
    }

    boolean contains(ShardId id) {
        return shards.containsKey(id);
    }

    /**
     * Every started copy that has committed a position, read afresh.
     *
     * <p>⚠️ A COPY THAT CANNOT BE READ IS LEFT OUT, NOT THE WHOLE NODE: a
     * shard closing or relocating mid-read throws, and silence for that copy
     * already means "freeze it" to the ingester. A copy with no commit yet is
     * left out for the same reason.
     */
    @Override
    public List<ProgressReporter.ShardPosition> current() {
        List<ProgressReporter.ShardPosition> here = new ArrayList<>();
        for (IndexShard shard : shards.values()) {
            try {
                Long committed = committed(shard);
                if (committed != null) {
                    here.add(new ProgressReporter.ShardPosition(
                            shard.shardId().getIndex().getUUID(), shard.shardId().id(),
                            shard.routingEntry().allocationId().getId(), committed));
                }
            } catch (IOException | RuntimeException unreadable) {
                LOG.log(Logger.Level.DEBUG, () -> "no committed position for "
                        + shard.shardId() + " this interval: " + unreadable);
            }
        }
        return here;
    }

    private static Long committed(IndexShard shard) throws IOException {
        Store store = shard.store();
        if (!store.tryIncRef()) {
            return null;
        }
        try {
            String batchStart = store.readLastCommittedSegmentsInfo().getUserData()
                    .get(StreamPoller.BATCH_START);
            return batchStart == null ? null : BinStoreOffset.fromString(batchStart).offset();
        } finally {
            store.decRef();
        }
    }
}
