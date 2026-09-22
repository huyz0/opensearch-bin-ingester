// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.common.settings.Settings;
import sun.misc.Unsafe;

/** A late close must not evict a replacement shard with the same ShardId (M8.71). */
class ShardPositionsCloseTest {

    @Test
    void aLATECloseRemovesOnlyTheSameShardInstance() {
        ShardId id = new ShardId(new Index("logs", "uuid"), 0);
        IndexShard oldShard = uninitializedIndexShard();
        IndexShard replacement = uninitializedIndexShard();
        Map<ShardId, IndexShard> shards = new ConcurrentHashMap<>();
        shards.put(id, replacement);
        ShardPositions positions = new ShardPositions(shards);

        positions.beforeIndexShardClosed(id, oldShard, Settings.EMPTY);
        assertThat(positions.contains(id))
                .as("a close callback for an old instance must not evict its replacement")
                .isTrue();

        positions.beforeIndexShardClosed(id, replacement, Settings.EMPTY);
        assertThat(positions.contains(id)).isFalse();
    }

    private static IndexShard uninitializedIndexShard() {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            return (IndexShard) ((Unsafe) field.get(null)).allocateInstance(IndexShard.class);
        } catch (ReflectiveOperationException exception) {
            throw new AssertionError("could not allocate an identity-only IndexShard", exception);
        }
    }
}
