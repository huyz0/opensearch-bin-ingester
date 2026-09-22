// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server.chaos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.StoreCounts;
import io.github.huyz0.os.biningester.binstore.backend.S3Fixture;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.ingest.FetchPolicy;
import io.github.huyz0.os.biningester.ingest.FetchPolicyConfig;
import io.github.huyz0.os.biningester.ingest.SegmentCache;
import io.github.huyz0.os.biningester.ingest.SegmentProxy;
import io.github.huyz0.os.biningester.ingest.SegmentServing;
import io.github.huyz0.os.biningester.ingest.SegmentSink;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import io.github.huyz0.os.biningester.plugin.NodeSubscriptions;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** M9.9: RustFS read requests stay flat in consumer-node and shard fan-out. */
@Timeout(300)
class ReadRequestRateIT {

    private static final String SEGMENT = "bins/cluster-a/data/read-rate-segment";
    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000a9");
    private static final int CONSUMERS_AT_FLATNESS_POINT = 9;
    private static final int SMALL_SHARDS = 16;
    private static final int LARGE_SHARDS = 1_600;

    @Test
    void storeGetsStayFlatAcrossConsumerNodesAndShards() throws Exception {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a T3 suite");
        try (ChaosBucket bucket = ChaosBucket.create()) {
            CountingBinStore store = new CountingBinStore(bucket.observer());
            store.put(SEGMENT, Body.ofBytes(segmentBytes()));

            nodeRegistrationStaysMergedAt1600Shards();

            long oneConsumer = getsForConsumers(store, 1, SMALL_SHARDS, 1);
            long nineConsumers = getsForConsumers(store, CONSUMERS_AT_FLATNESS_POINT,
                    SMALL_SHARDS, 1);
            long threeServingNodes = getsForConsumers(store, CONSUMERS_AT_FLATNESS_POINT,
                    SMALL_SHARDS, 3);
            long sixteenShards = getsForConsumers(store, CONSUMERS_AT_FLATNESS_POINT,
                    SMALL_SHARDS, 1);
            long sixteenHundredShards = getsForConsumers(store, CONSUMERS_AT_FLATNESS_POINT,
                    LARGE_SHARDS, 1);

            System.out.println("M9.9 GET deltas: oneConsumer=" + oneConsumer
                    + ", nineConsumers=" + nineConsumers
                    + ", threeServingNodes=" + threeServingNodes
                    + ", sixteenShards=" + sixteenShards
                    + ", sixteenHundredShards=" + sixteenHundredShards);

            assertThat(oneConsumer)
                    .as("one serving ingester node pays one cold GET")
                    .isEqualTo(1);
            assertThat(nineConsumers)
                    .as("the segment cache absorbs eight additional consumer nodes")
                    .isEqualTo(oneConsumer);
            assertThat(threeServingNodes)
                    .as("three serving ingester nodes pay one cold GET each")
                    .isEqualTo(3);
            assertThat(sixteenHundredShards)
                    .as("1,600 shards cannot turn one segment into per-shard GETs")
                    .isLessThan((long) Math.ceil(sixteenShards * 1.2));
        }
    }

    private static long getsForConsumers(CountingBinStore store, int consumerNodes, int shards,
            int servingNodes) throws Exception {
        List<SubscriptionHub> hubs = new ArrayList<>(servingNodes);
        List<SegmentServing> servings = new ArrayList<>(servingNodes);
        List<List<RunCommit>> runsByServingNode = new ArrayList<>(servingNodes);
        for (int node = 0; node < servingNodes; node++) {
            hubs.add(new SubscriptionHub());
            servings.add(new SegmentServing(
                    new FetchPolicy(new FetchPolicyConfig(Long.MAX_VALUE, Long.MAX_VALUE, 0,
                            false)),
                    store.capabilities(),
                    new SegmentProxy(store, 1024, new SegmentCache(segmentBytes().length))));
            runsByServingNode.add(new ArrayList<>());
        }
        List<AutoCloseable> subscriptions = new ArrayList<>();
        AtomicInteger opened = new AtomicInteger();
        AtomicLong deliveredBytes = new AtomicLong();
        List<SubscriptionHub.Subscriber> consumers = new ArrayList<>(consumerNodes);
        for (int consumer = 0; consumer < consumerNodes; consumer++) {
            consumers.add(pushes -> {
                opened.incrementAndGet();
                return (bytes, offset, length) -> deliveredBytes.addAndGet(length);
            });
        }
        for (int shard = 0; shard < shards; shard++) {
            RunKey key = new RunKey(INDEX, shard);
            RunCommit run = new RunCommit(key, 1, shard);
            int consumer = shard % consumerNodes;
            int servingNode = consumer % servingNodes;
            runsByServingNode.get(servingNode).add(run);
            subscriptions.add(hubs.get(servingNode).subscribe(key, consumers.get(consumer)));
        }

        StoreCounts before = store.counts();
        for (int node = 0; node < servingNodes; node++) {
            List<RunCommit> runs = runsByServingNode.get(node);
            if (runs.isEmpty()) {
                continue;
            }
            hubs.get(node).publish(new CommitDelta(1, SEGMENT, runs), null, null,
                    servings.get(node));
            // The writer's in-memory bytes are the inline arm. It must serve
            // the same subscribers without buying a store GET.
            hubs.get(node).publish(new CommitDelta(1, SEGMENT, runs), SEGMENT,
                    segmentBytes(), servings.get(node));
            // Publish one more time to model a late consumer replay. A disabled
            // cache turns this into one GET per consumer node; the configured
            // serving-node cache keeps it at one per serving node.
            hubs.get(node).publish(new CommitDelta(1, SEGMENT, runs), null, null,
                    servings.get(node));
        }
        for (AutoCloseable subscription : subscriptions) {
            subscription.close();
        }

        assertThat(opened.get())
                .as("every consumer node is opened for all three segment deliveries")
                .isEqualTo(consumerNodes * 3);
        assertThat(deliveredBytes.get()).isEqualTo(3L * consumerNodes * segmentBytes().length);
        return store.counts().gets() - before.gets();
    }

    /** The production plugin must merge all shard registrations on one node. */
    private static void nodeRegistrationStaysMergedAt1600Shards() {
        AtomicInteger singleSubscriptions = new AtomicInteger();
        AtomicInteger mergedSubscriptions = new AtomicInteger();
        java.util.Set<RunKey> addedKeys = new java.util.HashSet<>();
        java.util.Set<RunKey> expectedKeys = new java.util.HashSet<>();
        SubscriptionTransport transport = new SubscriptionTransport() {
            @Override
            public AutoCloseable subscribe(RunKey key, Listener listener) {
                singleSubscriptions.incrementAndGet();
                return () -> { };
            }

            @Override
            public MultiSubscription subscribe(List<RunKey> keys, Listener listener) {
                mergedSubscriptions.incrementAndGet();
                return new MultiSubscription() {
                    @Override
                    public void add(RunKey key) {
                        addedKeys.add(key);
                    }

                    @Override
                    public void remove(RunKey key) {
                    }

                    @Override
                    public void close() {
                    }
                };
            }
        };
        try (NodeSubscriptions node = new NodeSubscriptions(transport, 16)) {
            for (int shard = 0; shard < LARGE_SHARDS; shard++) {
                RunKey key = new RunKey(INDEX, shard);
                expectedKeys.add(key);
                node.clientFor(key);
            }
            assertThat(mergedSubscriptions)
                    .as("one merged subscription represents all shards on this consumer node")
                    .hasValue(1);
            assertThat(singleSubscriptions)
                    .as("the consumer node must not open one transport subscription per shard")
                    .hasValue(0);
            assertThat(addedKeys)
                    .as("all 1,600 distinct shard keys must be added to the merged registration")
                    .containsExactlyInAnyOrderElementsOf(expectedKeys);
        }
    }

    private static byte[] segmentBytes() {
        byte[] bytes = new byte[64 * 1024];
        byte[] marker = "m9.9-read-rate".getBytes(StandardCharsets.UTF_8);
        for (int offset = 0; offset < bytes.length; offset++) {
            bytes[offset] = marker[offset % marker.length];
        }
        return bytes;
    }
}
