// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.BinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.SequencerTransport;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * A node that has never committed makes no store read for a floor, and sends
 * none (M8.46, NFR-2).
 *
 * <p>⚠️ **THE GUARD WAS UNPINNED**: {@code Assembly}'s floor source skips the
 * read below epoch 1, and deleting it left every test green. Without it, a
 * follower that never committed asks the store for a checkpoint of an epoch
 * that cannot exist -- a read per refresh, on a pod with nothing to report.
 */
class FloorGuardTest {

    private static final String PREFIX = "bins/cluster-a";

    private static ServerConfig config(String pod) {
        return new ServerConfig(pod, "az-a", "cluster-a", PREFIX,
                new StoreConfig("memory", Optional.empty()),
                Duration.ofSeconds(10), Duration.ofSeconds(3), "http://" + pod + ":8080",
                IngestConfig.defaults("cluster-a"), 0, "producer-1", Set.of("logs"), RetentionConfig.defaults(), java.util.Optional.empty(), "uid-" + pod, io.github.huyz0.os.biningester.ingest.CostTopKReporter.DEFAULT_INTERVAL, io.github.huyz0.os.biningester.ingest.IndexQuotas.Config.none(), false, java.util.Optional.empty(), PeerConfig.off(0));
    }

    private static SequencerTransport noPeers() {
        return new SequencerTransport() {
            @Override
            public CommitDelta send(String endpoint, CommitRequest request) {
                throw new UnsupportedOperationException("no forward expected: " + endpoint);
            }

            @Override
            public void close() {
            }
        };
    }

    /** Counts every store call that is not a write. */
    private static BinStore countingReads(BinStore real, AtomicInteger reads) {
        return (BinStore) Proxy.newProxyInstance(BinStore.class.getClassLoader(),
                new Class<?>[] {BinStore.class}, (proxy, method, args) -> {
                    if (Set.of("get", "getRange", "stat", "list").contains(method.getName())) {
                        reads.incrementAndGet();
                    }
                    try {
                        return method.invoke(real, args);
                    } catch (InvocationTargetException thrown) {
                        throw thrown.getCause();
                    }
                });
    }

    @Test
    void aNODEThatNeverCommittedREADSNothingForAFloorAndSENDSNone() throws Exception {
        try (BinStore shared = StoreFactory.open(new StoreConfig("memory", Optional.empty()));
                Assembly leader = Assembly.open(config("pod1"), shared, noPeers(),
                        Clock.systemUTC())) {
            assertThat(leader.leading()).as("the premise: pod1 holds the term").isTrue();
            AtomicInteger reads = new AtomicInteger();
            try (Assembly follower = Assembly.open(config("pod2"), countingReads(shared, reads),
                    noPeers(), Clock.systemUTC())) {
                assertThat(follower.leading()).as("the premise: pod2 does not").isFalse();
                int before = reads.get();

                java.util.OptionalLong floor = follower.floors()
                        .floorOf(new RunKey(UUID.randomUUID(), 0));

                assertThat(reads.get() - before)
                        .as("⚠️ ZERO READS: a node with no commit has no chain to read")
                        .isZero();
                assertThat(floor).as("and it sends no floor").isEmpty();
            }
        }
    }
}
