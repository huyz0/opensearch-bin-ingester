// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.http.EndpointSliceView;
import io.github.huyz0.os.biningester.ingest.ChainPublisher;
import io.github.huyz0.os.biningester.ingest.FetchPolicy;
import io.github.huyz0.os.biningester.ingest.FetchPolicyConfig;
import io.github.huyz0.os.biningester.ingest.IngestConfig;
import io.github.huyz0.os.biningester.ingest.SegmentProxy;
import io.github.huyz0.os.biningester.ingest.SegmentServing;
import io.github.huyz0.os.biningester.ingest.SubscriptionHub;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** Deltas committed before the chain publisher exists are held, in order (M10.20a). */
class DeltaDeliveryTest {

    private static final RunKey KEY = new RunKey(UUID.randomUUID(), 0);

    private static ServerConfig config() {
        return new ServerConfig("pod1", "az-a", "cluster-a", "bins/cluster-a",
                new StoreConfig("memory", Optional.empty()), Duration.ofSeconds(10),
                Duration.ofSeconds(3), "http://pod1:8080", IngestConfig.defaults("cluster-a"),
                0, "producer", Set.of("logs"));
    }

    private static CommitDelta delta(long sequence) {
        return new CommitDelta(sequence, "s" + sequence, List.of(new RunCommit(KEY, 2, 2 * sequence)));
    }

    @Test
    void deltasCommittedBeforeAttachAreDeliveredInOrderOnAttach() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        for (int i = 0; i < 3; i++) {
            store.put("s" + i, io.github.huyz0.os.biningester.binstore.Body.ofBytes(new byte[64]));
        }
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> got = new CopyOnWriteArrayList<>();
        try (var sub = hub.subscribe(KEY, SubscriptionHub.assembling(got::add));
                DeltaDelivery delivery = new DeltaDelivery(config(), null,
                        new EndpointSliceView(), (epoch, sequence) -> Optional.empty())) {
            delivery.committed(delta(0), 1);
            delivery.committed(delta(1), 1);
            ChainPublisher publisher = new ChainPublisher(hub, new SegmentServing(
                    new FetchPolicy(FetchPolicyConfig.defaultsFor(store.capabilities().costs())),
                    store.capabilities(), new SegmentProxy(store)), 0);
            delivery.attach(publisher);
            delivery.committed(delta(2), 1);

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (got.size() < 3 && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(got).extracting(SubscriptionHub.Push::segmentKey)
                    .as("the held ones first, then the next: none dropped as stale")
                    .containsExactly("s0", "s1", "s2");
            assertThat(delivery.droppedBeforeAttach()).isZero();
        }
    }

    @Test
    void theHoldIsBoundedAndCountsWhatItDrops() throws Exception {
        try (DeltaDelivery delivery = new DeltaDelivery(config(), null, new EndpointSliceView(),
                (epoch, sequence) -> Optional.empty())) {
            for (int i = 0; i < DeltaDelivery.PENDING_LIMIT + 2; i++) {
                delivery.committed(delta(i), 1);
            }
            delivery.pushed(1, delta(0));

            assertThat(delivery.droppedBeforeAttach()).isEqualTo(3);
        }
    }

    @Test
    void theHookIsInstalledBeforeTheDrainRunsSoADrainCommitIsNeverMissed() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        store.put("seg/drained", io.github.huyz0.os.biningester.binstore.Body.ofBytes(new byte[64]));
        io.github.huyz0.os.biningester.sequencer.LeaseManager leases =
                new io.github.huyz0.os.biningester.sequencer.LeaseManager(store,
                        new io.github.huyz0.os.biningester.sequencer.LeaseConfig(
                                "bins/cluster-a", "pod1", "", Duration.ofSeconds(10),
                                Duration.ofSeconds(3)), java.time.Clock.systemUTC());
        io.github.huyz0.os.biningester.sequencer.LocalSequencer term =
                io.github.huyz0.os.biningester.sequencer.LocalSequencer.start(store,
                        "bins/cluster-a", leases, 8).orElseThrow();
        SubscriptionHub hub = new SubscriptionHub();
        List<SubscriptionHub.Push> got = new CopyOnWriteArrayList<>();
        try (var sub = hub.subscribe(KEY, SubscriptionHub.assembling(got::add));
                DeltaDelivery delivery = new DeltaDelivery(config(), null,
                        new EndpointSliceView(), (epoch, sequence) -> Optional.empty())) {
            // A drain that commits AT ONCE, the worst case for a hook put on after it.
            delivery.hookThenDrain(term, () -> {
                try {
                    term.commitAll(List.of(new io.github.huyz0.os.biningester.sequencer.CommitRequest(
                            "podx", "i1", 0, "seg/drained", java.util.Map.of(KEY, 2))));
                } catch (java.io.IOException e) {
                    throw new java.io.UncheckedIOException(e);
                }
            });
            delivery.attach(new ChainPublisher(hub, new SegmentServing(
                    new FetchPolicy(FetchPolicyConfig.defaultsFor(store.capabilities().costs())),
                    store.capabilities(), new SegmentProxy(store)), 0));

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (got.isEmpty() && System.nanoTime() < deadline) {
                Thread.onSpinWait();
            }
            assertThat(got).extracting(SubscriptionHub.Push::segmentKey)
                    .as("the drain's commit was seen by the hook, held, and delivered")
                    .containsExactly("seg/drained");
        } finally {
            term.close();
        }
    }
}
