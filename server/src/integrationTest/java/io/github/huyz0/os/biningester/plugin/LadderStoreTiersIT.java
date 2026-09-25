// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.huyz0.os.biningester.binstore.Body;
import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.StoreCounts;
import io.github.huyz0.os.biningester.binstore.backend.S3Fixture;
import io.github.huyz0.os.biningester.client.ConsumerClient;
import io.github.huyz0.os.biningester.client.Delivery;
import io.github.huyz0.os.biningester.client.FallbackLadder;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.Checkpoint;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import io.github.huyz0.os.biningester.server.chaos.ChaosBucket;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** M9.45: Tier 2 and Tier 3 recover a shared live gap from RustFS during outage. */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class LadderStoreTiersIT {

    private static final String PREFIX = ChaosBucket.PREFIX;
    private static final long EPOCH = 7;
    private static final int SUBSCRIPTIONS = 16;

    @Test
    void oneNodeMovesFromTierTwoToContiguousTierThreeRecoveryWithoutLists() throws Exception {
        assumeTrue(S3Fixture.dockerAvailable(), "no Docker daemon: this is a T3 suite");

        try (ChaosBucket bucket = ChaosBucket.create()) {
            CountingBinStore store = new CountingBinStore(bucket.observer());
            List<RunKey> keys = java.util.stream.IntStream.range(0, SUBSCRIPTIONS)
                    .mapToObj(i -> new RunKey(new UUID(1, i + 1), 0)).toList();
            String root = PREFIX + "/ctl/log/0/%016x/".formatted(EPOCH);
            String firstKey = PREFIX + "/data/first.bseg";
            String gapKey = PREFIX + "/data/gap.bseg";
            String tailKey = PREFIX + "/data/tail.bseg";
            String restoredGapKey = PREFIX + "/data/restored-gap.bseg";
            String restoredTailKey = PREFIX + "/data/restored-tail.bseg";
            byte[] first = segment(keys, "first");
            byte[] gap = segment(keys, "missing");
            byte[] tail = segment(keys, "tail");
            byte[] restoredGap = segment(keys, "restored-missing");
            byte[] restoredTail = segment(keys, "restored-tail");
            put(store, root + "ckpt/LATEST", new Checkpoint(0, Map.of(), Map.of()).encode());
            put(store, root + "%016x.delta".formatted(0), delta(0, firstKey, keys, 0));
            put(store, root + "%016x.delta".formatted(1), delta(1, gapKey, keys, 1));
            put(store, root + "%016x.delta".formatted(2), delta(2, tailKey, keys, 2));
            put(store, root + "%016x.delta".formatted(3), delta(3, restoredGapKey, keys, 3));
            put(store, root + "%016x.delta".formatted(4), delta(4, restoredTailKey, keys, 4));
            put(store, gapKey, gap);
            put(store, restoredGapKey, restoredGap);
            StoreCounts before = store.counts();

            AtomicBoolean ingesterAnswers = new AtomicBoolean(false);
            AtomicReference<String> restoreAtSegment = new AtomicReference<>();
            List<String> tierThreeGets = new ArrayList<>();
            RecordingTransport transport = new RecordingTransport(ingesterAnswers,
                    restoredGapKey, restoredGap);
            try (NodeSubscriptions node = new NodeSubscriptions(transport, 32)) {
                List<ConsumerClient> consumers = keys.stream().map(node::clientFor).toList();
                node.enableTierTwo((epoch, sequence) -> {
                    String key = "%s/ctl/log/0/%016x/%016x.delta"
                            .formatted(PREFIX, epoch, sequence);
                    try (InputStream body = store.get(key)) {
                        return Optional.of(CommitDelta.decode(body.readAllBytes()));
                    } catch (IOException missing) {
                        return Optional.empty();
                    }
                }, ingesterAnswers::get, node::offerTierTwoDelta, () -> { });
                node.enableTierThree(new TierThreeRecovery("unused-by-in-process-reader", PREFIX,
                        new TierThreeRecovery.Reader() {
                            @Override
                            public OptionalLong stat(String ignoredBucket, String ignoredPrefix,
                                    String key) throws IOException {
                                return store.stat(key).map(stat -> OptionalLong.of(stat.size()))
                                        .orElseGet(OptionalLong::empty);
                            }

                            @Override
                            public Optional<InputStream> get(String ignoredBucket,
                                    String ignoredPrefix, String key) throws IOException {
                                try {
                                    tierThreeGets.add(key);
                                    InputStream body = store.get(key);
                                    if (key.equals(restoreAtSegment.get())) {
                                        // The ingester returns while Tier 3 is in progress.
                                        ingesterAnswers.set(true);
                                    }
                                    return Optional.of(body);
                                } catch (IOException absent) {
                                    return Optional.empty();
                                }
                            }
                        }));
                NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(transport, node,
                        Optional::empty);

                for (int i = 0; i < keys.size(); i++) {
                    transport.deliver(new Delivery(keys.get(i), firstKey, 1, 0,
                            FetchMode.INLINE, first, null, EPOCH, 0));
                    assertThat(consumers.get(i).readNext(Duration.ZERO).orElseThrow().offset())
                            .isZero();
                }
                for (int i = 0; i < keys.size(); i++) {
                    transport.deliver(new Delivery(keys.get(i), tailKey, 1, 2,
                            FetchMode.INLINE, tail, null, EPOCH, 2));
                    assertThat(consumers.get(i).readNext(Duration.ZERO)).isEmpty();
                    assertThat(consumers.get(i).lastGap()).isPresent();
                }

                // Tier 2's one node-wide GET discovers the next committed delta.
                node.pollTierTwo(1);
                assertThat(node.takeTierTwoDelta().sequence()).isEqualTo(3);

                // The catch-up endpoint is unavailable everywhere; Tier 3 replays RustFS.
                coordinator.attempt();

                for (ConsumerClient consumer : consumers) {
                    assertThat(consumer.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(1);
                    assertThat(consumer.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(2);
                    assertThat(consumer.readNext(Duration.ZERO)).isEmpty();
                }
                assertThat(transport.catchUpAttempts).isEqualTo(1);
                StoreCounts recovered = store.counts();
                assertThat(recovered.gets() - before.gets()).isEqualTo(6);
                assertThat(recovered.gets() - before.gets() - 1)
                        .as("Tier 3 GETs: checkpoint, three ordered deltas, and one shared gap segment")
                        .isEqualTo(FallbackLadder.AutomaticTier.RECOVER.getsPerNodePerInterval());
                assertThat(recovered.stats() - before.stats()).isEqualTo(1);
                assertThat(recovered.lists() - before.lists()).isZero();

                // Start another gap; RustFS makes the ingester return during Tier 3.
                for (int i = 0; i < keys.size(); i++) {
                    transport.deliver(new Delivery(keys.get(i), restoredTailKey, 1, 4,
                            FetchMode.INLINE, restoredTail, null, EPOCH, 4));
                    assertThat(consumers.get(i).readNext(Duration.ZERO)).isEmpty();
                }
                restoreAtSegment.set(restoredGapKey);
                coordinator.attempt();
                assertThat(ingesterAnswers.get()).isTrue();
                assertThat(transport.catchUpAttempts).isEqualTo(2);
                StoreCounts afterInterruptedFallback = store.counts();
                assertThat(afterInterruptedFallback.gets() - recovered.gets())
                        .as("an interrupted Tier 3 episode reads the checkpoint, four deltas, "
                                + "and its one shared missing segment, within the 30-GET cap: %s",
                                tierThreeGets)
                        .isEqualTo(6);
                assertThat(afterInterruptedFallback.stats() - recovered.stats()).isEqualTo(1);
                assertThat(afterInterruptedFallback.lists() - recovered.lists()).isZero();

                // The answering ingester resumes the held gap; fallback stays idle thereafter.
                node.pollTierTwo(2);
                coordinator.attempt();
                for (ConsumerClient consumer : consumers) {
                    assertThat(consumer.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(3);
                    assertThat(consumer.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(4);
                    assertThat(consumer.readNext(Duration.ZERO)).isEmpty();
                }
                StoreCounts afterIngesterResume = store.counts();
                node.pollTierTwo(3);
                coordinator.attempt();
                assertThat(store.counts()).isEqualTo(afterIngesterResume);
            }
        }
    }

    private static void put(CountingBinStore store, String key, byte[] bytes) throws IOException {
        store.put(key, Body.ofBytes(bytes));
    }

    private static byte[] segment(List<RunKey> keys, String id) throws IOException {
        SegmentWriter writer = new SegmentWriter();
        for (RunKey key : keys) {
            writer.add(key, new SegmentRecord(id + key.indexId(), OpType.INDEX,
                    OptionalLong.of(1), id.getBytes(StandardCharsets.UTF_8)), 1L);
        }
        return writer.toByteArray(1L);
    }

    private static byte[] delta(long sequence, String segmentKey, List<RunKey> keys, long offset) {
        return new CommitDelta(sequence, List.of(new SegmentCommit(segmentKey, keys.stream()
                .map(key -> new RunCommit(key, 1, offset)).toList()))).encode();
    }

    private static final class RecordingTransport implements SubscriptionTransport {
        private final AtomicBoolean answers;
        private final String restoredGapKey;
        private final byte[] restoredGap;
        private final List<Listener> listeners = new ArrayList<>();
        private int catchUpAttempts;

        RecordingTransport(AtomicBoolean answers, String restoredGapKey, byte[] restoredGap) {
            this.answers = answers;
            this.restoredGapKey = restoredGapKey;
            this.restoredGap = restoredGap;
        }

        @Override
        public boolean ingesterAnswers() {
            return answers.get();
        }

        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            listeners.add(listener);
            return () -> listeners.remove(listener);
        }

        @Override
        public CatchUpResult requestCatchUp(
                io.github.huyz0.os.biningester.format.CatchUpRequestFrame request,
                java.util.function.Consumer<io.github.huyz0.os.biningester.format.SubscriptionEvent> lane)
                throws IOException {
            catchUpAttempts++;
            if (!answers.get()) {
                throw new IOException("all ingester endpoints are unreachable");
            }
            for (var stream : request.streams()) {
                lane.accept(new io.github.huyz0.os.biningester.format.SubscriptionEvent(
                        UUID.randomUUID().toString(), EPOCH, 1, stream.key(), restoredGapKey,
                        stream.batchStart(), 1, FetchMode.INLINE, restoredGap, null,
                        io.github.huyz0.os.biningester.format.SubscriptionEvent.RANGE_ABSENT,
                        io.github.huyz0.os.biningester.format.SubscriptionEvent.RANGE_ABSENT, 3));
            }
            return CatchUpResult.COMPLETE;
        }

        void deliver(Delivery delivery) {
            listeners.getFirst().onDelivery(delivery);
        }
    }
}
