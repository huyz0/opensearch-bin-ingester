// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.client.ConsumerClient;
import io.github.huyz0.os.biningester.client.Delivery;
import io.github.huyz0.os.biningester.client.SubscriptionTransport;
import io.github.huyz0.os.biningester.format.Checkpoint;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class TierThreeRecoveryTest {

    @Test
    void nodeGapFallsBackToLocalTierThreeAndReplaysSharedRunsContiguously() throws Exception {
        List<RunKey> keys = java.util.stream.IntStream.range(0, 16)
                .mapToObj(i -> new RunKey(new UUID(2, i + 1), 0)).toList();
        byte[] firstBytes = segment(keys, "first");
        byte[] gapBytes = segment(keys, "gap");
        byte[] tailBytes = segment(keys, "tail");
        String firstKey = "prefix/data/2030/01/01/00/0000000000000000001-pod-0000000000000000-h1-all.bseg";
        String gapKey = "prefix/data/2030/01/01/00/0000000000000000002-pod-0000000000000000-h1-all.bseg";
        String tailKey = "prefix/data/2030/01/01/00/0000000000000000003-pod-0000000000000000-h1-all.bseg";
        Checkpoint checkpoint = new Checkpoint(0, Map.of(), Map.of());
        Map<String, byte[]> objects = new HashMap<>();
        String root = "prefix/ctl/log/0/0000000000000004/";
        objects.put(root + "ckpt/LATEST", checkpoint.encode());
        objects.put(root + "0000000000000000.delta", delta(0, firstKey, keys, 0));
        objects.put(root + "0000000000000001.delta", delta(1, gapKey, keys, 1));
        objects.put(root + "0000000000000002.delta", delta(2, tailKey, keys, 2));
        objects.put(gapKey, gapBytes);
        MapReader reader = new MapReader(objects);
        FailingTransport transport = new FailingTransport();
        try (NodeSubscriptions clients = new NodeSubscriptions(transport, 8)) {
            List<ConsumerClient> consumers = keys.stream().map(clients::clientFor).toList();
            clients.enableTierThree(new TierThreeRecovery("bucket", "prefix", reader));
            NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(transport, clients,
                    Optional::<List<io.github.huyz0.os.biningester.format.CatchUpRequestFrame.Stream>>empty);
            for (int i = 0; i < keys.size(); i++) {
                RunKey key = keys.get(i);
                consumers.get(i).deliver(new Delivery(key, firstKey, 1, 0, FetchMode.INLINE,
                        firstBytes, null, 4, 0));
                assertThat(consumers.get(i).readNext(Duration.ZERO).orElseThrow().offset())
                        .isZero();
                consumers.get(i).deliver(new Delivery(key, tailKey, 1, 2, FetchMode.INLINE,
                        tailBytes, null, 4, 2));
            }
            for (ConsumerClient consumer : consumers) {
                assertThat(consumer.readNext(Duration.ZERO)).isEmpty();
            }

            coordinator.attempt();

            assertThat(reader.stats).hasSize(1);
            assertThat(reader.gets).containsExactly(root + "ckpt/LATEST",
                    root + "0000000000000000.delta", root + "0000000000000001.delta",
                    gapKey, root + "0000000000000002.delta");
            assertThat(reader.lists).isZero();
            for (ConsumerClient consumer : consumers) {
                assertThat(consumer.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(1);
                assertThat(consumer.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(2);
                assertThat(consumer.lastGap()).isPresent();
            }
        }
    }

    @Test
    void oneNodeEpisodeSharesSegmentsAndDeliversOnlyAfterTheWholeGapIsReadable()
            throws Exception {
        List<RunCommit> runs = new ArrayList<>();
        Map<RunKey, TierThreeRecovery.Gap> gaps = new HashMap<>();
        for (int i = 0; i < 16; i++) {
            RunKey key = new RunKey(new UUID(0, i + 1), 0);
            runs.add(new RunCommit(key, 1, 0));
            gaps.put(key, new TierThreeRecovery.Gap(0, 1));
        }
        String segmentKey = "prefix/data/2030/01/01/00/0000000000000000001-pod-0000000000000000-h1-all.bseg";
        Checkpoint checkpoint = new Checkpoint(0, Map.of(), Map.of());
        byte[] segmentBytes = segment(new ArrayList<>(gaps.keySet()), "replay");
        CommitDelta delta = new CommitDelta(0, List.of(new SegmentCommit(segmentKey, runs)));
        RecordingReader reader = new RecordingReader(checkpoint.encode(), delta.encode(),
                segmentBytes);
        List<SubscriptionEvent> delivered = new ArrayList<>();

        boolean complete = new TierThreeRecovery("bucket", "prefix", reader)
                .recover(4, 0, gaps, delivered::add);

        assertThat(complete).isTrue();
        assertThat(reader.stats).containsExactly("prefix/ctl/log/0/0000000000000004/ckpt/LATEST");
        assertThat(reader.gets).containsExactly(
                "prefix/ctl/log/0/0000000000000004/ckpt/LATEST",
                "prefix/ctl/log/0/0000000000000004/0000000000000000.delta", segmentKey);
        assertThat(reader.lists).isZero();
        assertThat(delivered).hasSize(16);
        assertThat(delivered).allSatisfy(event -> {
            assertThat(event.inline()).containsExactly(segmentBytes);
            assertThat(event.chainSequence()).isZero();
        });
    }

    @Test
    void anUnavailableSegmentLeavesEveryGapUnrepairedAndCursorParked() throws Exception {
        RunKey key = new RunKey(new UUID(0, 1), 0);
        String segmentKey = "prefix/data/2030/01/01/00/0000000000000000001-pod-0000000000000000-h1-all.bseg";
        Checkpoint checkpoint = new Checkpoint(0, Map.of(), Map.of());
        CommitDelta delta = new CommitDelta(0, segmentKey, List.of(new RunCommit(key, 1, 0)));
        RecordingReader reader = new RecordingReader(checkpoint.encode(), delta.encode(), null);
        List<SubscriptionEvent> delivered = new ArrayList<>();

        boolean complete = new TierThreeRecovery("bucket", "prefix", reader)
                .recover(4, 0, Map.of(key, new TierThreeRecovery.Gap(0, 1)), delivered::add);

        assertThat(complete).isFalse();
        assertThat(delivered).isEmpty();
        assertThat(reader.gets).hasSize(3);
        assertThat(reader.lists).isZero();
    }

    @Test
    void pointerDeltaAndSegmentFailuresNeverPartiallyReleaseTheGap() throws Exception {
        RunKey key = new RunKey(new UUID(0, 1), 0);
        String root = "prefix/ctl/log/0/0000000000000004/";
        String pointer = root + "ckpt/LATEST";
        String deltaKey = root + "0000000000000000.delta";
        String segmentKey = "prefix/data/2030/01/01/00/0000000000000000001-pod-0000000000000000-h1-all.bseg";
        Map<String, byte[]> objects = Map.of(pointer,
                new Checkpoint(0, Map.of(), Map.of()).encode(), deltaKey,
                new CommitDelta(0, segmentKey, List.of(new RunCommit(key, 1, 0))).encode(),
                segmentKey, segment(List.of(key), "replay"));

        for (Failure failure : Failure.values()) {
            FaultReader reader = new FaultReader(objects, failure, pointer, deltaKey, segmentKey);
            List<SubscriptionEvent> delivered = new ArrayList<>();
            boolean complete = new TierThreeRecovery("bucket", "prefix", reader)
                    .recover(4, 0, Map.of(key, new TierThreeRecovery.Gap(0, 1)), delivered::add);

            assertThat(complete).as(failure.toString()).isFalse();
            assertThat(delivered).as(failure.toString()).isEmpty();
            assertThat(reader.stats).as(failure.toString()).containsExactly(pointer);
            assertThat(reader.lists).as(failure.toString()).isZero();
            if (failure == Failure.STAT) {
                assertThat(reader.gets).as(failure.toString()).isEmpty();
            } else if (failure == Failure.POINTER_MISSING
                    || failure == Failure.POINTER_CORRUPT) {
                assertThat(reader.gets).as(failure.toString()).containsOnly(pointer);
            } else if (failure == Failure.DELTA_MISSING || failure == Failure.DELTA_CORRUPT) {
                assertThat(reader.gets).as(failure.toString())
                        .containsExactly(pointer, deltaKey);
            } else {
                assertThat(reader.gets).as(failure.toString())
                        .containsExactly(pointer, deltaKey, segmentKey);
            }
        }
    }

    @Test
    void refusesTheThirtyFirstGetBeforeIssuingIt() throws Exception {
        List<RunCommit> runs = new ArrayList<>();
        Map<RunKey, TierThreeRecovery.Gap> gaps = new HashMap<>();
        List<String> segmentKeys = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            RunKey key = new RunKey(new UUID(1, i + 1), 0);
            runs.add(new RunCommit(key, 1, 0));
            gaps.put(key, new TierThreeRecovery.Gap(0, 1));
            segmentKeys.add("prefix/data/2030/01/01/00/0000000000000000001-pod-"
                    + String.format("%016x", i) + "-0000000000000000-h1-all.bseg");
        }
        List<SegmentCommit> segments = new ArrayList<>();
        for (int i = 0; i < runs.size(); i++) {
            segments.add(new SegmentCommit(segmentKeys.get(i), List.of(runs.get(i))));
        }
        Checkpoint checkpoint = new Checkpoint(0, Map.of(), Map.of());
        CommitDelta delta = new CommitDelta(0, segments);
        RecordingReader reader = new RecordingReader(checkpoint.encode(), delta.encode(),
                segment(new ArrayList<>(gaps.keySet()), "replay"));
        List<SubscriptionEvent> delivered = new ArrayList<>();

        boolean complete = new TierThreeRecovery("bucket", "prefix", reader)
                .recover(4, 0, gaps, delivered::add);

        assertThat(complete).isFalse();
        assertThat(reader.gets).hasSize(30);
        assertThat(delivered).isEmpty();
        assertThat(reader.lists).isZero();
    }

    private static final class RecordingReader implements TierThreeRecovery.Reader {
        private final byte[] checkpoint;
        private final byte[] delta;
        private final byte[] segment;
        private final List<String> stats = new ArrayList<>();
        private final List<String> gets = new ArrayList<>();
        private int lists;

        RecordingReader(byte[] checkpoint, byte[] delta, byte[] segment) {
            this.checkpoint = checkpoint;
            this.delta = delta;
            this.segment = segment;
        }

        @Override
        public OptionalLong stat(String bucket, String prefix, String key) {
            stats.add(key);
            return OptionalLong.of(checkpoint.length);
        }

        @Override
        public java.util.Optional<InputStream> get(String bucket, String prefix, String key)
                throws IOException {
            gets.add(key);
            byte[] bytes = key.endsWith("LATEST") ? checkpoint
                    : key.endsWith(".delta") ? delta
                    : key.contains("-pod-") ? null : segment;
            if (key.contains("/data/") || key.startsWith("data/")) {
                bytes = segment;
            }
            return bytes == null ? java.util.Optional.empty()
                    : java.util.Optional.of(new ByteArrayInputStream(bytes));
        }
    }

    private static byte[] segment(List<RunKey> keys, String prefix) throws IOException {
        SegmentWriter writer = new SegmentWriter();
        for (RunKey key : keys) {
            writer.add(key, new SegmentRecord(prefix + key.partitionId(), OpType.INDEX,
                    OptionalLong.of(key.partitionId()), (prefix + key.partitionId())
                            .getBytes(StandardCharsets.UTF_8)), 1L);
        }
        return writer.toByteArray(1L);
    }

    private static byte[] delta(long sequence, String segmentKey, List<RunKey> keys,
            long offset) {
        return new CommitDelta(sequence, segmentKey, keys.stream()
                .map(key -> new RunCommit(key, 1, offset)).toList()).encode();
    }

    private static final class MapReader implements TierThreeRecovery.Reader {
        private final Map<String, byte[]> objects;
        private final List<String> stats = new ArrayList<>();
        private final List<String> gets = new ArrayList<>();
        private int lists;

        MapReader(Map<String, byte[]> objects) {
            this.objects = objects;
        }

        @Override
        public OptionalLong stat(String bucket, String prefix, String key) {
            stats.add(key);
            byte[] body = objects.get(key);
            return body == null ? OptionalLong.empty() : OptionalLong.of(body.length);
        }

        @Override
        public Optional<InputStream> get(String bucket, String prefix, String key) {
            gets.add(key);
            byte[] body = objects.get(key);
            return body == null ? Optional.empty()
                    : Optional.of(new ByteArrayInputStream(body));
        }
    }

    private enum Failure {
        STAT, POINTER_MISSING, POINTER_CORRUPT, DELTA_MISSING, DELTA_CORRUPT,
        SEGMENT_MISSING, SEGMENT_CORRUPT
    }

    private static final class FaultReader implements TierThreeRecovery.Reader {
        private final Map<String, byte[]> objects;
        private final Failure failure;
        private final String pointer;
        private final String delta;
        private final String segment;
        private final List<String> stats = new ArrayList<>();
        private final List<String> gets = new ArrayList<>();
        private int lists;

        FaultReader(Map<String, byte[]> objects, Failure failure, String pointer,
                String delta, String segment) {
            this.objects = objects;
            this.failure = failure;
            this.pointer = pointer;
            this.delta = delta;
            this.segment = segment;
        }

        @Override
        public OptionalLong stat(String bucket, String prefix, String key) throws IOException {
            stats.add(key);
            if (failure == Failure.STAT) {
                throw new IOException("simulated stat failure");
            }
            return OptionalLong.of(objects.get(key).length);
        }

        @Override
        public Optional<InputStream> get(String bucket, String prefix, String key) {
            gets.add(key);
            if ((failure == Failure.POINTER_MISSING && key.equals(pointer))
                    || (failure == Failure.DELTA_MISSING && key.equals(delta))
                    || (failure == Failure.SEGMENT_MISSING && key.equals(segment))) {
                return Optional.empty();
            }
            byte[] bytes = objects.get(key);
            if ((failure == Failure.POINTER_CORRUPT && key.equals(pointer))
                    || (failure == Failure.DELTA_CORRUPT && key.equals(delta))
                    || (failure == Failure.SEGMENT_CORRUPT && key.equals(segment))) {
                bytes = new byte[] {0};
            }
            return Optional.of(new ByteArrayInputStream(bytes));
        }
    }

    private static final class FailingTransport implements SubscriptionTransport {
        @Override
        public AutoCloseable subscribe(RunKey key, Listener listener) {
            return () -> { };
        }

        @Override
        public void register(io.github.huyz0.os.biningester.format.IndexRegistration registration) {
        }

        @Override
        public CatchUpResult requestCatchUp(
                io.github.huyz0.os.biningester.format.CatchUpRequestFrame request,
                java.util.function.Consumer<SubscriptionEvent> events) throws IOException {
            throw new IOException("ingester unavailable");
        }

        @Override
        public boolean ingesterAnswers() {
            return false;
        }
    }
}
