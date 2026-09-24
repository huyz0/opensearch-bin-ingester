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
import io.github.huyz0.os.biningester.format.SegmentWriter;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TierThreeRecoveryCoordinatorTest {

    @Test
    void completedNodeRecoveryIsNotRepeatedOnTheNextProgressTick() throws Exception {
        RunKey key = new RunKey(new UUID(0, 1), 0);
        String first = "prefix/data/first.bseg";
        String missing = "prefix/data/missing.bseg";
        String control = "prefix/ctl/log/0/0000000000000004/";
        byte[] firstBytes = segment(key, "first");
        byte[] missingBytes = segment(key, "missing");
        Map<String, byte[]> objects = Map.of(
                control + "ckpt/LATEST", new Checkpoint(0, Map.of(), Map.of()).encode(),
                control + "0000000000000000.delta",
                    new CommitDelta(0, first, List.of(new RunCommit(key, 1, 0))).encode(),
                control + "0000000000000001.delta",
                    new CommitDelta(1, missing, List.of(new RunCommit(key, 1, 1))).encode(),
                control + "0000000000000002.delta",
                    new CommitDelta(2, first,
                            List.of(new RunCommit(new RunKey(new UUID(0, 2), 0), 1, 0)))
                            .encode(),
                missing, missingBytes);
        RecordingReader reader = new RecordingReader(objects);
        FailingTransport transport = new FailingTransport();
        try (NodeSubscriptions clients = new NodeSubscriptions(transport, 8)) {
            ConsumerClient consumer = clients.clientFor(key);
            clients.enableTierThree(new TierThreeRecovery("bucket", "prefix", reader));
            NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(transport, clients,
                    Optional::<List<io.github.huyz0.os.biningester.format.CatchUpRequestFrame.Stream>>empty);
            consumer.deliver(new Delivery(key, first, 1, 0, FetchMode.INLINE, firstBytes,
                    null, 4, 0));
            assertThat(consumer.readNext(Duration.ZERO).orElseThrow().offset()).isZero();
            consumer.deliver(new Delivery(key, "prefix/data/live.bseg", 1, 2,
                    FetchMode.INLINE, missingBytes, null, 4, 2));
            assertThat(consumer.readNext(Duration.ZERO)).isEmpty();

            coordinator.attempt();

            assertThat(consumer.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(1);
            assertThat(consumer.readNext(Duration.ZERO).orElseThrow().offset()).isEqualTo(2);
            List<String> firstGets = List.copyOf(reader.gets);
            List<String> firstStats = List.copyOf(reader.stats);
            assertThat(transport.requests).isEqualTo(1);

            coordinator.attempt();

            assertThat(reader.gets).isEqualTo(firstGets);
            assertThat(reader.stats).isEqualTo(firstStats);
            assertThat(transport.requests).isEqualTo(1);
        }
    }

    @Test
    void unavailableSegmentKeepsTheGapVisibleAcrossProgressRetries() throws Exception {
        RunKey key = new RunKey(new UUID(0, 1), 0);
        RunKey unrelated = new RunKey(new UUID(0, 2), 0);
        String first = "prefix/data/first.bseg";
        String missing = "prefix/data/missing.bseg";
        String control = "prefix/ctl/log/0/0000000000000004/";
        byte[] firstBytes = segment(key, "first");
        byte[] checkpoint = new Checkpoint(0, Map.of(), Map.of()).encode();
        Map<String, byte[]> objects = Map.of(
                control + "ckpt/LATEST", checkpoint,
                control + "0000000000000000.delta",
                    new CommitDelta(0, first, List.of(new RunCommit(unrelated, 1, 0))).encode(),
                control + "0000000000000001.delta",
                    new CommitDelta(1, missing, List.of(new RunCommit(key, 1, 1))).encode(),
                control + "0000000000000002.delta",
                    new CommitDelta(2, first, List.of(new RunCommit(unrelated, 1, 0))).encode());
        RecordingReader reader = new RecordingReader(objects);
        FailingTransport transport = new FailingTransport();
        try (NodeSubscriptions clients = new NodeSubscriptions(transport, 8)) {
            ConsumerClient consumer = clients.clientFor(key);
            clients.enableTierThree(new TierThreeRecovery("bucket", "prefix", reader));
            NodeCatchUpCoordinator coordinator = new NodeCatchUpCoordinator(transport, clients,
                    Optional::<List<io.github.huyz0.os.biningester.format.CatchUpRequestFrame.Stream>>empty);
            consumer.deliver(new Delivery(key, first, 1, 0, FetchMode.INLINE, firstBytes,
                    null, 4, 0));
            assertThat(consumer.readNext(Duration.ZERO).orElseThrow().offset()).isZero();
            consumer.deliver(new Delivery(key, "prefix/data/live.bseg", 1, 2,
                    FetchMode.INLINE, firstBytes, null, 4, 2));
            assertThat(consumer.readNext(Duration.ZERO)).isEmpty();

            coordinator.attempt();
            coordinator.attempt();

            assertThat(consumer.lastGap()).isPresent();
            assertThat(consumer.readNext(Duration.ZERO)).isEmpty();
            assertThat(transport.requests).isEqualTo(2);
            assertThat(reader.stats).containsExactly(
                    "prefix/ctl/log/0/0000000000000004/ckpt/LATEST",
                    "prefix/ctl/log/0/0000000000000004/ckpt/LATEST");
            assertThat(reader.gets).containsExactly(
                    "prefix/ctl/log/0/0000000000000004/ckpt/LATEST",
                    "prefix/ctl/log/0/0000000000000004/0000000000000000.delta",
                    "prefix/ctl/log/0/0000000000000004/0000000000000001.delta", missing,
                    "prefix/ctl/log/0/0000000000000004/ckpt/LATEST",
                    "prefix/ctl/log/0/0000000000000004/0000000000000000.delta",
                    "prefix/ctl/log/0/0000000000000004/0000000000000001.delta", missing);
        }
    }

    private static byte[] segment(RunKey key, String id) throws IOException {
        SegmentWriter writer = new SegmentWriter();
        writer.add(key, new SegmentRecord(id, OpType.INDEX, OptionalLong.of(0),
                id.getBytes(StandardCharsets.UTF_8)), 1L);
        return writer.toByteArray(1L);
    }

    private static final class RecordingReader implements TierThreeRecovery.Reader {
        private final Map<String, byte[]> objects;
        private final List<String> stats = new java.util.ArrayList<>();
        private final List<String> gets = new java.util.ArrayList<>();

        RecordingReader(Map<String, byte[]> objects) {
            this.objects = objects;
        }

        @Override
        public OptionalLong stat(String bucket, String prefix, String key) {
            stats.add(key);
            byte[] bytes = objects.get(key);
            return bytes == null ? OptionalLong.empty() : OptionalLong.of(bytes.length);
        }

        @Override
        public Optional<InputStream> get(String bucket, String prefix, String key) {
            gets.add(key);
            byte[] bytes = objects.get(key);
            return bytes == null ? Optional.empty()
                    : Optional.of(new ByteArrayInputStream(bytes));
        }
    }

    private static final class FailingTransport implements SubscriptionTransport {
        private int requests;

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
            requests++;
            throw new IOException("ingester unavailable");
        }

        @Override
        public boolean ingesterAnswers() {
            return false;
        }
    }
}
