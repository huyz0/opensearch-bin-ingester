// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.Checkpoint;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TierThreeAvailabilityTransitionTest {

    @Test
    void anIngesterReturningAfterPointerReadStopsFurtherFallbackGets() throws Exception {
        RunKey key = new RunKey(new UUID(0, 1), 0);
        String pointer = "prefix/ctl/log/0/0000000000000004/ckpt/LATEST";
        String delta = "prefix/ctl/log/0/0000000000000004/0000000000000000.delta";
        String segmentKey = "prefix/data/segment.bseg";
        byte[] checkpoint = new Checkpoint(0, Map.of(), Map.of()).encode();
        SegmentWriter writer = new SegmentWriter();
        writer.add(key, new SegmentRecord("row", OpType.INDEX, OptionalLong.of(0),
                new byte[] {1}), 1L);
        Map<String, byte[]> objects = Map.of(pointer, checkpoint,
                delta, new CommitDelta(0, segmentKey, List.of(new RunCommit(key, 1, 0))).encode(),
                segmentKey, writer.toByteArray(1L));
        MutableTransport transport = new MutableTransport();
        List<String> gets = new ArrayList<>();
        TierThreeRecovery.Reader reader = new TierThreeRecovery.Reader() {
            @Override
            public OptionalLong stat(String bucket, String prefix, String objectKey) {
                byte[] bytes = objects.get(objectKey);
                return bytes == null ? OptionalLong.empty() : OptionalLong.of(bytes.length);
            }

            @Override
            public Optional<InputStream> get(String bucket, String prefix, String objectKey) {
                gets.add(objectKey);
                if (objectKey.equals(pointer)) {
                    transport.answers = true;
                }
                byte[] bytes = objects.get(objectKey);
                return bytes == null ? Optional.empty()
                        : Optional.of(new ByteArrayInputStream(bytes));
            }
        };

        boolean recovered;
        try (NodeSubscriptions subscriptions = new NodeSubscriptions(transport, 1)) {
            subscriptions.enableTierThree(new TierThreeRecovery("bucket", "prefix", reader));
            recovered = subscriptions.recoverTierThree(4, 0,
                    Map.of(key, new TierThreeRecovery.Gap(0, 1)), event -> true);
        }

        assertThat(recovered).isFalse();
        assertThat(gets).containsExactly(pointer);
    }

    private static final class MutableTransport
            implements io.github.huyz0.os.biningester.client.SubscriptionTransport {
        private boolean answers;

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
                java.util.function.Consumer<io.github.huyz0.os.biningester.format.SubscriptionEvent> events) {
            return CatchUpResult.COMPLETE;
        }

        @Override
        public boolean ingesterAnswers() {
            return answers;
        }
    }
}
