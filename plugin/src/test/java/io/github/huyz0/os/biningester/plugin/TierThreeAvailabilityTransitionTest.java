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
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
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

    private static final RunKey KEY = new RunKey(new UUID(0, 1), 0);
    private static final String POINTER = "prefix/ctl/log/0/0000000000000004/ckpt/LATEST";
    private static final String DELTA = "prefix/ctl/log/0/0000000000000004/0000000000000000.delta";
    private static final String SEGMENT = "prefix/data/segment.bseg";

    @Test
    void anIngesterReturningAfterPointerReadStopsFurtherFallbackGets() throws Exception {
        List<String> gets = new ArrayList<>();
        List<SubscriptionEvent> delivered = new ArrayList<>();

        boolean recovered = recoverFlippingReachabilityDuring(POINTER, gets, delivered);

        assertThat(recovered).isFalse();
        assertThat(gets).containsExactly(POINTER);
    }

    // M9.44 review: once the final segment GET has returned every byte the episode needs, no
    // further GET remains to be refused, so only the pre-delivery guard stands between an
    // ingester that returned DURING that GET and a fallback delivery racing the live path.
    @Test
    void anIngesterReturningDuringTheFinalSegmentGetDeliversNothing() throws Exception {
        List<String> gets = new ArrayList<>();
        List<SubscriptionEvent> delivered = new ArrayList<>();

        boolean recovered = recoverFlippingReachabilityDuring(SEGMENT, gets, delivered);

        assertThat(gets).containsExactly(POINTER, DELTA, SEGMENT);
        assertThat(recovered).isFalse();
        assertThat(delivered).isEmpty();
    }

    private static boolean recoverFlippingReachabilityDuring(String flipKey, List<String> gets,
            List<SubscriptionEvent> delivered) throws Exception {
        byte[] checkpoint = new Checkpoint(0, Map.of(), Map.of()).encode();
        SegmentWriter writer = new SegmentWriter();
        writer.add(KEY, new SegmentRecord("row", OpType.INDEX, OptionalLong.of(0),
                new byte[] {1}), 1L);
        Map<String, byte[]> objects = Map.of(POINTER, checkpoint,
                DELTA, new CommitDelta(0, SEGMENT, List.of(new RunCommit(KEY, 1, 0))).encode(),
                SEGMENT, writer.toByteArray(1L));
        MutableTransport transport = new MutableTransport();
        TierThreeRecovery.Reader reader = new TierThreeRecovery.Reader() {
            @Override
            public OptionalLong stat(String bucket, String prefix, String objectKey) {
                byte[] bytes = objects.get(objectKey);
                return bytes == null ? OptionalLong.empty() : OptionalLong.of(bytes.length);
            }

            @Override
            public Optional<InputStream> get(String bucket, String prefix, String objectKey) {
                gets.add(objectKey);
                if (objectKey.equals(flipKey)) {
                    transport.answers = true;
                }
                byte[] bytes = objects.get(objectKey);
                return bytes == null ? Optional.empty()
                        : Optional.of(new ByteArrayInputStream(bytes));
            }
        };

        try (NodeSubscriptions subscriptions = new NodeSubscriptions(transport, 1)) {
            subscriptions.enableTierThree(new TierThreeRecovery("bucket", "prefix", reader));
            return subscriptions.recoverTierThree(4, 0,
                    Map.of(KEY, new TierThreeRecovery.Gap(0, 1)), event -> delivered.add(event));
        }
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
