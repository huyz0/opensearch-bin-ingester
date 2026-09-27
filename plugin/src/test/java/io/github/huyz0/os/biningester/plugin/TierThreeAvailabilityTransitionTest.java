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
        List<String> gets = new ArrayList<>();
        List<Object> delivered = new ArrayList<>();
        boolean recovered = recoverFlippingOn(POINTER, gets, delivered);

        assertThat(recovered).isFalse();
        assertThat(gets).containsExactly(POINTER);
    }

    /**
     * ⚠️ THE PRE-DELIVERY GUARD (M10.12, harvested from 895710a's review):
     * the ingester answers again DURING the final segment GET, after every
     * object was read. Recovery must still hand no event to a consumer -- the
     * live path owns the stream again -- which only the check made just before
     * the first delivery can catch.
     */
    @Test
    void anIngesterReturningDuringTheFinalSegmentReadDeliversNothing() throws Exception {
        List<String> gets = new ArrayList<>();
        List<Object> delivered = new ArrayList<>();
        boolean recovered = recoverFlippingOn(SEGMENT, gets, delivered);

        assertThat(gets).as("the premise: every object was read").containsExactly(
                POINTER, DELTA, SEGMENT);
        assertThat(recovered).isFalse();
        assertThat(delivered).as("no event after the ingester returned").isEmpty();
    }

    private static final String POINTER = "prefix/ctl/log/0/0000000000000004/ckpt/LATEST";
    private static final String DELTA =
            "prefix/ctl/log/0/0000000000000004/0000000000000000.delta";
    private static final String SEGMENT = "prefix/data/segment.bseg";

    /** One Tier 3 recovery whose ingester starts answering during the GET of {@code flipOn}. */
    private static boolean recoverFlippingOn(String flipOn, List<String> gets,
            List<Object> delivered) throws Exception {
        RunKey key = new RunKey(new UUID(0, 1), 0);
        String pointer = POINTER;
        String delta = DELTA;
        String segmentKey = SEGMENT;
        byte[] checkpoint = new Checkpoint(0, Map.of(), Map.of()).encode();
        SegmentWriter writer = new SegmentWriter();
        writer.add(key, new SegmentRecord("row", OpType.INDEX, OptionalLong.of(0),
                new byte[] {1}), 1L);
        Map<String, byte[]> objects = Map.of(pointer, checkpoint,
                delta, new CommitDelta(0, segmentKey, List.of(new RunCommit(key, 1, 0))).encode(),
                segmentKey, writer.toByteArray(1L));
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
                if (objectKey.equals(flipOn)) {
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
                    Map.of(key, new TierThreeRecovery.Gap(0, 1)), event -> delivered.add(event));
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
