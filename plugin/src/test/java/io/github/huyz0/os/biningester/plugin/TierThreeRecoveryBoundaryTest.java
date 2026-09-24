// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.Checkpoint;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TierThreeRecoveryBoundaryTest {

    private static final RunKey RUN = new RunKey(new UUID(0, 1), 0);
    private static final String POINTER = "prefix/ctl/log/0/0000000000000004/ckpt/LATEST";
    private static final String DELTA = "prefix/ctl/log/0/0000000000000004/0000000000000000.delta";
    private static final String SEGMENT = "prefix/data/one.bseg";
    private static final byte[] CHECKPOINT = new Checkpoint(0, Map.of(), Map.of()).encode();

    @Test
    void invalidEpisodeAndOffsetRangesAreRejectedBeforeStoreIo() {
        List<Map<RunKey, TierThreeRecovery.Gap>> invalid = new java.util.ArrayList<>();
        invalid.add(Map.of());
        invalid.add(Map.of(RUN, new TierThreeRecovery.Gap(-1, 1)));
        invalid.add(Map.of(RUN, new TierThreeRecovery.Gap(1, 1)));
        Map<RunKey, TierThreeRecovery.Gap> nullGap = new HashMap<>();
        nullGap.put(RUN, null);
        invalid.add(nullGap);

        for (Map<RunKey, TierThreeRecovery.Gap> gaps : invalid) {
            Reader reader = new Reader(CHECKPOINT.length, CHECKPOINT, unrelatedDelta(), null);
            assertThat(recovery(reader).recover(4, 0, gaps, event -> { })).isFalse();
            assertThat(reader.stats).isEmpty();
            assertThat(reader.gets).isEmpty();
        }
        for (long epoch : new long[] {-1, 4}) {
            Reader reader = new Reader(CHECKPOINT.length, CHECKPOINT, unrelatedDelta(), null);
            assertThat(recovery(reader).recover(epoch, epoch < 0 ? 0 : -1,
                    Map.of(RUN, new TierThreeRecovery.Gap(0, 1)), event -> { })).isFalse();
            assertThat(reader.stats).isEmpty();
            assertThat(reader.gets).isEmpty();
        }
    }

    @Test
    void checkpointPointerSizeAcceptsInclusiveBoundsAndRejectsOutsideThem() {
        for (long size : new long[] {0, (64L << 20) + 1}) {
            Reader reader = new Reader(size, CHECKPOINT, unrelatedDelta(), null);
            boolean recovered = recovery(reader).recover(4, 0,
                    Map.of(RUN, new TierThreeRecovery.Gap(0, 1)), event -> { });
            assertThat(recovered).isFalse();
            assertThat(reader.gets).as("pointer size %s", size).isEmpty();
        }
        for (long size : new long[] {CHECKPOINT.length, 64L << 20}) {
            Reader reader = new Reader(size, CHECKPOINT, unrelatedDelta(), null);
            boolean recovered = recovery(reader).recover(4, 0,
                    Map.of(RUN, new TierThreeRecovery.Gap(0, 1)), event -> { });
            assertThat(recovered).isFalse();
            assertThat(reader.gets).as("pointer size %s", size).containsExactly(POINTER, DELTA);
        }
    }

    @Test
    void runTouchingTheExclusiveGapEndIsNotFetchedAsGapData() {
        CommitDelta delta = new CommitDelta(0, SEGMENT,
                List.of(new RunCommit(RUN, 1, 2)));
        Reader reader = new Reader(CHECKPOINT.length, CHECKPOINT, delta.encode(), null);

        boolean recovered = recovery(reader).recover(4, 0,
                Map.of(RUN, new TierThreeRecovery.Gap(0, 2)), event -> { });

        assertThat(recovered).isFalse();
        assertThat(reader.gets).containsExactly(POINTER, DELTA);
    }

    @Test
    void everyReadBodyIsClosedAfterACompleteRecovery() throws Exception {
        byte[] segment = new byte[] {1};
        CommitDelta delta = new CommitDelta(0, SEGMENT,
                List.of(new RunCommit(RUN, 1, 0)));
        Reader reader = new Reader(CHECKPOINT.length, CHECKPOINT, delta.encode(), segment);

        recovery(reader).recover(4, 0, Map.of(RUN, new TierThreeRecovery.Gap(0, 1)), event -> { });

        assertThat(reader.bodies).hasSize(3).allSatisfy(body -> assertThat(body.closed).isTrue());
    }

    private static TierThreeRecovery recovery(Reader reader) {
        return new TierThreeRecovery("bucket", "prefix", reader);
    }

    private static byte[] unrelatedDelta() {
        RunKey unrelated = new RunKey(new UUID(0, 2), 0);
        return new CommitDelta(0, SEGMENT, List.of(new RunCommit(unrelated, 1, 0))).encode();
    }

    private static final class Reader implements TierThreeRecovery.Reader {
        private final long size;
        private final byte[] pointer;
        private final byte[] delta;
        private final byte[] segment;
        private final List<String> gets = new java.util.ArrayList<>();
        private final List<String> stats = new java.util.ArrayList<>();
        private final List<CloseTrackingInputStream> bodies = new java.util.ArrayList<>();

        Reader(long size, byte[] pointer, byte[] delta, byte[] segment) {
            this.size = size;
            this.pointer = pointer;
            this.delta = delta;
            this.segment = segment;
        }

        @Override
        public OptionalLong stat(String bucket, String prefix, String key) {
            stats.add(key);
            return OptionalLong.of(size);
        }

        @Override
        public Optional<InputStream> get(String bucket, String prefix, String key) {
            gets.add(key);
            byte[] value = key.equals(POINTER) ? pointer : key.equals(DELTA) ? delta : segment;
            if (value == null) {
                return Optional.empty();
            }
            CloseTrackingInputStream body = new CloseTrackingInputStream(value);
            bodies.add(body);
            return Optional.of(body);
        }
    }

    private static final class CloseTrackingInputStream extends ByteArrayInputStream {
        private boolean closed;

        CloseTrackingInputStream(byte[] bytes) {
            super(bytes);
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }
}
