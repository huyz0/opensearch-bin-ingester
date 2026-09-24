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
import java.io.InputStream;
import java.util.Map;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class TierThreeRecoveryMemoryBudgetTest {

    @Test
    void checkpointWhoseReportedSizeIsExactlySixtyFourMiBIsFetched() throws Exception {
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
        List<String> gets = new ArrayList<>();
        AtomicLong reportedSize = new AtomicLong(TierThreeRecovery.MAX_EPISODE_BYTES);
        TierThreeRecovery.Reader reader = new TierThreeRecovery.Reader() {
            @Override
            public OptionalLong stat(String bucket, String prefix, String key) {
                return OptionalLong.of(reportedSize.get());
            }

            @Override
            public Optional<InputStream> get(String bucket, String prefix, String key) {
                gets.add(key);
                return Optional.of(new java.io.ByteArrayInputStream(objects.get(key)));
            }
        };

        TierThreeRecovery recovery = new TierThreeRecovery("bucket", "prefix", reader);
        for (long size : List.of(1L, TierThreeRecovery.MAX_EPISODE_BYTES)) {
            reportedSize.set(size);
            gets.clear();
            boolean recovered = recovery.recover(4, 0,
                    Map.of(key, new TierThreeRecovery.Gap(0, 1)), event -> { });
            assertThat(recovered).isTrue();
            assertThat(gets).contains(pointer);
        }
    }

    @Test
    void objectReadStopsAtEpisodeBudgetPlusOneByte() {
        CountingInputStream body = new CountingInputStream(
                TierThreeRecovery.MAX_EPISODE_BYTES + 512);
        byte[] checkpoint = new Checkpoint(0, Map.of(), Map.of()).encode();
        TierThreeRecovery.Reader reader = new TierThreeRecovery.Reader() {
            @Override
            public OptionalLong stat(String bucket, String prefix, String key) {
                return OptionalLong.of(checkpoint.length);
            }

            @Override
            public Optional<InputStream> get(String bucket, String prefix, String key) {
                return key.endsWith("LATEST")
                        ? Optional.of(new java.io.ByteArrayInputStream(checkpoint))
                        : Optional.of(body);
            }
        };
        RunKey key = new RunKey(new UUID(0, 1), 0);

        boolean recovered = new TierThreeRecovery("bucket", "prefix", reader).recover(
                4, 0, Map.of(key, new TierThreeRecovery.Gap(0, 1)), event -> { });

        assertThat(recovered).isFalse();
        assertThat(body.bytesRead()).isEqualTo(
                TierThreeRecovery.MAX_EPISODE_BYTES - checkpoint.length + 1);
        assertThat(body.closed).isTrue();
    }

    private static final class CountingInputStream extends InputStream {
        private final long length;
        private long read;
        private boolean closed;

        CountingInputStream(long length) {
            this.length = length;
        }

        long bytesRead() {
            return read;
        }

        @Override
        public int read() {
            return read++ < length ? 0 : -1;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
