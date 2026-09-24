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
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TierThreeRecoveryBackpressureTest {

    @Test
    void recoveryReportsRefusedEventWithoutCompletingEpisode() throws Exception {
        RunKey key = new RunKey(new UUID(0, 1), 0);
        String segment = "prefix/data/segment.bseg";
        String control = "prefix/ctl/log/0/0000000000000004/";
        SegmentWriter writer = new SegmentWriter();
        writer.add(key, new SegmentRecord("first", OpType.INDEX, OptionalLong.of(0),
                new byte[] {1}), 1L);
        byte[] body = writer.toByteArray(1L);
        Map<String, byte[]> objects = Map.of(
                control + "ckpt/LATEST", new Checkpoint(0, Map.of(), Map.of()).encode(),
                control + "0000000000000000.delta",
                    new CommitDelta(0, segment, List.of(new RunCommit(key, 1, 0))).encode(),
                segment, body);
        TierThreeRecovery.Reader reader = new TierThreeRecovery.Reader() {
            @Override
            public OptionalLong stat(String bucket, String prefix, String key) {
                byte[] bytes = objects.get(key);
                return bytes == null ? OptionalLong.empty() : OptionalLong.of(bytes.length);
            }

            @Override
            public Optional<InputStream> get(String bucket, String prefix, String key) {
                byte[] bytes = objects.get(key);
                return bytes == null ? Optional.empty()
                        : Optional.of(new ByteArrayInputStream(bytes));
            }
        };

        boolean recovered = new TierThreeRecovery("bucket", "prefix", reader).recoverWithSink(
                4, 0, Map.of(key, new TierThreeRecovery.Gap(0, 1)), event -> false);

        assertThat(recovered).isFalse();
    }
}
