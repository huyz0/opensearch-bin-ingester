// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.Checkpoint;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class TierThreeRecoveryCheckpointTest {

    @Test
    void replayStartsAtResolvedNonzeroCheckpointSequence() throws Exception {
        RunKey key = new RunKey(new UUID(0, 1), 0);
        RunKey unrelated = new RunKey(new UUID(0, 2), 0);
        String data = "prefix/data/recovered.bseg";
        String ctl = "prefix/ctl/log/0/0000000000000004/";
        SegmentWriter writer = new SegmentWriter();
        writer.add(key, new SegmentRecord("recovered", OpType.INDEX,
                OptionalLong.of(0), new byte[] {1}), 1L);
        byte[] segment = writer.toByteArray(1L);
        Map<String, byte[]> objects = Map.of(
                ctl + "ckpt/LATEST", new Checkpoint(2, Map.of(), Map.of()).encode(),
                ctl + "0000000000000002.delta",
                    new CommitDelta(2, List.of(new SegmentCommit(data,
                            List.of(new RunCommit(unrelated, 1, 0))))).encode(),
                ctl + "0000000000000003.delta",
                    new CommitDelta(3, List.of(new SegmentCommit(data,
                            List.of(new RunCommit(key, 1, 0))))).encode(),
                data, segment);
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
                byte[] bytes = objects.get(objectKey);
                return bytes == null ? Optional.empty()
                        : Optional.of(new ByteArrayInputStream(bytes));
            }
        };

        List<Long> offsets = new ArrayList<>();
        boolean recovered = new TierThreeRecovery("bucket", "prefix", reader).recover(
                4, 3, Map.of(key, new TierThreeRecovery.Gap(0, 1)),
                event -> offsets.add(event.firstOffset()));

        assertThat(recovered).isTrue();
        assertThat(offsets).containsExactly(0L);
        assertThat(gets).containsExactly(ctl + "ckpt/LATEST",
                ctl + "0000000000000002.delta", ctl + "0000000000000003.delta", data);
    }
}
