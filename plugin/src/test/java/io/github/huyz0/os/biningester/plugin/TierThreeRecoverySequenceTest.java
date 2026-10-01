// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.Checkpoint;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Recovery;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import io.github.huyz0.os.biningester.format.SegmentWriter;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A recovery at the wrong slot is not that slot's commits (M13.25a, review
 * round 3 T4): tier 3 checks a recovery's sequence as it checks a delta's.
 */
class TierThreeRecoverySequenceTest {

    private static final String SEGMENT_KEY =
            "prefix/data/2030/01/01/00/0000000000000000001-pod-0000000000000000-h1-all.bseg";

    @Test
    void aRECOVERYNamingAnotherSequenceIsNotRepairedFrom() throws Exception {
        RunKey key = new RunKey(new UUID(0, 1), 0);
        SegmentWriter writer = new SegmentWriter();
        writer.add(key, new SegmentRecord("r0", OpType.INDEX, OptionalLong.of(0),
                "r0".getBytes(StandardCharsets.UTF_8)), 1L);
        byte[] segment = writer.toByteArray(1L);
        byte[] checkpoint = new Checkpoint(0, Map.of(), Map.of()).encode();
        byte[] misplaced = new Recovery(7,
                List.of(new SegmentCommit(SEGMENT_KEY, List.of(new RunCommit(key, 1, 0)))),
                List.of()).encode();
        TierThreeRecovery.Reader reader = new TierThreeRecovery.Reader() {
            @Override
            public OptionalLong stat(String bucket, String prefix, String objectKey) {
                return OptionalLong.of(checkpoint.length);
            }

            @Override
            public Optional<InputStream> get(String bucket, String prefix, String objectKey) {
                byte[] bytes = objectKey.endsWith("LATEST") ? checkpoint
                        : objectKey.endsWith(".delta") ? misplaced
                        : segment;
                return Optional.of(new ByteArrayInputStream(bytes));
            }
        };
        List<SubscriptionEvent> delivered = new ArrayList<>();

        boolean complete = new TierThreeRecovery("bucket", "prefix", reader)
                .recover(4, 0, Map.of(key, new TierThreeRecovery.Gap(0, 1)), delivered::add);

        assertThat(complete).as("slot 0 holds sequence 7's entry: not slot 0's commits")
                .isFalse();
        assertThat(delivered).isEmpty();
    }
}
