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
 * The plugin's tier-3 re-read takes a recovery entry's commits as a delta's
 * (M13.25, non-negotiable 8; ADR-0082 §5).
 *
 * <p>⚠️ IT READ BY {@code instanceof CommitDelta}: a recovered run sitting in
 * a recovery entry was not found, and a consumer's gap over it stayed
 * unrepaired with the records readable in the store.
 */
class TierThreeRecoveryEntryTest {

    private static final String SEGMENT_KEY =
            "prefix/data/2030/01/01/00/0000000000000000001-pod-0000000000000000-h1-all.bseg";

    @Test
    void aGAPOverARecoveredRunIsRepairedFromTheRecoveryEntry() throws Exception {
        RunKey key = new RunKey(new UUID(0, 1), 0);
        SegmentWriter writer = new SegmentWriter();
        writer.add(key, new SegmentRecord("r0", OpType.INDEX, OptionalLong.of(0),
                "r0".getBytes(StandardCharsets.UTF_8)), 1L);
        byte[] segment = writer.toByteArray(1L);
        byte[] checkpoint = new Checkpoint(0, Map.of(), Map.of()).encode();
        byte[] recovery = new Recovery(0,
                List.of(new SegmentCommit(SEGMENT_KEY, List.of(new RunCommit(key, 1, 0)))),
                List.of(new Recovery.VoidRange(key, 1, 50))).encode();
        TierThreeRecovery.Reader reader = new TierThreeRecovery.Reader() {
            @Override
            public OptionalLong stat(String bucket, String prefix, String objectKey) {
                return OptionalLong.of(checkpoint.length);
            }

            @Override
            public Optional<InputStream> get(String bucket, String prefix, String objectKey) {
                byte[] bytes = objectKey.endsWith("LATEST") ? checkpoint
                        : objectKey.endsWith(".delta") ? recovery
                        : segment;
                return Optional.of(new ByteArrayInputStream(bytes));
            }
        };
        List<SubscriptionEvent> delivered = new ArrayList<>();

        boolean complete = new TierThreeRecovery("bucket", "prefix", reader)
                .recover(4, 0, Map.of(key, new TierThreeRecovery.Gap(0, 1)), delivered::add);

        assertThat(complete).isTrue();
        assertThat(delivered).hasSize(1);
        assertThat(delivered.get(0).firstOffset()).isZero();
    }
}
