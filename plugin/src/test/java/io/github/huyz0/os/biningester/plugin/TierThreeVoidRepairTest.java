// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.plugin;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.Checkpoint;
import io.github.huyz0.os.biningester.format.CommitDelta;
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
 * A tier-3 repair over a void delivers it as a void event and completes
 * (M13.25g, ADR-0082 §5).
 *
 * <p>⚠️ **A VOID IN THE GAP'S WINDOW LEFT IT UNCOVERED**: the walk advanced a
 * stream only over committed runs, so a gap a takeover voided part of could
 * never be repaired from the store -- held and asked again for ever.
 */
class TierThreeVoidRepairTest {

    private static final RunKey WANTED = new RunKey(new UUID(0, 1), 0);
    private static final String FIRST =
            "prefix/data/2030/01/01/00/0000000000000000001-pod-0000000000000000-h1-all.bseg";
    private static final String SECOND =
            "prefix/data/2030/01/01/00/0000000000000000002-pod-0000000000000000-h1-all.bseg";

    private static byte[] segment(int records) throws Exception {
        SegmentWriter writer = new SegmentWriter();
        for (int i = 0; i < records; i++) {
            writer.add(WANTED, new SegmentRecord("r" + i, OpType.INDEX, OptionalLong.of(0),
                    ("r" + i).getBytes(StandardCharsets.UTF_8)), 1L);
        }
        return writer.toByteArray(1L);
    }

    private static List<SubscriptionEvent> recover(byte[] slot1, Map<String, byte[]> segments,
            long gapEnd, boolean[] complete) throws Exception {
        return recover(slot1, segments, 0, gapEnd, complete);
    }

    private static List<SubscriptionEvent> recover(byte[] slot1, Map<String, byte[]> segments,
            long gapStart, long gapEnd, boolean[] complete) throws Exception {
        byte[] checkpoint = new Checkpoint(0, Map.of(), Map.of()).encode();
        byte[] slot0 = new CommitDelta(0, FIRST, List.of(new RunCommit(WANTED, 2, 0))).encode();
        TierThreeRecovery.Reader reader = new TierThreeRecovery.Reader() {
            @Override
            public OptionalLong stat(String bucket, String prefix, String key) {
                return OptionalLong.of(checkpoint.length);
            }

            @Override
            public Optional<InputStream> get(String bucket, String prefix, String key) {
                byte[] bytes = key.endsWith("LATEST") ? checkpoint
                        : key.endsWith("0000000000000000.delta") ? slot0
                        : key.endsWith("0000000000000001.delta") ? slot1
                        : segments.get(key);
                return Optional.ofNullable(bytes).map(ByteArrayInputStream::new);
            }
        };
        List<SubscriptionEvent> delivered = new ArrayList<>();
        complete[0] = new TierThreeRecovery("bucket", "prefix", reader)
                .recover(4, 1, Map.of(WANTED, new TierThreeRecovery.Gap(gapStart, gapEnd)),
                        delivered::add);
        return delivered;
    }

    @Test
    void aGAPCoveredByARunAndThenAVoidIsRepaired() throws Exception {
        byte[] voids = new Recovery(1, List.of(),
                List.of(new Recovery.VoidRange(WANTED, 2, 6))).encode();
        boolean[] complete = {false};

        List<SubscriptionEvent> delivered = recover(voids, Map.of(FIRST, segment(2)), 6, complete);

        assertThat(complete[0]).as("⚠️ THE VOID COVERS THE REST OF THE GAP").isTrue();
        assertThat(delivered).hasSize(2);
        assertThat(delivered.get(1).voided()).isTrue();
        assertThat(delivered.get(1).firstOffset()).isEqualTo(2);
        assertThat(delivered.get(1).recordCount()).isEqualTo(4);
        assertThat(delivered.get(1).chainSequence()).as("the recovery's sequence").isEqualTo(1);
        assertThat(delivered.get(1).sequencerEpoch()).as("the gap's chain epoch").isEqualTo(4);
    }

    @Test
    void aVOIDPastTheHeldOffsetIsNoPartOfTheRepair() throws Exception {
        // ⚠️ M13.25g review T1: a recovery voids a later hole too; let into the
        // walk, it starts past the coverage and abandons the whole repair.
        byte[] voids = new Recovery(1, List.of(), List.of(
                new Recovery.VoidRange(WANTED, 2, 6), new Recovery.VoidRange(WANTED, 8, 10)))
                .encode();
        boolean[] complete = {false};

        List<SubscriptionEvent> delivered = recover(voids, Map.of(FIRST, segment(2)), 6, complete);

        assertThat(complete[0]).isTrue();
        assertThat(delivered).hasSize(2);
    }

    @Test
    void aVOIDStartingBeforeTheGapIsCutToIt() throws Exception {
        // ⚠️ M13.25g review T2: emitted from its own start, it overlaps what the
        // consumer already holds, and the coordinator refuses the overlap.
        byte[] voids = new Recovery(1, List.of(),
                List.of(new Recovery.VoidRange(WANTED, 2, 6))).encode();
        boolean[] complete = {false};

        List<SubscriptionEvent> delivered =
                recover(voids, Map.of(FIRST, segment(2)), 3, 6, complete);

        assertThat(complete[0]).isTrue();
        assertThat(delivered).hasSize(1);
        assertThat(delivered.get(0).firstOffset()).isEqualTo(3);
        assertThat(delivered.get(0).recordCount()).isEqualTo(3);
    }

    @Test
    void aVOIDWhollyCoveredAlreadyIsSkipped() throws Exception {
        // ⚠️ M13.25g review T4: nothing of it is left to cover.
        byte[] voids = new Recovery(1, List.of(), List.of(
                new Recovery.VoidRange(WANTED, 1, 2), new Recovery.VoidRange(WANTED, 2, 6)))
                .encode();
        boolean[] complete = {false};

        List<SubscriptionEvent> delivered = recover(voids, Map.of(FIRST, segment(2)), 6, complete);

        assertThat(complete[0]).isTrue();
        assertThat(delivered).extracting(SubscriptionEvent::firstOffset).containsExactly(0L, 2L);
    }

    @Test
    void aVOIDLongerThanTheGapIsClippedToIt() throws Exception {
        byte[] voids = new Recovery(1, List.of(),
                List.of(new Recovery.VoidRange(WANTED, 2, 50))).encode();
        boolean[] complete = {false};

        List<SubscriptionEvent> delivered = recover(voids, Map.of(FIRST, segment(2)), 6, complete);

        assertThat(complete[0]).isTrue();
        assertThat(delivered.get(1).recordCount()).as("only up to the held offset").isEqualTo(4);
    }

    @Test
    void aVOIDPastTheCoverageDoesNotStandInForMissingRecords() throws Exception {
        // ⚠️ Offsets 2-3 are committed nowhere this walk reads: a void from 4
        // must not be clipped back to cover them.
        byte[] voids = new Recovery(1, List.of(),
                List.of(new Recovery.VoidRange(WANTED, 4, 6))).encode();
        boolean[] complete = {true};

        recover(voids, Map.of(FIRST, segment(2)), 6, complete);

        assertThat(complete[0]).isFalse();
    }

    @Test
    void aVOIDBeforeARunInOneRecoveryIsWalkedInOffsetOrder() throws Exception {
        byte[] slot1 = new Recovery(1,
                List.of(new SegmentCommit(SECOND, List.of(new RunCommit(WANTED, 2, 4)))),
                List.of(new Recovery.VoidRange(WANTED, 2, 4))).encode();
        boolean[] complete = {false};

        List<SubscriptionEvent> delivered = recover(slot1,
                Map.of(FIRST, segment(2), SECOND, segment(2)), 6, complete);

        assertThat(complete[0]).isTrue();
        assertThat(delivered).extracting(SubscriptionEvent::firstOffset).containsExactly(0L, 2L, 4L);
        assertThat(delivered).extracting(SubscriptionEvent::voided)
                .containsExactly(false, true, false);
    }
}
