// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.format.RunKey;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * ⚠️ THE REQUEST IS SHAPED FOR A COMMIT THAT ARRIVES FROM ANOTHER NODE.
 * `podId` and `flushSeq` are not decoration: M4 ships only the local
 * {@link Sequencer}, but M5 adds commit forwarding, and a forwarded commit is
 * meaningless without the identity of the node that produced it and that node's
 * own flush sequence. Carrying them from the first commit is what keeps the
 * record shape from changing when the remote implementation lands (M4 SPEC
 * § Deployment constraint), and it is what M4.10's idempotency will key on.
 */
class CommitRequestTest {

    private static final UUID LOGS = UUID.fromString("00000000-0000-0000-0000-0000000000aa");

    private static Map<RunKey, Integer> counts() {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        m.put(new RunKey(LOGS, 0), 3);
        return m;
    }

    @Test
    void aBlankPodIdIsRefused() {
        // ⚠️ Not cosmetic: podId is the idempotency key's first half (M4.10),
        // so a blank one would collapse every node's commits into one identity
        // and make a replay from node A look like a replay from node B.
        assertThatThrownBy(() -> new CommitRequest("  ", 0L, "k", counts()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("podId");
        assertThatThrownBy(() -> new CommitRequest(null, 0L, "k", counts()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void aPodIdContainingADashOrSlashIsRefused() {
        // ⚠️ INHERITED, not intrinsic: no sequencer key contains podId (M4.0
        // settled the chain path as ctl/log/<slot>/...). The restriction exists
        // so this is the same identity as SegmentPublisher's podShortId, whose
        // own key grammar IS parsed on those separators. Consequence worth
        // knowing: a Kubernetes POD_NAME is hyphenated in every StatefulSet, so
        // a caller must pass the short id, not the pod name.
        assertThatThrownBy(() -> new CommitRequest("pod-1", 0L, "k", counts()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CommitRequest("pod/1", 0L, "k", counts()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aNegativeFlushSeqIsRefused() {
        assertThatThrownBy(() -> new CommitRequest("pod1", -1L, "k", counts()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("flushSeq");
    }

    @Test
    void aBlankSegmentKeyIsRefused() {
        assertThatThrownBy(() -> new CommitRequest("pod1", 0L, " ", counts()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("segmentKey");
    }

    @Test
    void anEmptyOrNonPositiveRecordCountIsRefused() {
        // ⚠️ CommitDelta already refuses an empty run list, with a stated
        // reason -- "an empty delta would consume a sequence number and commit
        // nothing, so a replay would see a gap it cannot explain". Refusing it
        // HERE means the caller learns at the seam rather than several layers
        // down inside the delta's own constructor.
        assertThatThrownBy(() -> new CommitRequest("pod1", 0L, "k", Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("recordCounts");
        assertThatThrownBy(() -> new CommitRequest("pod1", 0L, "k",
                Map.of(new RunKey(LOGS, 0), 0)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void theRecordCountsAreCopiedSoALaterMutationCannotChangeACommitAlreadyMade() {
        // ⚠️ The request crosses a seam and, from M5, a network. A caller that
        // reused its map would otherwise be able to alter what was committed
        // after the fact.
        Map<RunKey, Integer> mutable = counts();
        CommitRequest r = new CommitRequest("pod1", 0L, "k", mutable);
        mutable.put(new RunKey(LOGS, 1), 99);
        assertThat(r.recordCounts()).hasSize(1);
    }

    @Test
    void podIdAndFlushSeqSurviveConstruction() {
        // ⚠️ Round-1 test review: an earlier draft deleted a field-preservation
        // test as "true by construction for a record". That is FALSE for a
        // record with a compact constructor -- this one already reassigns
        // `recordCounts` -- and podId/flushSeq are the two fields the whole
        // seam design rests on. Mutating the compact constructor to pin either
        // to a constant went undetected by every other test here.
        CommitRequest r = new CommitRequest("podz", 42L, "bins/cluster-a/seg", counts());
        assertThat(r.podId()).isEqualTo("podz");
        assertThat(r.flushSeq()).isEqualTo(42L);
        assertThat(r.segmentKey()).isEqualTo("bins/cluster-a/seg");
    }

    @Test
    void theCopiedRecordCountsAreImmutableNotMerelyUnaliased() {
        // ⚠️ Aliasing was pinned; immutability was not -- `new HashMap<>(...)`
        // passed the aliasing test while leaving a holder able to mutate what
        // it was given, which matters for a record that crosses a network.
        CommitRequest r = new CommitRequest("pod1", 0L, "k", counts());
        assertThatThrownBy(() -> r.recordCounts().put(new RunKey(LOGS, 9), 1))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
