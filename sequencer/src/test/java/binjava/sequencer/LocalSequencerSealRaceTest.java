// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.BinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.ChainEntry;
import binjava.format.Continue;
import binjava.format.RunKey;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * ADR-0037's RACE: the probe that chooses the ancestor and the seal that fences
 * it are not atomic, and nothing renews the lease during {@code start}.
 *
 * <p>⚠️ SPLIT FROM {@link LocalSequencerAncestorSealTest} along a real seam,
 * forced by the 500-line cap. That file asks WHICH ancestor the walk chooses
 * given a settled store; this one asks what happens when the store CHANGES
 * between the probe and the seal. Every case here needs
 * {@link OpenOnFirstProbeStore} and none of the cases there does.
 *
 * <p>⚠️ THE RACE IS REAL, NOT HYPOTHETICAL: a pod that stalls in {@code start}
 * past its TTL -- no renewer runs during {@code start} -- can open a chain the
 * walk has just read as empty. Review found the first draft of ADR-0037's loop
 * reassigning offsets in exactly that window.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class LocalSequencerSealRaceTest {

    private static final UUID STREAM = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    private static final String PREFIX = "bins/cluster-a";

    private static Map<RunKey, Integer> counts(int n) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        m.put(new RunKey(STREAM, 0), n);
        return m;
    }

    private static LeaseManager manager(BinStore store, String pod) {
        return LocalSequencerAncestorSealTest.manager(store, pod);
    }

    private static void burnEpochs(BinStore store, int upTo) throws IOException {
        LocalSequencerAncestorSealTest.burnEpochs(store, upTo);
    }

    private static ChainEntry slotZero(BinStore store, long epoch) throws IOException {
        return LocalSequencerAncestorSealTest.slotZero(store, epoch);
    }


    /**
     * ⚠️ A CROSSED EPOCH THAT IS OPENED BETWEEN THE PROBE AND THE SEAL IS THE
     * REAL ANCESTOR, and the seal's own return value is the proof.
     *
     * <p>The probe ({@code firstInheritableAncestor}: a stat plus a get on slot
     * 0) and the seal are not atomic, and nothing renews the lease during
     * {@code start} -- so a pod that stalled past its TTL can win slot 0 of a
     * crossed epoch in between and commit. {@code CommitLog.seal} redrives past
     * whatever landed, so a {@code Seal} returned at a sequence ABOVE 0 says
     * exactly one thing: that chain was genuinely opened after the probe read
     * it empty.
     *
     * <p>⚠️ DISCARDING THAT RETURN VALUE REASSIGNS OFFSETS. The first draft of
     * ADR-0037's loop did, keeping the `prevEpoch` chosen from the stale probe;
     * review measured it committing at offset 3 where the pre-image committed
     * at 5, reassigning 3 and 4. That is the mirror hazard the ADR rejects as
     * alternative (c), arriving through a race rather than through a missing
     * seal.
     */
    @Test
    void aCrossedEpochOPENEDBetweenProbeAndSealBecomesTheAncestor() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();

        CommitLog first = new CommitLog(backing, PREFIX, 1);
        first.open(0, 0);
        first.commit("seg/a", counts(3));
        first.seal(3, 8);

        burnEpochs(backing, 3);

        // ⚠️ THE INTERLEAVING, forced deterministically: the moment the walk
        // probes slot 0 of epoch 3 and finds it empty, epoch 3's stalled leader
        // opens and commits. Everything after that probe sees an OPENED epoch 3.
        String probed = new CommitLog(backing, PREFIX, 3).keyFor(0);
        BinStore racing = new OpenOnFirstProbeStore(backing, probed, () -> {
            CommitLog stalled = new CommitLog(backing, PREFIX, 3);
            stalled.open(1, 2);
            stalled.commit("seg/stalled", counts(2));
        });

        LocalSequencer successor = LocalSequencer.start(racing, PREFIX,
                manager(backing, "podB"), 8).orElseThrow();
        assertThat(successor.epoch()).isEqualTo(4);

        ChainEntry opening = slotZero(backing, 4);
        assertThat(((Continue) opening).prevEpoch())
                .as("epoch 3 was opened after the probe, so the seal redrove above slot 0 "
                        + "and epoch 3 -- not epoch 1 -- is the ancestor")
                .isEqualTo(3);

        var resumed = successor.commit(new CommitRequest("podB", "i1", 1, "seg/b", counts(1)));
        assertThat(resumed.runs().getFirst().firstOffset())
                .as("epoch 1 assigned 0..2 and epoch 3 assigned 3..4, so this resumes at 5")
                .isEqualTo(5);
    }

    /**
     * ⚠️ THE BOUNDARY AT SLOT 1, which the sibling above cannot reach. Its
     * interleave writes TWO entries, so the seal redrives to slot 2 and
     * {@code landed.sequence() > 0} is never tested at 1 -- review measured
     * {@code > 1} surviving the whole suite because of it. Here the stalled
     * leader only OPENS, so the seal lands at exactly slot 1.
     */
    @Test
    void aCrossedEpochThatOnlyOPENEDIsStillTheAncestor() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        CommitLog first = new CommitLog(backing, PREFIX, 1);
        first.open(0, 0);
        first.commit("seg/a", counts(3));
        first.seal(3, 8);
        burnEpochs(backing, 3);

        String probed = new CommitLog(backing, PREFIX, 3).keyFor(0);
        BinStore racing = new OpenOnFirstProbeStore(backing, probed,
                () -> new CommitLog(backing, PREFIX, 3).open(1, 2));

        LocalSequencer successor = LocalSequencer.start(racing, PREFIX,
                manager(backing, "podB"), 8).orElseThrow();

        assertThat(((Continue) slotZero(backing, 4)).prevEpoch())
                .as("a CONTINUE alone makes epoch 3 opened, so the seal lands at slot 1 "
                        + "and epoch 3 is the ancestor")
                .isEqualTo(3);
        var resumed = successor.commit(new CommitRequest("podB", "i1", 1, "seg/b", counts(1)));
        assertThat(resumed.runs().getFirst().firstOffset())
                .as("epoch 3 committed nothing, so this resumes at epoch 1's mark")
                .isEqualTo(3);
    }

    /**
     * ⚠️ THE REDRIVE BUDGET IS WHY `recoverChainEnd()` PRECEDES THE SEAL, and
     * review MEASURED that without it this shape fails outright: `seal` proposes
     * at slot 0 and walks up one slot at a time, so against a racing chain
     * LONGER than the budget it never converges --
     * "seal did not converge at seq 8 within 8 redrive(s)" -- `start` releases
     * the lease, and the cluster has no sequencer until the next acquisition.
     * That is the NFR-9 outage the redrive branch exists to prevent.
     *
     * <p>⚠️ IT IS ALSO A REQUEST-COUNT DEFECT in the direction that matters:
     * one PUT plus one GET per entry of the racing chain, a rate scaling with
     * RECORDS COMMITTED. The probe turns that into one LIST plus one GET.
     */
    @Test
    void aRacingChainLONGERThanTheRedriveBudgetStillConverges() throws Exception {
        MemoryBinStore backing = new MemoryBinStore();
        CommitLog first = new CommitLog(backing, PREFIX, 1);
        first.open(0, 0);
        first.commit("seg/a", counts(3));
        first.seal(3, 8);
        burnEpochs(backing, 3);

        String probed = new CommitLog(backing, PREFIX, 3).keyFor(0);
        BinStore racing = new OpenOnFirstProbeStore(backing, probed, () -> {
            CommitLog stalled = new CommitLog(backing, PREFIX, 3);
            stalled.open(1, 2);
            // ⚠️ TWELVE, against a redrive budget of 8 below.
            for (int i = 0; i < 12; i++) {
                stalled.commit("seg/stalled" + i, counts(1));
            }
        });

        LocalSequencer successor = LocalSequencer.start(racing, PREFIX,
                manager(backing, "podB"), 8).orElseThrow();

        assertThat(((Continue) slotZero(backing, 4)).prevEpoch()).isEqualTo(3);
        var resumed = successor.commit(new CommitRequest("podB", "i1", 1, "seg/b", counts(1)));
        assertThat(resumed.runs().getFirst().firstOffset())
                .as("epoch 1 assigned 0..2 and epoch 3 assigned 3..14, so this resumes at 15")
                .isEqualTo(15);
    }
}
