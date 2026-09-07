// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.format.Checkpoint.PodState;
import binjava.format.SegmentCommit.Attribution;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Objects written by an ADR-0036 build must still parse (M4.10c).
 *
 * <p>⚠️ Do NOT regenerate these to make a test pass — the discipline
 * {@link GoldenChainEntryTest} states. They were generated once and verified BY
 * HAND against the layout: the delta header really is {@code 42 44 4c 54} then
 * big-endian version 1 then kind {@code 03}; {@code 04 70 6f 64 61} really is
 * "poda"; and the first segment's trailing {@code 04} really is flushSeq 4 while
 * the second's is 5. The checkpoint really is {@code 42 43 4b 50} then version
 * 1, and its second pod's trailing {@code 00} really is the no-pointer flag.
 *
 * <p>⚠️ THE SEGMENTS DIFFER IN {@code flushSeq}, NOT ONLY IN {@code podId}. A
 * fixture whose segments differ only by pod is satisfied by the PARTIAL hoist —
 * podId per segment, incarnation and flushSeq once per delta and fanned out on
 * decode — which is the mutation these bytes exist to kill.
 *
 * <p>⚠️ AND THE CHECKPOINT MIXES a pod with a pointer and one without, because
 * that is the NORMAL state after an upgrade: a pod that has committed since
 * carries an incarnation, one recovered from a v0 checkpoint does not.
 */
class GoldenAttributedTest {

    private static byte[] golden(String name) throws Exception {
        try (var in = GoldenAttributedTest.class.getResourceAsStream("/golden/" + name)) {
            assertThat(in).as(name + " must be on the test classpath").isNotNull();
            return in.readAllBytes();
        }
    }

    @Test
    void anAttributedDeltaFromAnEarlierBuildStillParses() throws Exception {
        CommitDelta d = (CommitDelta) ChainEntry.decode(golden("chain-delta-attributed-v1.bin"));

        assertThat(d.sequence()).isEqualTo(9);
        assertThat(d.segments()).hasSize(3);
        // ⚠️ VALUES FIRST, hand-verified, before any byte comparison: a re-encode
        // check alone is satisfied by a codec that is self-consistently wrong.
        assertThat(d.segments().get(0).attribution())
                .isEqualTo(new Attribution("poda", "inc-a", 4));
        assertThat(d.segments().get(1).attribution())
                .as("the SECOND segment's own flushSeq, not the first's")
                .isEqualTo(new Attribution("poda", "inc-a", 5));
        assertThat(d.segments().get(2).attribution())
                .isEqualTo(new Attribution("podb", "inc-b", 0));
    }

    @Test
    void reEncodingTheAttributedDeltaReproducesItByteForByte() throws Exception {
        byte[] bytes = golden("chain-delta-attributed-v1.bin");
        assertThat(((CommitDelta) ChainEntry.decode(bytes)).encode()).isEqualTo(bytes);
    }

    @Test
    void anAttributedCheckpointFromAnEarlierBuildStillParses() throws Exception {
        Checkpoint c = Checkpoint.decode(golden("checkpoint-v1.bin"));

        assertThat(c.sequence()).isEqualTo(9);
        assertThat(c.pods().get("poda")).isEqualTo(new PodState("inc-a", 42, 7, 1234));
        assertThat(c.pods().get("podb"))
                .as("a slot with no pointer, which is what a v0 recovery leaves")
                .isEqualTo(PodState.bare(77));
        assertThat(c.pods().get("podb").hasPointer()).isFalse();
    }

    @Test
    void reEncodingTheAttributedCheckpointReproducesItByteForByte() throws Exception {
        byte[] bytes = golden("checkpoint-v1.bin");
        assertThat(Checkpoint.decode(bytes).encode()).isEqualTo(bytes);
    }

    /** The frozen layouts are untouched: an unattributed object still encodes as before. */
    @Test
    void theLegacyGoldensAreUnchangedByThisChange() throws Exception {
        byte[] v0 = golden("chain-delta-v0.bin");
        assertThat(((CommitDelta) ChainEntry.decode(v0)).encode())
                .as("a delta carrying no attribution still encodes as VERSION_DELTA")
                .isEqualTo(v0);
        byte[] ckpt = golden("checkpoint-v0.bin");
        assertThat(Checkpoint.decode(ckpt).encode())
                .as("and a checkpoint whose slots carry no incarnation stays v0")
                .isEqualTo(ckpt);
    }

    @Test
    void theDeltaGoldenUsesTheReservedKindAndTheCheckpointItsOwnVersion() throws Exception {
        assertThat(golden("chain-delta-attributed-v1.bin")[7])
                .as("VERSION_KINDED, so the delta stays a ChainEntry")
                .isEqualTo((byte) ChainEntry.VERSION_KINDED);
        assertThat(golden("chain-delta-attributed-v1.bin")[8])
                .as("KIND_DELTA_ATTRIBUTED, reserved for exactly this")
                .isEqualTo((byte) ChainEntry.KIND_DELTA_ATTRIBUTED);
        assertThat(golden("checkpoint-v1.bin")[7])
                .as("a checkpoint is NOT a chain entry and carries its own version")
                .isEqualTo((byte) Checkpoint.VERSION_ATTRIBUTED);
    }
}
