// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.format.SegmentCommit.Attribution;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * ADR-0036: a delta carries `(podId, incarnationId, flushSeq)` PER
 * `SegmentCommit`, so a successor can rebuild the idempotency window from the
 * uncheckpointed tail.
 *
 * <p>⚠️ PER SEGMENT, NOT PER DELTA. `commitAll` is the primitive and a batch may
 * mix pods, so a once-per-delta field is byte-equivalent in production today —
 * no production delta is multi-segment — and drops every pod but one on M5's
 * first forwarded multi-pod batch. The fixtures below give segments DIFFERING
 * `flushSeq`, which is what a hoist cannot survive; differing `podId` alone
 * would not, because the partial hoist keeps `podId` per segment.
 *
 * <p>⚠️ A DELTA CARRYING NO ATTRIBUTION MUST ENCODE EXACTLY AS IT DOES TODAY.
 * `GoldenChainEntryTest` asserts byte-identity against `chain-delta-v0.bin` and
 * `chain-delta-batched-v1.bin`, and `ChainEntry` states why: a version IS a
 * layout, and every delta already in a bucket stays readable for the retention
 * window without a rewrite.
 */
class AttributedDeltaTest {

    private static RunCommit run(int partition, int count, long first) {
        return new RunCommit(new RunKey(UUID.fromString(
                "00000000-0000-0000-0000-0000000000aa"), partition), count, first);
    }

    private static SegmentCommit seg(String key, String pod, String inc, long flushSeq) {
        return new SegmentCommit(key, List.of(run(0, 2, 100)), new Attribution(pod, inc, flushSeq));
    }

    @Test
    void oneAttributedSegmentRoundTripsWithItsTriple() throws Exception {
        CommitDelta original = new CommitDelta(7, List.of(seg("seg/a", "poda", "inc-1", 4)));

        CommitDelta decoded = CommitDelta.decode(original.encode());

        assertThat(decoded.segments()).hasSize(1);
        assertThat(decoded.segments().get(0).attribution())
                .as("the triple survives the round trip")
                .isEqualTo(new Attribution("poda", "inc-1", 4));
        assertThat(decoded).isEqualTo(original);
    }

    /**
     * ⚠️ THE HOIST TEST. Segments differ in `flushSeq`, so an encoder that wrote
     * the triple once per delta and fanned it out on decode cannot reproduce
     * them — which differing `podId` alone would not catch.
     */
    @Test
    void everySegmentKeepsItsOwnTripleAcrossABatch() throws Exception {
        CommitDelta original = new CommitDelta(9, List.of(
                seg("seg/a", "poda", "inc-1", 0),
                seg("seg/b", "poda", "inc-1", 1),
                seg("seg/c", "podb", "inc-2", 0)));

        List<SegmentCommit> back = CommitDelta.decode(original.encode()).segments();

        assertThat(back).hasSize(3);
        assertThat(back.get(0).attribution()).isEqualTo(new Attribution("poda", "inc-1", 0));
        assertThat(back.get(1).attribution())
                .as("the SECOND segment keeps its own flushSeq, not the first's")
                .isEqualTo(new Attribution("poda", "inc-1", 1));
        assertThat(back.get(2).attribution()).isEqualTo(new Attribution("podb", "inc-2", 0));
    }

    /** A pre-ADR-0036 delta is unchanged on the wire, so no bucket churns. */
    @Test
    void aDeltaWithNoAttributionEncodesExactlyAsBefore() throws Exception {
        CommitDelta legacy = new CommitDelta(3, List.of(
                new SegmentCommit("seg/legacy", List.of(run(0, 2, 100)))));

        byte[] bytes = legacy.encode();

        // ⚠️ bytes[7], NOT bytes[4]: the version is a big-endian int, so [4] is
        // its high byte and always 0x00 -- an assertion on it passes even with
        // the v0 branch deleted entirely, which review measured.
        assertThat(bytes[7]).as("still VERSION_DELTA, not a new version")
                .isEqualTo((byte) ChainEntry.VERSION_DELTA);
        assertThat(CommitDelta.decode(bytes).segments().get(0).attribution())
                .as("and decodes back with no attribution")
                .isNull();
    }

    // ⚠️ THE FAILURE PATH. `KIND_DELTA_ATTRIBUTED` shipped with a happy path and
    // nothing else: review measured `if (count <= 0 || count > c.remaining())`
    // weakened to `if (count == 0)` leaving the FULL build green, while an entry
    // claiming 0x7FFFFFFF segments raised OutOfMemoryError from
    // `new ArrayList<>((int) count)` -- past `ChainReplay`'s `throws IOException`.
    // `BatchedCommitDeltaTest` pins exactly this for `KIND_DELTA` with five
    // tests; the new kind inherited the norm and none of them.

    private static byte[] spliceAt(byte[] original, int at, byte[] replacement) {
        byte[] out = new byte[original.length + replacement.length - 1];
        System.arraycopy(original, 0, out, 0, at);
        System.arraycopy(replacement, 0, out, at, replacement.length);
        System.arraycopy(original, at + 1, out, at + replacement.length,
                original.length - at - 1);
        return out;
    }

    /** The segment count is the byte after the header, kind and sequence. */
    private static int countOffset() {
        return 4 + 4 + 1 + 1;
    }

    @Test
    void anOverLargeSegmentCountIsREFUSEDNotAllocated() throws Exception {
        byte[] one = new CommitDelta(9, List.of(seg("seg/a", "poda", "inc-1", 4))).encode();
        // 0x7FFFFFFF as a uvarint: an allocation the VM cannot satisfy.
        byte[] huge = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x07};

        assertThatThrownBy(() -> CommitDelta.decode(spliceAt(one, countOffset(), huge)))
                .as("refused by the bound, never reaching new ArrayList")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("segments");
    }

    @Test
    void aNegativeReadingSegmentCountIsREFUSEDNotAllocated() throws Exception {
        byte[] one = new CommitDelta(9, List.of(seg("seg/a", "poda", "inc-1", 4))).encode();
        byte[] negativeNarrowingToMaxInt = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
            (byte) 0xF7, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x01};

        assertThatThrownBy(() ->
                CommitDelta.decode(spliceAt(one, countOffset(), negativeNarrowingToMaxInt)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("segments");
    }

    /**
     * ⚠️ THE INCARNATION LENGTH, which the sibling test names and does not
     * splice, and the SIGN half for both fields — the identical hole closed in
     * `Checkpoint.incLen` in this same commit, where the fix crossed into this
     * helper and the test did not.
     */
    @Test
    void bothAttributionLengthsAreBoundedInBothDirections() throws Exception {
        byte[] one = new CommitDelta(9, List.of(seg("seg/a", "poda", "inc-1", 4))).encode();
        int podAt = prefixOf(one, (byte) 4, "poda");
        int incAt = prefixOf(one, (byte) 5, "inc-1");

        // negative as a long, narrows to the CORRECT length: only the sign check refuses it
        byte[] negativeNarrowingToFour = {(byte) 0x84, (byte) 0x80, (byte) 0x80, (byte) 0x80,
            (byte) 0xF0, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0x01};
        assertThatThrownBy(() -> CommitDelta.decode(spliceAt(one, podAt, negativeNarrowingToFour)))
                .as("a NEGATIVE podId length narrowing to a valid int")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("podId");

        byte[] wideFive = {(byte) 0x85, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x10};
        assertThatThrownBy(() -> CommitDelta.decode(spliceAt(one, incAt, wideFive)))
                .as("and the INCARNATION length, not only the podId")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("incarnationId");
    }

    /** The index of a length prefix immediately followed by its own bytes. */
    private static int prefixOf(byte[] bytes, byte len, String value) {
        byte[] want = value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        outer:
        for (int i = bytes.length - want.length - 1; i > 8; i--) {
            if (bytes[i] != len) {
                continue;
            }
            for (int j = 0; j < want.length; j++) {
                if (bytes[i + 1 + j] != want[j]) {
                    continue outer;
                }
            }
            return i;
        }
        throw new AssertionError("no length-prefixed " + value + " in the fixture");
    }

    /**
     * ⚠️ THE GUARDS ARE ON THE WIRE PATH. `Attribution` is built from decoded
     * bytes, so its compact constructor is the last thing between a corrupt
     * object and a delta claiming an empty pod. Review measured the whole
     * constructor deletable with three modules green: rewriting a length prefix
     * to 0 returned a valid delta with `podId=[]`. The sibling API field got a
     * refusal case in this same commit; the more exposed one had none.
     */
    @Test
    void aBlankPodIdOnTheWireIsREFUSED() throws Exception {
        byte[] one = new CommitDelta(9, List.of(seg("seg/a", "poda", "inc-1", 4))).encode();
        int podAt = prefixOf(one, (byte) 4, "poda");

        // length 0: the field decodes as "" and the record must refuse it
        byte[] blanked = new byte[one.length - 4];
        System.arraycopy(one, 0, blanked, 0, podAt);
        blanked[podAt] = 0;
        System.arraycopy(one, podAt + 5, blanked, podAt + 1, one.length - podAt - 5);

        assertThatThrownBy(() -> CommitDelta.decode(blanked))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("never blank");
    }

    /**
     * ⚠️ A DELTA CANNOT MIX attributed and unattributed segments: the reader
     * would have no flag to tell which shape each carries. Review measured the
     * `IllegalStateException` replaced by `return;` surviving two modules.
     */
    @Test
    void aDeltaMixingAttributedAndUnattributedSegmentsIsREFUSED() {
        CommitDelta mixed = new CommitDelta(9, List.of(
                seg("seg/a", "poda", "inc-1", 0),
                new SegmentCommit("seg/b", List.of(run(0, 1, 5)))));

        assertThatThrownBy(mixed::encode)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("mix");
    }
}
