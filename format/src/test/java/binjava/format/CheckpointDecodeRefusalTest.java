// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.format.Checkpoint.StreamOffsets;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * What {@link Checkpoint#decode} refuses (M4.8a).
 *
 * <p>⚠️ SPLIT FROM {@link CheckpointTest}, which reached the 500-line limit.
 * That file asks whether a checkpoint carrying valid facts survives a round
 * trip and encodes canonically; this one asks what happens to bytes no
 * conforming writer could have produced.
 *
 * <p>⚠️ EVERY TEST HERE EXISTS BECAUSE A MUTATION SURVIVED. The decode guards
 * were written production-first in an earlier round and only one of five had a
 * test, which is the inversion non-negotiable 3 forbids; each of these was
 * added after a reviewer measured the guard it covers being deletable with the
 * suite green.
 *
 * <p>⚠️ THE {@code < 0} HALF IS NOT THE {@code > limit} HALF. Four bounds here
 * read {@code X < 0 || X > limit}, and feeding only positive wide values pins
 * the second half alone. For {@code partitionId} and {@code podCount} the first
 * half is the one that matters: without it a negative-reading value narrows to
 * a VALID int and the object is accepted carrying the wrong stream, or no pods
 * at all.
 */
class CheckpointDecodeRefusalTest {

    private static UUID idx(int n) {
        return new UUID(0x1111_2222_3333_4444L, n);
    }

    private static byte[] oneStream(long next, long oldest) {
        Map<RunKey, StreamOffsets> one = new LinkedHashMap<>();
        one.put(new RunKey(idx(1), 1), new StreamOffsets(next, oldest));
        return new Checkpoint(1, one, Map.of("poda", 1L)).encode();
    }

    /** The uvarint encoding of {@code 0x1_0000_0001}, which narrows to 1 as an int. */
    private static final byte[] WIDE_ONE = {(byte) 0x81, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x10};

    /**
     * The uvarint encoding of {@code 0xFFFFFFFF00000001} — NEGATIVE as a long,
     * and exactly {@code 1} as an int.
     *
     * <p>⚠️ EVERY DECODE BOUND HERE IS {@code X < 0 || X > limit}, and the
     * wide-value tests feed only POSITIVE values, so the {@code > limit} half
     * alone refuses them and the {@code < 0} half of all four was once
     * unconstrained. That half is not cosmetic: deleting it flips REFUSE into
     * ACCEPT-WITH-WRONG-DATA in all four fields — measured, a partitionId of
     * this value decodes to partition 1, a stream or pod count to a checkpoint
     * carrying none, and a podId length narrows to 1 and eats a byte of the
     * next field.
     */
    private static final byte[] WIDE_NEGATIVE = {(byte) 0x81, (byte) 0x80, (byte) 0x80,
        (byte) 0x80, (byte) 0xf0, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x01};

    private static byte[] spliceAt(byte[] original, int at, byte[] replacement) {
        byte[] out = new byte[original.length + replacement.length - 1];
        System.arraycopy(original, 0, out, 0, at);
        System.arraycopy(replacement, 0, out, at, replacement.length);
        System.arraycopy(original, at + 1, out, at + replacement.length,
                original.length - at - 1);
        return out;
    }

    /**
     * Header is 8 bytes, then sequence and stream-count uvarints; a stream entry
     * is 16 bytes of UUID then partitionId, nextOffset, oldestRetainedOffset.
     * With every value under 128 each uvarint is one byte, so the offsets below
     * are computed from the layout rather than guessed.
     */
    private static final int HEADER = 8;
    private static final int FIRST_STREAM = HEADER + 1 + 1;
    private static final int NEXT_OFFSET_AT = FIRST_STREAM + 16 + 1;
    private static final int STREAM_ENTRY_BYTES = 16 + 1 + 1 + 1;


    @Test
    void bytesThatAreNotACheckpointAreRefusedRatherThanMisread() {
        assertThatThrownBy(() -> Checkpoint.decode(new byte[] {1, 2, 3}))
                .isInstanceOf(IOException.class);
        // ⚠️ SEVEN BYTES, not three: `bytes.length < 8` -> `< 7` survives a
        // three-byte fixture, and a 7-byte object whose first four bytes ARE
        // the magic then throws IndexOutOfBoundsException out of a method
        // declared `throws IOException` -- the same checked-versus-unchecked
        // contract the record-construction wrap exists for.
        assertThatThrownBy(() -> Checkpoint.decode(new byte[] {0x42, 0x43, 0x4b, 0x50, 0, 0, 0}))
                .as("a truncated header is corrupt input, not an unchecked throw")
                .isInstanceOf(IOException.class);
        // ⚠️ A VALID CHECKPOINT WITH ONLY ITS MAGIC CORRUPTED, because a
        // zero-filled buffer does NOT pin the magic check: measured, with that
        // check disabled a 32-byte zero array still fails on the trailing-bytes
        // guard, so the test passed against its own mutation. These bytes decode
        // cleanly once past the header, so only the magic can refuse them.
        byte[] foreign = CheckpointTest.fixture().encode();
        foreign[0] = 'X';
        assertThatThrownBy(() -> Checkpoint.decode(foreign))
                .as("an object that is not a checkpoint is refused on its magic")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("bad magic");
    }

    @Test
    void anUNKNOWNVersionSTOPSRatherThanGuessingTheLayout() throws Exception {
        // ⚠️ THE FORWARD-COMPAT GUARD `wire-format-change` RESTS ON, and it was
        // unconstrained: deleting the version check left the whole build green.
        byte[] bytes = CheckpointTest.fixture().encode();
        bytes[7] = 1;

        assertThatThrownBy(() -> Checkpoint.decode(bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("unsupported checkpoint version");
    }

    @Test
    void BYTESAfterTheLastFieldAreRefused() throws Exception {
        byte[] bytes = CheckpointTest.fixture().encode();
        byte[] withTail = java.util.Arrays.copyOf(bytes, bytes.length + 1);

        assertThatThrownBy(() -> Checkpoint.decode(withTail))
                .as("a longer object is not silently accepted as a shorter one")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("after its last field");
    }

    @Test
    void aCOUNTThatCannotFitInTheRemainingBytesIsRefusedBeforeAnyLoop() throws Exception {
        // ⚠️ THE COUNT IS NOT AN ALLOCATION SIZE, which is the defect M4.7a
        // measured in a delta. Both counts are bounded, and both bounds were
        // unconstrained until this test asserted each. A fixture claiming 0x7FFFFFFF alone cannot tell a real
        // bound from a constant cap, so this also asserts the MODEST case that
        // a cap would wave through.
        byte[] head = java.util.Arrays.copyOf(CheckpointTest.fixture().encode(), 9);
        byte[] absurd = java.util.Arrays.copyOf(head, head.length + 5);
        absurd[9] = (byte) 0xff;
        absurd[10] = (byte) 0xff;
        absurd[11] = (byte) 0xff;
        absurd[12] = (byte) 0xff;
        absurd[13] = 0x07;
        assertThatThrownBy(() -> Checkpoint.decode(absurd))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("streams but only");

        byte[] modest = java.util.Arrays.copyOf(head, head.length + 1);
        modest[9] = 9;
        assertThatThrownBy(() -> Checkpoint.decode(modest))
                .as("nine streams in eight bytes is refused too, not only a billion")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("streams but only");

        // ⚠️ AND THE POD COUNT, which an earlier version of this test claimed
        // ("both counts are bounded") while asserting only the stream half.
        // Its exposure is milder -- without the bound an absurd count still
        // ends in IOException, because every iteration consumes a byte -- so
        // what this pins is the operator's message, not accept-versus-refuse.
        byte[] pods = java.util.Arrays.copyOf(oneStream(90, 10), FIRST_STREAM
                + STREAM_ENTRY_BYTES + 1);
        pods[pods.length - 1] = 9;
        assertThatThrownBy(() -> Checkpoint.decode(pods))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("pods but only");
    }

    @Test
    void aTORNObjectIsAnIOExceptionRatherThanAnUncheckedOne() throws Exception {
        // ⚠️ EVERY WIRE VALUE GOES INTO A VALIDATING RECORD, so without a wrap
        // a torn checkpoint throws IllegalArgumentException straight past every
        // `catch (IOException)` a reader writes -- and M4.9 is written against
        // this contract. Here the two offsets are swapped on the WIRE only, so
        // the bytes say oldest(90) > next(10), which no encoder can produce.
        byte[] bytes = oneStream(90, 10);
        bytes[NEXT_OFFSET_AT] = 10;
        bytes[NEXT_OFFSET_AT + 1] = 90;

        assertThatThrownBy(() -> Checkpoint.decode(bytes))
                .as("a record invariant broken by the WIRE is corrupt input, not a bug")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("corrupt checkpoint");
    }

    @Test
    void aREPEATEDStreamIsRefusedRatherThanLastWinning() throws Exception {
        // ⚠️ MEASURED: a repeated RunKey decoded to whichever copy came last,
        // taking an offset BACKWARDS, with the trailing-bytes check still green.
        // ⚠️ THE STREAM HALF ONLY. An earlier name said "StreamOrPod" and
        // exercised just this one, so dropping the pod guard stayed green --
        // `aREPEATEDPodIsRefusedToo` is the other half.
        byte[] single = oneStream(90, 10);
        byte[] doubled = new byte[single.length + STREAM_ENTRY_BYTES];
        System.arraycopy(single, 0, doubled, 0, FIRST_STREAM);
        doubled[FIRST_STREAM - 1] = 2;
        System.arraycopy(single, FIRST_STREAM, doubled, FIRST_STREAM, STREAM_ENTRY_BYTES);
        System.arraycopy(single, FIRST_STREAM, doubled,
                FIRST_STREAM + STREAM_ENTRY_BYTES, STREAM_ENTRY_BYTES);
        System.arraycopy(single, FIRST_STREAM + STREAM_ENTRY_BYTES, doubled,
                FIRST_STREAM + 2 * STREAM_ENTRY_BYTES,
                single.length - FIRST_STREAM - STREAM_ENTRY_BYTES);

        assertThatThrownBy(() -> Checkpoint.decode(doubled))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("repeats stream");
    }

    @Test
    void aWIDEPartitionIdIsRefusedRatherThanNarrowedToADifferentStream() throws Exception {
        // ⚠️ THE FIELD THAT SAYS WHICH STREAM THE OFFSETS BELONG TO. A
        // partitionId is 64-bit on the wire and int in the record, and the cast
        // between them had no guard: measured, `0x1_0000_0001` decoded to
        // partitionId 1 and was returned as VALID -- one stream's offsets
        // silently attributed to another, with no exception for M4.9's
        // `catch (IOException)` to see. Only values narrowing to a NEGATIVE int
        // were refused, and by `RunKey` rather than by the decoder.
        byte[] single = oneStream(90, 10);
        int at = FIRST_STREAM + 16;
        byte[] wide = new byte[single.length + WIDE_ONE.length - 1];
        System.arraycopy(single, 0, wide, 0, at);
        System.arraycopy(WIDE_ONE, 0, wide, at, WIDE_ONE.length);
        System.arraycopy(single, at + 1, wide, at + WIDE_ONE.length, single.length - at - 1);

        assertThatThrownBy(() -> Checkpoint.decode(wide))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("partitionId");
    }

    /** Header, then the given body bytes -- an object that ends exactly where it says. */
    private static byte[] raw(int... body) {
        byte[] out = new byte[8 + body.length];
        out[0] = 0x42;
        out[1] = 0x43;
        out[2] = 0x4b;
        out[3] = 0x50;
        for (int i = 0; i < body.length; i++) {
            out[8 + i] = (byte) body[i];
        }
        return out;
    }

    @Test
    void aNEGATIVEReadingLengthOrCountIsRefusedInEVERYFieldThatNarrows() throws Exception {
        // ⚠️ THE FIRST THREE OBJECTS ARE BUILT BY HAND AND END EXACTLY AFTER
        // THE FIELD UNDER TEST, which is the whole point of building them; the
        // fourth is a splice, and works because replacing one byte with ten
        // still leaves the object ending at its last field. An earlier version spliced
        // a wide varint into a valid checkpoint, which left trailing bytes --
        // so `has bytes after its last field` fired whether or not the guard
        // was there, and a bare `isInstanceOf(IOException)` could not tell the
        // two apart. Measured: with only the `podCount < 0` half removed that
        // version was GREEN.
        // ⚠️ ALL THREE ARE ACCEPT-WITH-WRONG-DATA WITHOUT THEIR GUARD, measured,
        // not merely differently-worded refusals: a negative stream count
        // decodes to a checkpoint with NO streams, a negative pod count to one
        // with NO pods, and a negative podId length narrows to 1 and takes one
        // byte of the next field as a podId.
        int[] wideNeg = {0x81, 0x80, 0x80, 0x80, 0xf0, 0xff, 0xff, 0xff, 0xff, 0x01};

        byte[] negStreamCount = raw(0x01,
                wideNeg[0], wideNeg[1], wideNeg[2], wideNeg[3], wideNeg[4],
                wideNeg[5], wideNeg[6], wideNeg[7], wideNeg[8], wideNeg[9], 0x00);
        assertThatThrownBy(() -> Checkpoint.decode(negStreamCount))
                .as("a negative stream count decodes to NO streams without its guard")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("streams but only");

        byte[] negPodCount = raw(0x01, 0x00,
                wideNeg[0], wideNeg[1], wideNeg[2], wideNeg[3], wideNeg[4],
                wideNeg[5], wideNeg[6], wideNeg[7], wideNeg[8], wideNeg[9]);
        assertThatThrownBy(() -> Checkpoint.decode(negPodCount))
                .as("a negative pod count decodes to NO pods without its guard")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("pods but only");

        byte[] negPodLen = raw(0x01, 0x00, 0x01,
                wideNeg[0], wideNeg[1], wideNeg[2], wideNeg[3], wideNeg[4],
                wideNeg[5], wideNeg[6], wideNeg[7], wideNeg[8], wideNeg[9], 'p', 0x07);
        assertThatThrownBy(() -> Checkpoint.decode(negPodLen))
                .as("a negative podId length narrows to 1 without its guard")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("podId");

        byte[] single = oneStream(90, 10);
        assertThatThrownBy(() ->
                Checkpoint.decode(spliceAt(single, FIRST_STREAM + 16, WIDE_NEGATIVE)))
                .as("partitionId -- accepted as partition 1 without the < 0 half")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("partitionId");
    }

    @Test
    void aWIDEPodIdLengthIsRefusedRatherThanNarrowedToAShorterRead() throws Exception {
        // ⚠️ THE SAME NARROWING ONE FIELD OVER, and the guard added for it was
        // itself unconstrained: deleting it left the module green. ⚠️ WITH THE
        // GUARD DELETED THESE EXACT BYTES ARE REFUSED BY THE TRAILING-BYTES
        // CHECK, not accepted -- an earlier draft of this comment claimed
        // otherwise. What the guard buys is the diagnostic: without it the
        // failure names leftover bytes rather than the length that caused them.
        byte[] single = oneStream(90, 10);
        int at = single.length - 6;
        byte[] wide = new byte[single.length + WIDE_ONE.length - 1];
        System.arraycopy(single, 0, wide, 0, at);
        System.arraycopy(WIDE_ONE, 0, wide, at, WIDE_ONE.length);
        System.arraycopy(single, at + 1, wide, at + WIDE_ONE.length, single.length - at - 1);

        assertThatThrownBy(() -> Checkpoint.decode(wide))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("podId");
    }

    @Test
    void aREPEATEDPodIsRefusedToo() throws Exception {
        // ⚠️ THE OTHER HALF OF THE DUPLICATE GUARD; its sibling exercised only
        // streams. ⚠️ THE FIXTURE REPEATS IDENTICAL BYTES, so what the guard
        // buys here is refusal rather than a changed value -- an earlier draft
        // of this comment claimed `lastAppliedFlushSeq` goes BACKWARDS, which
        // these bytes cannot show. Last-wins over DIFFERING copies is what
        // moves a value, and refusing the repeat is what prevents both.
        byte[] single = oneStream(90, 10);
        int podsAt = single.length - 7;
        byte[] doubled = new byte[single.length + 6];
        System.arraycopy(single, 0, doubled, 0, podsAt);
        doubled[podsAt] = 2;
        System.arraycopy(single, podsAt + 1, doubled, podsAt + 1, 6);
        System.arraycopy(single, podsAt + 1, doubled, podsAt + 7, 6);

        assertThatThrownBy(() -> Checkpoint.decode(doubled))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("repeats pod");
    }
}
