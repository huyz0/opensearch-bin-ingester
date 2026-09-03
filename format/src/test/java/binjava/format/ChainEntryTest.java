// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * ⚠️ THREE SHAPES AT ONE KEY (M4.5, ADR-0028). The chain carries deltas, and
 * from M4 it also carries the two entries that have no runs at all: a
 * {@code SEAL} ending an epoch's chain, and a {@code CONTINUE} opening the
 * next one with a link back across the boundary.
 *
 * <p>⚠️ The version distinguishes LAYOUTS, not releases. v0 <em>is</em> "a
 * delta" — it has no kind field, so a v0 object can only ever be one — and v1
 * <em>is</em> "a kinded entry". That is why a delta still writes v0: nothing
 * about a delta changed, so re-encoding one would churn bytes and golden files
 * to say the same thing.
 */
class ChainEntryTest {

    private static final RunKey KEY =
            new RunKey(UUID.fromString("00000000-0000-0000-0000-0000000000aa"), 3);

    @Test
    void aSealRoundTripsAndCarriesNoRuns() throws Exception {
        Seal seal = new Seal(42, 8);
        ChainEntry back = ChainEntry.decode(seal.encode());
        assertThat(back).isEqualTo(seal);
        assertThat(back.sequence()).isEqualTo(42);
        assertThat(((Seal) back).continuedAt())
                .as("the sealer stamps the epoch it already holds -- the corpus's "
                        + "SEAL{continuedAt}, so a reader can cross the boundary forward")
                .isEqualTo(8);
    }

    @Test
    void aContinueRoundTripsAndNamesThePreviousChain() throws Exception {
        Continue cont = new Continue(0, 7, 1234);
        ChainEntry back = ChainEntry.decode(cont.encode());
        assertThat(back).isEqualTo(cont);
        assertThat(((Continue) back).prevEpoch()).isEqualTo(7);
        assertThat(((Continue) back).prevSeq()).isEqualTo(1234);
    }

    @Test
    void aV0DeltaStillParsesThroughTheSharedDecoder() throws Exception {
        // ⚠️ THE COMPATIBILITY HALF. Every delta in the bucket is v0, and they
        // outlive this change by the whole retention window.
        CommitDelta delta = new CommitDelta(5, "seg/a",
                List.of(new RunCommit(KEY, 2, 10)));
        byte[] bytes = delta.encode();
        assertThat(ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt(4))
                .as("a delta still writes v0 -- nothing about a delta changed")
                .isZero();
        assertThat(ChainEntry.decode(bytes)).isEqualTo(delta);
    }

    @Test
    void anUnknownKindStopsRatherThanBeingIgnored() throws Exception {
        // ⚠️ STOP, NOT IGNORE, and the wire-format-change skill requires the
        // answer be stated rather than left to whichever branch happens to run.
        // Skipping an entry a reader does not understand is how a chain silently
        // loses a SEAL and a reader keeps applying a discarded suffix -- I3.
        byte[] seal = new Seal(1, 3).encode();
        byte[] mutant = seal.clone();
        mutant[8] = (byte) 0x63;
        assertThatThrownBy(() -> ChainEntry.decode(mutant))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("kind");
    }

    @Test
    void anUnknownVersionStopsToo() throws Exception {
        byte[] bytes = new Seal(1, 3).encode();
        ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).putInt(4, 99);
        assertThatThrownBy(() -> ChainEntry.decode(bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version");
    }

    @Test
    void trailingBytesAreRefusedOnEveryShape() throws Exception {
        // ⚠️ The DELTA is in this list because it is the shape with the most
        // to lose: it is the one every object in the bucket already is, and the
        // one whose own trailing-byte check the shared decoder replaced.
        for (ChainEntry e : List.of(new Seal(1, 3), new Continue(0, 1, 2),
                new CommitDelta(1, "s", List.of(new RunCommit(KEY, 1, 0))))) {
            byte[] padded = java.util.Arrays.copyOf(e.encode(), e.encode().length + 1);
            assertThatThrownBy(() -> ChainEntry.decode(padded))
                    .as("a chain entry with bytes after its last field is not that entry")
                    .isInstanceOf(IOException.class);
        }
    }

    @Test
    void aContinueNeverEncodesAnAbsentPreviousChain() {
        // ⚠️ M4.4b reserved epoch 0 for the UNLEASED chain, so 0 names a live
        // one and cannot mean "none". The resolution is not a different
        // sentinel: it is that ABSENCE IS NOT REPRESENTABLE, because every
        // chain above epoch 0 has a predecessor and epoch 0 is the unleased
        // chain that conceptually always exists. A first leader at epoch 1
        // continues from {prevEpoch=0, prevSeq=0}, which truthfully says the
        // unleased chain was empty rather than that there was none.
        assertThatThrownBy(() -> new Continue(0, -1, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("prevEpoch");
        assertThatThrownBy(() -> new Continue(0, 0, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("prevSeq");
        assertThat(new Continue(0, 0, 0).prevEpoch())
                .as("epoch 0 with seq 0 is a real link, not an absent one")
                .isZero();
    }

    @Test
    void aSealNeverContinuesAtTheUnleasedEpoch() {
        // ⚠️ Epoch 0 is the UNLEASED chain (M4.4b) and a seal is written by a
        // leader, whose first term is 1. A seal continuing at 0 would claim
        // failover TO the chain every leaderless caller writes.
        assertThatThrownBy(() -> new Seal(1, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("continuedAt");
    }

    @Test
    void aSealAndAContinueRefuseANegativeSequence() {
        assertThatThrownBy(() -> new Seal(-1, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sequence");
        assertThatThrownBy(() -> new Continue(-1, 0, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("sequence");
    }

    @Test
    void aValueTheWireCanCarryButARecordRefusesIsAnIoFailure() throws Exception {
        // ⚠️ CORRUPT INPUT IS NOT A PROGRAMMING ERROR. A record's
        // IllegalArgumentException is unchecked, so a torn or zero-filled
        // object would throw straight past `CommitLog.recover`'s own
        // `throws IOException` and every caller catching it -- a store read
        // failing as if the code had a bug.
        // ⚠️ These bytes are trivially reachable: eleven of them, and the
        // second is a zero-filled object.
        byte[] sealContinuedAtZero = {0x42, 0x44, 0x4C, 0x54, 0, 0, 0, 1, 0x01, 0x00, 0x00};
        assertThatThrownBy(() -> ChainEntry.decode(sealContinuedAtZero))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("corrupt chain entry")
                .hasMessageContaining("continuedAt");

        byte[] continueNegativePrevEpoch = {0x42, 0x44, 0x4C, 0x54, 0, 0, 0, 1, 0x02, 0x00,
            (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF,
            (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x01, 0x00};
        assertThatThrownBy(() -> ChainEntry.decode(continueNegativePrevEpoch))
                .as("a 10-byte varint with bit 63 set decodes to a negative long")
                .isInstanceOf(IOException.class)
                .hasMessageContaining("corrupt chain entry");
    }

    @Test
    void aHugeLengthFieldIsRefusedRatherThanAllocated() throws Exception {
        // ⚠️ THE CLAIM `Cursor` MAKES ABOUT ITSELF. `i + n` overflows int for a
        // large length field, so the guard it was written to be let exactly the
        // allocation it forbids through -- as an OutOfMemoryError out of a
        // method that promises an IOException.
        // ⚠️ Fourteen bytes: a v0 delta whose key length is 0x7FFFFFFF
        // (8 header + 1 sequence + a 5-byte varint).
        byte[] hugeKeyLength = {0x42, 0x44, 0x4C, 0x54, 0, 0, 0, 0, 0x05,
            (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x07};
        assertThatThrownBy(() -> ChainEntry.decode(hugeKeyLength))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("ends inside a field");
    }

    @Test
    void decodingAsADeltaRefusesTheOtherShapesRatherThanCastingBlindly() throws Exception {
        // ⚠️ A cast would succeed until the day a seal appears, and then fail
        // as an unchecked ClassCastException from a method declared to throw
        // IOException. ADR-0028 states this; nothing pinned it.
        byte[] seal = new Seal(9, 2).encode();
        assertThatThrownBy(() -> CommitDelta.decode(seal))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("expected a delta");
        assertThatThrownBy(() -> CommitDelta.decode(new Continue(0, 1, 2).encode()))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("expected a delta");
    }
}
