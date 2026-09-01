// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import org.junit.jupiter.api.Test;

/**
 * The adaptive tagged membership filter's WIRE FORMAT (M2.4; ADR-0003):
 * encode/decode round-trips for all five tags, and golden files pinning each
 * tag's byte shape so it stays stable across versions
 * ({@code wire-format-change} skill).
 *
 * <p>⚠️ Kirsch-Mitzenmacher hashing and the "no false negative"/measured-FPR
 * properties are M2.5's job, exercised there. This file only proves the
 * codec reproduces whatever {@link BitSet} it was handed.
 */
class MembershipFilterTest {

    private static String golden(String name) throws Exception {
        try (var in = MembershipFilterTest.class.getResourceAsStream("/golden/" + name)) {
            assertThat(in).as(name + " must be on the test classpath").isNotNull();
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    void allEncodesToTheSingleCharacterTagAndAlwaysMightContain() throws IOException {
        MembershipFilter.All a = new MembershipFilter.All();
        assertThat(a.encode()).isEqualTo("A");
        assertThat(a.mightContain(0)).isTrue();
        assertThat(a.mightContain(9_999)).isTrue();
        assertThat(MembershipFilter.decode("A")).isEqualTo(a);
    }

    @Test
    void noneEncodesToTheSingleCharacterTagAndCarriesNoPredicate() throws IOException {
        MembershipFilter.None n = new MembershipFilter.None();
        assertThat(n.encode()).isEqualTo("N");
        assertThat(MembershipFilter.decode("N")).isEqualTo(n);
        // ⚠️ Deliberately no mightContain method exists on None -- a caller
        // must read the header instead (ADR-0003). This is enforced at
        // compile time, not by a runtime assertion here.
    }

    @Test
    void exactBitmapRoundTripsAndAnswersMembershipExactly() throws IOException {
        BitSet bits = new BitSet();
        bits.set(0);
        bits.set(2);
        bits.set(5);
        bits.set(9);
        MembershipFilter.ExactBitmap z = new MembershipFilter.ExactBitmap(bits);
        MembershipFilter.ExactBitmap round = (MembershipFilter.ExactBitmap) MembershipFilter.decode(z.encode());
        for (int i : new int[] {0, 2, 5, 9}) {
            assertThat(round.mightContain(i)).as("ordinal %d is a member", i).isTrue();
        }
        for (int i : new int[] {1, 3, 4, 6, 7, 8, 20}) {
            assertThat(round.mightContain(i)).as("ordinal %d is not a member", i).isFalse();
        }
    }

    @Test
    void exactBitmapNegativeOrdinalIsRefused() {
        MembershipFilter.ExactBitmap z = new MembershipFilter.ExactBitmap(new BitSet());
        assertThatThrownBy(() -> z.mightContain(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void runLengthRoundTripsAndAnswersMembershipExactly() throws IOException {
        // ⚠️ Dense-with-gaps, matching the tag's own reason to exist: two
        // separate runs (3-5 and 10), with a gap before, between and after.
        BitSet bits = new BitSet();
        bits.set(3, 6); // 3, 4, 5
        bits.set(10);
        MembershipFilter.RunLength r = new MembershipFilter.RunLength(bits);
        MembershipFilter.RunLength round = (MembershipFilter.RunLength) MembershipFilter.decode(r.encode());
        for (int i : new int[] {3, 4, 5, 10}) {
            assertThat(round.mightContain(i)).as("ordinal %d is a member", i).isTrue();
        }
        for (int i : new int[] {0, 1, 2, 6, 7, 8, 9, 11, 50}) {
            assertThat(round.mightContain(i)).as("ordinal %d is not a member", i).isFalse();
        }
    }

    @Test
    void runLengthHandlesAnEmptyBitmap() throws IOException {
        MembershipFilter.RunLength r = new MembershipFilter.RunLength(new BitSet());
        MembershipFilter.RunLength round = (MembershipFilter.RunLength) MembershipFilter.decode(r.encode());
        assertThat(round.mightContain(0)).isFalse();
        assertThat(round.mightContain(500)).isFalse();
        assertThat(round.mightContain(1_000)).isFalse();
    }

    @Test
    void runLengthHandlesAMemberStartingAtOrdinalZero() throws IOException {
        // ⚠️ A zero-length INITIAL off-run: the encoding's first uvarint must
        // be able to be 0, or a bitmap with ordinal 0 set could not round-trip.
        BitSet bits = new BitSet();
        bits.set(0, 3);
        MembershipFilter.RunLength r = new MembershipFilter.RunLength(bits);
        MembershipFilter.RunLength round = (MembershipFilter.RunLength) MembershipFilter.decode(r.encode());
        assertThat(round.mightContain(0)).isTrue();
        assertThat(round.mightContain(1)).isTrue();
        assertThat(round.mightContain(2)).isTrue();
        assertThat(round.mightContain(3)).isFalse();
    }

    @Test
    void runLengthRoundTripsARunLengthNeedingAMultiByteUvarint() throws IOException {
        // ⚠️ round-1 review (M2.4): every other RunLength test produces run
        // lengths under 128, so the uvarint's continuation-bit branch (values
        // needing more than one byte) never actually executed. A gap or run
        // of 128+ is routine at this project's own stated 10,000-index scale.
        BitSet bits = new BitSet();
        bits.set(200, 205); // a gap of 200 (multi-byte), then a run of 5
        MembershipFilter.RunLength r = new MembershipFilter.RunLength(bits);
        MembershipFilter.RunLength round = (MembershipFilter.RunLength) MembershipFilter.decode(r.encode());
        for (int i = 0; i < 200; i++) {
            assertThat(round.mightContain(i)).as("ordinal %d is not a member", i).isFalse();
        }
        for (int i = 200; i < 205; i++) {
            assertThat(round.mightContain(i)).as("ordinal %d is a member", i).isTrue();
        }
        assertThat(round.mightContain(205)).isFalse();
    }

    @Test
    void runLengthDecodeRefusesATruncatedMultiByteUvarint() {
        // ⚠️ round-1 review (M2.4): a validly base64url-encoded payload that
        // decodes to a lone continuation byte (top bit set, no following
        // byte) was never exercised -- decodeRefusesMalformedBase64ForZRAndB
        // only tests INVALID base64 characters, a different failure entirely.
        // "RgA" is valid base64url decoding to the single byte 0x80.
        assertThatThrownBy(() -> MembershipFilter.decode("RgA")).isInstanceOf(IOException.class);
    }

    @Test
    void bloomRoundTripsItsRawBitsKAndM() throws IOException {
        BitSet bits = new BitSet();
        bits.set(1);
        bits.set(4);
        bits.set(9);
        bits.set(15);
        MembershipFilter.Bloom b = new MembershipFilter.Bloom(3, bits, 16);
        MembershipFilter.Bloom round = (MembershipFilter.Bloom) MembershipFilter.decode(b.encode());
        assertThat(round.k()).isEqualTo(3);
        assertThat(round.m()).isEqualTo(16);
        assertThat(round.bits()).isEqualTo(bits);
    }

    @Test
    void bloomsPaddedLengthActuallyScalesWithMNotAFixedSize() throws IOException {
        // ⚠️ round-1 test-review (M2.4): every other Bloom test uses m=16, so
        // a production bug that pads to a hardcoded byte count (e.g. always 2
        // bytes) instead of deriving it from m/8 would pass every one of
        // them. m=32 here (4 bytes, none of them zero) is only distinguishable
        // from a hardcoded-2-bytes encoding by actually deriving the length
        // from m.
        BitSet bits = new BitSet();
        bits.set(1);
        bits.set(17);
        bits.set(25);
        bits.set(31); // sets the highest bit of all 4 bytes at m=32
        MembershipFilter.Bloom b = new MembershipFilter.Bloom(5, bits, 32);
        MembershipFilter.Bloom round = (MembershipFilter.Bloom) MembershipFilter.decode(b.encode());
        assertThat(round.k()).isEqualTo(5);
        assertThat(round.m()).isEqualTo(32);
        assertThat(round.bits()).isEqualTo(bits);
    }

    @Test
    void bloomPreservesMEvenWhenTheHighBitsAreAllZero() throws IOException {
        // ⚠️ THE reason m is padded rather than derived from
        // BitSet.toByteArray() directly: that method trims trailing all-zero
        // BYTES, so a filter whose top byte happens to be all zero would
        // decode with a SMALLER m than it was built with -- corrupting every
        // (mod m) hash position M2.5 computes against it.
        BitSet bits = new BitSet();
        bits.set(1); // nothing at all in the top byte of a 16-bit filter
        MembershipFilter.Bloom b = new MembershipFilter.Bloom(2, bits, 16);
        MembershipFilter.Bloom round = (MembershipFilter.Bloom) MembershipFilter.decode(b.encode());
        assertThat(round.m()).as("m survives even with an all-zero top byte").isEqualTo(16);
    }

    @Test
    void bloomKMustBeASingleDigitOneThroughNine() {
        BitSet bits = new BitSet();
        assertThatThrownBy(() -> new MembershipFilter.Bloom(0, bits, 8))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new MembershipFilter.Bloom(10, bits, 8))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void bloomMMustBeAPositiveMultipleOfEight() {
        BitSet bits = new BitSet();
        assertThatThrownBy(() -> new MembershipFilter.Bloom(3, bits, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new MembershipFilter.Bloom(3, bits, 10))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void bloomRefusesASetBitThatDoesNotFitItsOwnM() {
        BitSet bits = new BitSet();
        bits.set(16); // bit 16 needs m >= 17, i.e. at least 24 once byte-aligned
        assertThatThrownBy(() -> new MembershipFilter.Bloom(3, bits, 16))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void decodeRefusesAnUnrecognisedTag() {
        // ⚠️ Reader-side "treat like N" policy is M2.7's job, not this one's --
        // this codec's contract is to fail loudly on a tag it does not know.
        assertThatThrownBy(() -> MembershipFilter.decode("Q")).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> MembershipFilter.decode("")).isInstanceOf(IOException.class);
    }

    @Test
    void decodeRefusesAPayloadOnATagThatCarriesNone() {
        assertThatThrownBy(() -> MembershipFilter.decode("Ax")).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> MembershipFilter.decode("Nx")).isInstanceOf(IOException.class);
    }

    @Test
    void decodeRefusesMalformedBase64ForZRAndB() {
        assertThatThrownBy(() -> MembershipFilter.decode("Z***")).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> MembershipFilter.decode("R***")).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> MembershipFilter.decode("B3***")).isInstanceOf(IOException.class);
    }

    @Test
    void decodeRefusesAnEmptyBPayloadOrAMissingKDigit() {
        assertThatThrownBy(() -> MembershipFilter.decode("B")).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> MembershipFilter.decode("Bx")).isInstanceOf(IOException.class); // k is not a digit
    }

    // -- golden files: committed once, never regenerated to make a test pass --

    @Test
    void theCommittedGoldenAFilterStillDecodes() throws Exception {
        MembershipFilter.All a = (MembershipFilter.All) MembershipFilter.decode(golden("filter-a.txt"));
        assertThat(a.mightContain(0)).isTrue();
    }

    @Test
    void theCommittedGoldenNFilterStillDecodes() throws Exception {
        assertThat(MembershipFilter.decode(golden("filter-n.txt"))).isEqualTo(new MembershipFilter.None());
    }

    @Test
    void theCommittedGoldenZFilterStillDecodesToItsOriginalMembership() throws Exception {
        MembershipFilter.ExactBitmap z =
                (MembershipFilter.ExactBitmap) MembershipFilter.decode(golden("filter-z.txt"));
        for (int i : new int[] {0, 2, 5, 9}) {
            assertThat(z.mightContain(i)).isTrue();
        }
        assertThat(z.mightContain(1)).isFalse();
        assertThat(z.mightContain(8)).isFalse();
    }

    @Test
    void theCommittedGoldenRFilterStillDecodesToItsOriginalMembership() throws Exception {
        MembershipFilter.RunLength r =
                (MembershipFilter.RunLength) MembershipFilter.decode(golden("filter-r.txt"));
        for (int i : new int[] {3, 4, 5, 10}) {
            assertThat(r.mightContain(i)).isTrue();
        }
        assertThat(r.mightContain(2)).isFalse();
        assertThat(r.mightContain(6)).isFalse();
        assertThat(r.mightContain(11)).isFalse();
    }

    @Test
    void theCommittedGoldenBFilterStillDecodesToItsOriginalKMAndBits() throws Exception {
        MembershipFilter.Bloom b = (MembershipFilter.Bloom) MembershipFilter.decode(golden("filter-b.txt"));
        assertThat(b.k()).isEqualTo(3);
        assertThat(b.m()).isEqualTo(16);
        for (int i : new int[] {1, 4, 9, 15}) {
            assertThat(b.bits().get(i)).isTrue();
        }
        assertThat(b.bits().get(0)).isFalse();
    }
}
