// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;
import java.util.OptionalLong;
import java.util.UUID;
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
        // ⚠️ This codec's OWN contract (decode) is to fail loudly on a tag it
        // does not know -- that stays true. Reader-side "treat like N" policy
        // (M2.7) lives on decodeOrMustRead, exercised below, never here.
        assertThatThrownBy(() -> MembershipFilter.decode("Q")).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> MembershipFilter.decode("")).isInstanceOf(IOException.class);
    }

    @Test
    void decodeOrMustReadTreatsAnUnrecognisedTagExactlyLikeN() throws IOException {
        // ⚠️ M2.7; ADR-0003: "an unknown tag and N both mean must read the
        // header, never no match." "Q" simulates a tag this reader has never
        // seen -- a future writer's format -- not a malformed one.
        assertThat(MembershipFilter.decodeOrMustRead("Q")).isEqualTo(new MembershipFilter.None());
        assertThat(MembershipFilter.decodeOrMustRead("Qwhatever-a-future-writer-put-here"))
                .isEqualTo(new MembershipFilter.None());
        assertThat(MembershipFilter.decodeOrMustRead("N")).isEqualTo(new MembershipFilter.None());
    }

    @Test
    void decodeOrMustReadStillRefusesAMalformedPayloadOfAKnownTag() {
        // ⚠️ Only the TAG is forward-compatible. A malformed payload for a
        // tag this class DOES recognise is real corruption of an
        // understood format, not a future one -- swallowing it into "must
        // read" would hide the corruption instead of surfacing it. All FIVE
        // known tags are exercised here, not just A/N/B: test-reviewer
        // (M2.7, round 1) found Z and R untested, so a mutation dropping
        // either from the recognised-tag guard (silently turning a REAL Z or
        // R filter into "must read" on every access, defeating the whole
        // cost point of that filter) survived every test in this file.
        assertThatThrownBy(() -> MembershipFilter.decodeOrMustRead("Ax")).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> MembershipFilter.decodeOrMustRead("Nx")).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> MembershipFilter.decodeOrMustRead("Z***")).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> MembershipFilter.decodeOrMustRead("R***")).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> MembershipFilter.decodeOrMustRead("B")).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> MembershipFilter.decodeOrMustRead("")).isInstanceOf(IOException.class);
    }

    @Test
    void decodeOrMustReadRoundTripsEveryKnownTagUnchanged() throws IOException {
        // ⚠️ test-reviewer (M2.7, round 1): the OTHER new tests here only
        // ever pass decodeOrMustRead a Q, N, A or B string -- never a real Z
        // or R -- so a mutation that silently added 'Z' or 'R' to the
        // "unrecognised" branch (returning None for a filter this class
        // actually knows how to parse) would survive undetected. Each of the
        // five known tags must decode to ITS OWN real variant, never None.
        BitSet bits = new BitSet();
        bits.set(1);
        bits.set(9);
        MembershipFilter.ExactBitmap z = new MembershipFilter.ExactBitmap(bits);
        MembershipFilter.RunLength r = new MembershipFilter.RunLength(bits);
        MembershipFilter.Bloom b = MembershipFilter.Bloom.of(java.util.List.of(3, 7), 32, 2);
        MembershipFilter.All a = new MembershipFilter.All();

        assertThat(MembershipFilter.decodeOrMustRead(a.encode())).isEqualTo(a);
        assertThat(MembershipFilter.decodeOrMustRead(z.encode())).isEqualTo(z);
        assertThat(MembershipFilter.decodeOrMustRead(r.encode())).isEqualTo(r);
        assertThat(MembershipFilter.decodeOrMustRead(b.encode())).isEqualTo(b);
    }

    /**
     * ⚠️ Acceptance criterion 9 / test-plan row T0 (M2 SPEC): "asserting the
     * reader still finds the data," not merely that some function returns a
     * particular enum value. Builds a REAL segment (independent of the
     * ingest module) carrying one record for an index, simulates a reader
     * that received a key whose filter component uses a tag byte outside
     * {@code {A,N,Z,R,B}} -- a future writer's format -- and proves the
     * record is still found: the reader-decision helper below never treats
     * "must read" as a reason to skip a segment, so it always falls through
     * to actually reading it.
     */
    @Test
    void aReaderTreatingAnUnknownTagAsMustReadStillFindsTheRecord() throws Exception {
        UUID indexId = UUID.fromString("00000000-0000-0000-0000-0000000000ee");
        RunKey key = new RunKey(indexId, 0);
        SegmentRecord record = new SegmentRecord("doc-1", OpType.INDEX, OptionalLong.of(1),
                "{\"future\":true}".getBytes(StandardCharsets.UTF_8));
        SegmentWriter writer = new SegmentWriter();
        writer.add(key, record, 1_700_000_000_000L);
        byte[] segment = writer.toByteArray(1_700_000_000_000L);

        // A future writer's key embeds a tag this reader has never seen.
        MembershipFilter filter = MembershipFilter.decodeOrMustRead("Qsome-future-payload");

        // ⚠️ The decision a recovery/GC-path reader would make BEFORE paying
        // for a GET: skip fetching only on POSITIVE proof of absence. None
        // (what an unknown tag decodes to here) carries no such proof, by
        // design -- it exposes no mightContain at all, forcing this decision
        // to be made explicitly rather than by an always-true default that is
        // easy to forget the reason for.
        boolean wouldSkip = switch (filter) {
            case MembershipFilter.All ignored -> false;
            case MembershipFilter.ExactBitmap f -> !f.mightContain(0);
            case MembershipFilter.RunLength f -> !f.mightContain(0);
            case MembershipFilter.Bloom f -> !f.mightContain(0);
            case MembershipFilter.None ignored -> false; // must read -- never skip
        };
        assertThat(wouldSkip).as("an unknown tag must never be treated as proof of absence").isFalse();

        // Not skipped -- so the reader actually reads it, and finds the record.
        SegmentReader reader = SegmentReader.open(segment);
        assertThat(reader.find(key)).isPresent();
        assertThat(reader.read(reader.find(key).orElseThrow()))
                .as("the record a wrongly-skipped fetch would have silently lost")
                .hasSize(1);
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
