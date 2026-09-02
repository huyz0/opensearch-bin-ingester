// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

/**
 * ⚠️ THE EPOCH IS A COUNTER, NOT A CLOCK (ADR-0002, SlateDB's pattern). Every
 * test here that could have been written against a timestamp is written against
 * a counter instead, because that is the property the whole fencing argument
 * rests on: a wrong clock must cost availability, never correctness.
 *
 * <p>⚠️ Same shape as {@link IndexRegistry}, the other {@code putIfMatch} CAS'd
 * control object: real JSON, hand-written, because an operator can {@code cat}
 * a lease and read who holds it — which is exactly what someone does first when
 * a cluster stops committing.
 */
class LeaseTest {

    @Test
    void aLeaseRoundTripsThroughItsEncoding() throws Exception {
        Lease l = new Lease(7, "pod1", "10.0.0.4:8080", 1_700_000_000_000L);
        assertThat(Lease.decode(l.encode())).isEqualTo(l);
    }

    @Test
    void theEncodingIsReadableJsonAnOperatorCanInterpret() {
        // ⚠️ Not decorative. When a cluster stops committing, the first question
        // is "who holds the lease and until when", and the answer has to be
        // legible without a decoder -- the same reason IndexRegistry is JSON.
        String s = new String(new Lease(7, "pod1", "10.0.0.4:8080", 1_700_000_000_000L).encode(),
                StandardCharsets.UTF_8);
        assertThat(s).startsWith("{").endsWith("}")
                .contains("\"epoch\":7")
                .contains("\"holderPodId\":\"pod1\"")
                .contains("\"holderEndpoint\":\"10.0.0.4:8080\"")
                .contains("\"expiresAtMillis\":1700000000000");
    }

    @Test
    void theEncodingIsExactlyThisByteSequenceIncludingFieldOrder() {
        // ⚠️ THE WHOLE BYTE STRING, not just "two equal leases match" -- for a
        // record with fixed components that is true by construction, so it
        // would be coverage rather than verification (its own red record showed
        // it passing against a stub returning "{}"). Pinning the exact bytes
        // pins FIELD ORDER too, which is the property that actually matters: a
        // CAS'd object whose encoding varies for equal content makes every
        // putIfMatch a coin toss on whether the version moved.
        assertThat(new String(new Lease(3, "podA", "e", 99L).encode(), StandardCharsets.UTF_8))
                .isEqualTo("{\"epoch\":3,\"holderPodId\":\"podA\","
                        + "\"holderEndpoint\":\"e\",\"expiresAtMillis\":99}");
    }

    @Test
    void aNegativeEpochIsRefused() {
        // ⚠️ Epochs only ever increase, from 0. A negative one could only come
        // from corruption or a hand edit, and it would order BEFORE every real
        // epoch -- which is the one thing fencing must never allow.
        assertThatThrownBy(() -> new Lease(-1, "pod1", "e", 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("epoch");
    }

    @Test
    void aBlankHolderPodIdIsRefused() {
        assertThatThrownBy(() -> new Lease(0, "  ", "e", 1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("holderPodId");
        assertThatThrownBy(() -> new Lease(0, null, "e", 1L))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void aNegativeExpiryIsRefused() {
        assertThatThrownBy(() -> new Lease(0, "pod1", "e", -1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("expiresAtMillis");
    }

    @Test
    void expiryIsDecidedByTheSuppliedInstantNeverBySystemTime() {
        // ⚠️ The clock is INJECTED all the way down (non-negotiable 7). A lease
        // that read System.currentTimeMillis() here could not be simulated, and
        // M4.12's whole harness depends on driving expiry deterministically.
        Lease l = new Lease(1, "pod1", "e", 1_000L);
        assertThat(l.isExpiredAt(999L)).isFalse();
        assertThat(l.isExpiredAt(1_000L))
                .as("at exactly the expiry it is expired -- a holder that treats "
                        + "expiry as exclusive keeps sequencing one tick too long")
                .isTrue();
        assertThat(l.isExpiredAt(1_001L)).isTrue();
    }

    @Test
    void renewingKeepsTheEpochAndTheHolderAndMovesOnlyTheExpiry() {
        // ⚠️ RENEWAL MUST NOT BUMP THE EPOCH. The epoch identifies a term of
        // leadership and is in the object path; bumping it on every renew would
        // start a new chain every few seconds and orphan the old one.
        Lease l = new Lease(4, "pod1", "e1", 1_000L);
        Lease renewed = l.renewedUntil(2_000L);
        assertThat(renewed.epoch()).isEqualTo(4);
        assertThat(renewed.holderPodId()).isEqualTo("pod1");
        assertThat(renewed.holderEndpoint()).isEqualTo("e1");
        assertThat(renewed.expiresAtMillis()).isEqualTo(2_000L);
    }

    @Test
    void takingOverBumpsTheEpochByExactlyOneAndReplacesTheHolder() {
        // ⚠️ EXACTLY one. Skipping epochs would leave gaps a reader following
        // the chain cannot distinguish from an epoch it failed to read, and
        // reusing one would let a fenced writer's objects be mistaken for the
        // new term's.
        Lease l = new Lease(4, "pod1", "e1", 1_000L);
        Lease taken = l.takenOverBy("pod2", "e2", 5_000L);
        assertThat(taken.epoch()).isEqualTo(5);
        assertThat(taken.holderPodId()).isEqualTo("pod2");
        assertThat(taken.holderEndpoint()).isEqualTo("e2");
        assertThat(taken.expiresAtMillis()).isEqualTo(5_000L);
    }

    @Test
    void aMalformedOrTruncatedLeaseIsRefused() throws Exception {
        // ⚠️ Loudly, now. A lease a future reader cannot parse is worse than one
        // that fails here: leadership would be undecidable and the cluster would
        // stop committing with no explanation in the object itself.
        byte[] good = new Lease(1, "pod1", "e", 5L).encode();
        assertThatThrownBy(() -> Lease.decode(new byte[0])).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> Lease.decode("{}".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> Lease.decode("not json".getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> Lease.decode(
                java.util.Arrays.copyOf(good, good.length - 3))).isInstanceOf(IOException.class);
        String badNumber = "{\"epoch\":\"notanumber\",\"holderPodId\":\"p\","
                + "\"holderEndpoint\":\"\",\"expiresAtMillis\":1}";
        assertThatThrownBy(() -> Lease.decode(badNumber.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class);
    }

    @Test
    void anEmptyHolderEndpointIsAcceptedBecauseM4HasNoPeerMeshToPublish() throws Exception {
        // ⚠️ The field is in the shape from the first lease so that M5's commit
        // forwarding -- which needs to REACH the holder -- is not a format
        // change. M4 has no peer mesh, so it writes an empty endpoint, and that
        // must be legal rather than an error a future reader trips over.
        // ⚠️ Through DECODE, not an accessor echo. `holderEndpoint()` returning
        // what the constructor was given is true by construction for a record,
        // so an earlier draft of this assertion proved only that the skeleton it
        // was red against was over-strict. A one-character off-by-one in
        // `stringField`'s closing-quote search breaks exactly the empty case and
        // left the whole suite green.
        Lease l = new Lease(0, "pod1", "", 1_000L);
        assertThat(Lease.decode(l.encode())).isEqualTo(l);
        assertThat(Lease.decode(l.encode()).holderEndpoint()).isEmpty();
    }

    @Test
    void aHolderFieldContainingJsonHostileCharactersIsRefused() {
        // ⚠️ THE ENCODER CANNOT ESCAPE, so the constructor must refuse what it
        // cannot represent. Round-1 review measured the alternative: a podId of
        // `pod"1` encodes to invalid JSON and decodes back as `pod` -- silently,
        // no exception. Worse, an endpoint carrying `","expiresAtMillis":0,"x":"`
        // INJECTS A FIELD, and decode returns an already-expired lease. That is
        // a fabricated answer to "who may write the commit chain".
        // ⚠️ Refused HERE and now rather than escaped later: adding escaping
        // once M4.3b and M5 have written leases would be a format change under
        // non-negotiable 8.
        // ⚠️ 0x1F and 0x00 are the BOUNDARY cases: round-4 review measured
        // that `c < 0x20` -> `c < 0x1F` survived, and under it U+001F is
        // written raw into the JSON literal, which a conforming parser then
        // refuses -- destroying the `cat`-legibility this codec exists for.
        for (String hostile : new String[] {
                "pod\"1", "pod\\1", "pod\u0000", "pod\u0001", "pod\u001Fx", "pod\nx"}) {
            assertThatThrownBy(() -> new Lease(0, hostile, "e", 1L))
                    .as("podId %s", hostile)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("holderPodId");
            assertThatThrownBy(() -> new Lease(0, "pod1", hostile, 1L))
                    .as("endpoint %s", hostile)
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("holderEndpoint");
        }
    }

    @Test
    void theFieldInjectionThatWouldFabricateAnExpiredLeaseIsImpossible() {
        // ⚠️ The concrete attack round-1 review demonstrated, kept as its own
        // test because it is the one with a security shape rather than a
        // correctness shape.
        assertThatThrownBy(() -> new Lease(1, "pod1", "h:1\",\"expiresAtMillis\":0,\"x\":\"", 999L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aLeaseTruncatedByASingleByteIsRefused() {
        // ⚠️ ONE byte. The earlier test truncated by three, which happens to
        // land on the numeric path and so never exercised the trailing-brace
        // guard at all -- deleting that guard left the suite green.
        byte[] good = new Lease(1, "pod1", "e", 5L).encode();
        // ⚠️ The MESSAGE, not just the type. Round-2 review measured that
        // deleting either brace guard individually left all 17 green, because
        // the canonical comparison rejects the same inputs for a different
        // reason -- an equivalent mutant. Asserting WHICH guard fired pins the
        // shape check itself rather than its downstream effect.
        assertThatThrownBy(() -> Lease.decode(java.util.Arrays.copyOf(good, good.length - 1)))
                .isInstanceOf(IOException.class).hasMessageContaining("not a lease object");
        byte[] noBrace = new Lease(1, "pod1", "e", 5L).encode();
        noBrace[0] = ' ';
        assertThatThrownBy(() -> Lease.decode(noBrace))
                .isInstanceOf(IOException.class).hasMessageContaining("not a lease object");
    }

    @Test
    void aLeaseMissingAStringFieldIsRefused() {
        // ⚠️ Without this, dropping `stringField`'s missing-field guard makes a
        // lease with no holderPodId decode to `holderPodId=lderEndpoint` -- a
        // FABRICATED holder for the object that decides who may write. `{}`
        // does not reach this path: it exits through the epoch lookup first.
        String noHolder = "{\"epoch\":1,\"holderEndpoint\":\"e\",\"expiresAtMillis\":5}";
        assertThatThrownBy(() -> Lease.decode(noHolder.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class).hasMessageContaining("missing holderPodId");
        String noEndpoint = "{\"epoch\":1,\"holderPodId\":\"p\",\"expiresAtMillis\":5}";
        assertThatThrownBy(() -> Lease.decode(noEndpoint.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class).hasMessageContaining("missing holderEndpoint");
    }

    @Test
    void aStructurallyValidLeaseWithAnIllegalValueFailsAsIoNotIllegalArgument() {
        // ⚠️ `decode` is declared `throws IOException` and a caller
        // (M4.3b's LeaseManager) will catch exactly that. An unchecked
        // IllegalArgumentException escaping from the constructor underneath
        // would bypass every catch on the recovery path.
        String negativeEpoch = "{\"epoch\":-1,\"holderPodId\":\"p\","
                + "\"holderEndpoint\":\"\",\"expiresAtMillis\":5}";
        assertThatThrownBy(() -> Lease.decode(negativeEpoch.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class);
    }

    @Test
    void duplicateUnknownOrReorderedFieldsAreRefused() {
        // ⚠️ `{"epoch":1,"epoch":99,...}` decoded to 1, while jq and every
        // conforming parser say 99 -- so an operator verifying the fencing
        // counter with jq would get a different answer than the code uses.
        // IndexRegistry refuses duplicates for exactly this reason.
        // ⚠️ Unknown fields matter too: the M4 SPEC lists "a pointer carried in
        // the lease object" as a candidate M4.8 may pick, and there is no
        // version field -- so an older reader must refuse what it cannot
        // account for rather than silently discard it.
        String dup = "{\"epoch\":1,\"epoch\":99,\"holderPodId\":\"p\","
                + "\"holderEndpoint\":\"\",\"expiresAtMillis\":5}";
        assertThatThrownBy(() -> Lease.decode(dup.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class);
        String unknown = "{\"epoch\":1,\"holderPodId\":\"p\",\"holderEndpoint\":\"\","
                + "\"expiresAtMillis\":5,\"lastCheckpoint\":7}";
        assertThatThrownBy(() -> Lease.decode(unknown.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class);
        String reordered = "{\"holderPodId\":\"p\",\"epoch\":1,"
                + "\"holderEndpoint\":\"\",\"expiresAtMillis\":5}";
        assertThatThrownBy(() -> Lease.decode(reordered.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class);
    }

    @Test
    void anUnpairedSurrogateIsRefusedThoughAValidAstralPairIsNot() throws Exception {
        // ⚠️ BOTH round-2 reviewers found this independently, and it is the
        // same silent-corruption class as the quote case: an unpaired surrogate
        // is not a quote, not a backslash and not below 0x20, but
        // `getBytes(UTF_8)` replaces it with '?' -- so the round trip is not
        // identity and NOTHING throws. `decode`'s canonical check cannot see it
        // either, because both sides derive from the same lossy conversion.
        assertThatThrownBy(() -> new Lease(1, "pod\uD800x", "e", 5L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("holderPodId");
        assertThatThrownBy(() -> new Lease(1, "pod1", "e\uDC00", 5L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("holderEndpoint");
        // ⚠️ BOTH ENDS of the range, 0xD800 above and 0xDFFF here. Round-4
        // review measured that `cp <= 0xDFFF` -> `cp < 0xDFFF` survived every
        // other test and re-opened the round-2 MAJOR verbatim: `pod\uDFFFx`
        // encoded to `pod?x`, silently, nothing thrown.
        assertThatThrownBy(() -> new Lease(1, "pod\uDFFFx", "e", 5L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("holderPodId");
        // ⚠️ A WELL-FORMED pair must still be legal -- the guard uses the
        // code-point range, not `Character::isSurrogate` on a truncated char,
        // which would have refused every astral character including an emoji.
        Lease emoji = new Lease(1, "pod\uD83D\uDE00x", "e", 5L);
        assertThat(Lease.decode(emoji.encode())).isEqualTo(emoji);
        // ⚠️ U+1DC00 SPECIFICALLY, and it is what makes this test discriminate.
        // Round-3 review measured that the emoji alone does not: `(char)
        // 0x1F600` is U+F600, Private Use, for which `isSurrogate` is false, so
        // the `Character::isSurrogate`-with-a-cast implementation this test
        // exists to rule out passed anyway. U+1DC00's low 16 bits ARE 0xDC00,
        // so the cast misclassifies it and refuses a legal character.
        // ⚠️ U+1DC00, not U+1D800 -- round-4 review caught that round 3's own
        // verdict named the wrong character and this file had copied it. The
        // escape `\uD837\uDC00` was right; the name beside it was not.
        Lease astral = new Lease(1, "pod\uD837\uDC00x", "e", 5L);
        assertThat(Lease.decode(astral.encode())).isEqualTo(astral);
    }

    @Test
    void aLeaseTruncatedInsideAStringValueFailsAsIoNotAnUncheckedException() {
        // ⚠️ The realistic corrupt-object shape, and round-2 review found it
        // reachable: without `stringField`'s unterminated-value guard this
        // throws StringIndexOutOfBounds -- unchecked, so NOT caught by the
        // IllegalArgumentException wrap, and M4.3b's `catch (IOException)`
        // would miss it entirely.
        // ⚠️ Ends with `}` DELIBERATELY. An earlier draft used a value ending
        // in a quote, which `decode`'s shape guard rejects as "not a lease
        // object" before `stringField` is ever reached -- so the test passed
        // while the guard it was written for stayed deletable. Round-3 review
        // measured that: the deletion survived all 21 tests.
        String truncated = "{\"epoch\":1,\"holderPodId\":\"pod1}";
        assertThatThrownBy(() -> Lease.decode(truncated.getBytes(StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class);
    }

    @Test
    void decodingNullFailsAsIoNotAnUncheckedException() {
        // ⚠️ Same contract point: every rejection this method makes must be an
        // IOException, because that is what the recovery path catches.
        assertThatThrownBy(() -> Lease.decode(null)).isInstanceOf(IOException.class);
    }

    @Test
    void surroundingWhitespaceIsToleratedBecauseAHandEditedLeaseCarriesANewline() throws Exception {
        // ⚠️ This is what makes the `cat`/hand-edit story true, and round-2
        // review found nothing pinned it -- removing `strip()` left all tests
        // green. A lease written with `echo` has a trailing newline.
        Lease l = new Lease(2, "pod1", "e", 7L);
        byte[] padded = ("\n  " + new String(l.encode(), StandardCharsets.UTF_8) + "  \n")
                .getBytes(StandardCharsets.UTF_8);
        assertThat(Lease.decode(padded)).isEqualTo(l);
    }

    @Test
    void aLeaseWithAMalformedUtf8ByteIsRefusedRatherThanSilentlyReplaced() throws Exception {
        // ⚠️ The decode-side mirror of the unpaired-surrogate case, and
        // round-3 review measured it live: `new String(bytes, UTF_8)` replaces
        // a malformed byte with U+FFFD, so the canonical comparison re-encoded
        // the ALREADY-LOSSY string and compared it against itself -- a corrupt
        // byte inside a podId was ACCEPTED, yielding a holder nobody is called.
        // The fix is a strict decoder; this pins it.
        byte[] good = new Lease(1, "pod1", "e", 5L).encode();
        String asText = new String(good, StandardCharsets.UTF_8);
        int at = asText.indexOf("pod1") + 3;
        byte[] corrupt = good.clone();
        corrupt[at] = (byte) 0xFF;
        assertThatThrownBy(() -> Lease.decode(corrupt)).isInstanceOf(IOException.class);
    }
}
