// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The subscription event's bytes, pinned at BOTH shapes (M5.14, M5.15a).
 *
 * <p>⚠️ WHAT A GOLDEN FILE BUYS THAT A ROUND TRIP DOES NOT: a round trip passes
 * for any self-consistent codec, so renaming a field, reordering two, widening
 * a varint or switching endianness all survive it. Every one of those changes
 * the bytes, and every one silently breaks a peer that is not recompiled with
 * you. These four files are the only thing in the tree that would notice.
 *
 * <p>⚠️ THE OLD-SHAPE FILES NOW HAVE A SUBJECT, and M5.14's javadoc said they
 * did not. That was true then: nothing had ever been serialized, so
 * {@code wire-format-change}'s "golden files for the old AND the new shape" and
 * "ship the read side first" had nothing to be about, and the exemption was
 * recorded rather than the item skipped. M5.15a added a field, so v1 is now a
 * real old shape with real stored bytes -- and the version byte M5.14 wrote
 * against exactly this day is what makes reading them possible.
 *
 * <p>⚠️ BOTH VERSIONS ARE NOW ASSERTED IN BOTH DIRECTIONS, and an earlier
 * draft of this paragraph said v1 was decode-only because "nothing encodes v1
 * any more". That stopped being true when round-1 review made the ABSENT
 * sentinel out-of-band: the version follows the FIELD, so an event with no
 * session epoch IS a v1 event and re-encodes to v1 bytes. The compatibility
 * claim got stronger as a side effect of closing a different defect.
 *
 * <p>⚠️ IF ONE OF THESE FAILS, THE FORMAT MOVED. That is not a reason to
 * regenerate the file: it is the question of whether every reader and writer
 * moved with it, in this commit, which is the rule the skill exists to enforce.
 */
class GoldenSubscriptionEventTest {

    private static final UUID INDEX = UUID.fromString("0b1e5f2a-1111-4222-8333-444455556666");
    private static final RunKey KEY = new RunKey(INDEX, 7);

    private static byte[] golden(String name) throws IOException {
        try (var in = GoldenSubscriptionEventTest.class.getResourceAsStream("/golden/" + name)) {
            assertThat(in).as("missing golden file %s", name).isNotNull();
            return in.readAllBytes();
        }
    }

    private static SubscriptionEvent v2Inline() {
        return new SubscriptionEvent("sess-abc", 42L, 3L, KEY, "seg/2026/09/11/xyz",
                1_000L, 128, FetchMode.INLINE, new byte[] {1, 2, 3, 4, 5});
    }

    private static SubscriptionEvent v2Direct() {
        return new SubscriptionEvent("sess-abc", 42L, 3L, KEY, "seg/2026/09/11/xyz",
                1_000L, 128, FetchMode.DIRECT, new byte[0]);
    }

    private static SubscriptionEvent v3DirectWithRange() {
        return new SubscriptionEvent("sess-abc", 42L, 3L, KEY, "seg/2026/09/11/xyz",
                1_000L, 128, FetchMode.DIRECT, new byte[0],
                new Grant("https://store.example/seg/2026/09/11/xyz?sig=abc",
                        java.time.Instant.ofEpochMilli(1_757_764_800_000L)),
                4_096L, 65_536L);
    }

    private static SubscriptionEvent v3DirectWholeObject() {
        return new SubscriptionEvent("sess-abc", 42L, 3L, KEY, "seg/2026/09/11/xyz",
                1_000L, 128, FetchMode.DIRECT, new byte[0],
                new Grant("https://store.example/seg/2026/09/11/xyz?sig=abc",
                        java.time.Instant.ofEpochMilli(1_757_764_800_000L)),
                SubscriptionEvent.RANGE_ABSENT, SubscriptionEvent.RANGE_ABSENT);
    }

    /**
     * A v3 {@code direct} event carrying a grant AND its byte range.
     *
     * <p>⚠️ THE RANGE IS THE HALF security.md RULE 3 ASKED FOR: a grant "scoped
     * to one key and where possible one range". ADR-0042 dropped the
     * coordinates and named M5.44 as the row that decides whether closing that
     * means bringing them back; this file is that decision in bytes.
     */
    @Test
    void aV3DIRECTEventWithARangeMatchesItsGoldenBytes() throws Exception {
        byte[] stored = golden("subscription-event-direct-v3.bin");
        assertThat(v3DirectWithRange().encode()).as("the WRITER has not drifted")
                .isEqualTo(stored);
        assertThat(SubscriptionEvent.decode(stored))
                .as("and a READER built today parses the grant and the range back")
                .isEqualTo(v3DirectWithRange());
    }

    /**
     * A v3 event whose grant covers the WHOLE object still encodes, and its
     * range reads back ABSENT rather than as byte zero.
     *
     * <p>⚠️ THE PRESENCE BYTE IS WHAT MAKES THIS DISTINGUISHABLE.
     * {@code byteStart = 0} is the first byte of a real segment, so a sentinel
     * varint would have collided with a legitimate range starting at zero.
     */
    @Test
    void aV3GrantWithNORangeReadsBackABSENTNotZERO() throws Exception {
        byte[] stored = golden("subscription-event-direct-v3-whole.bin");
        assertThat(v3DirectWholeObject().encode()).isEqualTo(stored);
        SubscriptionEvent back = SubscriptionEvent.decode(stored);
        assertThat(back).isEqualTo(v3DirectWholeObject());
        assertThat(back.byteStart())
                .as("absent, and NOT the first byte of the object")
                .isEqualTo(SubscriptionEvent.RANGE_ABSENT);
        assertThat(back.grant()).isNotNull();
    }

    /**
     * The grant and the range are part of the VALUE, not decoration.
     *
     * <p>⚠️ {@code equals} HERE IS HAND-WRITTEN AND ENUMERATES COMPONENTS, so a
     * field added to the record and not to that list is invisible to every
     * {@code isEqualTo} above -- including the golden assertions. A decoder
     * that dropped the grant entirely would have matched its golden file.
     */
    @Test
    void aDifferingGRANTOrRANGEMakesTwoEventsUNEQUAL() {
        assertThat(v3DirectWithRange()).isNotEqualTo(v3DirectWholeObject());
        assertThat(v3DirectWithRange()).isNotEqualTo(v2Direct());
        assertThat(v3DirectWithRange().hashCode())
                .isNotEqualTo(v3DirectWholeObject().hashCode());

        // ⚠️ DIFFERING ONLY IN THE GRANT, AND THE THREE ABOVE DO NOT.
        // `v3DirectWithRange` and `v3DirectWholeObject` differ in the RANGE
        // too, and so does `v2Direct` -- so `byteStart == that.byteStart`
        // satisfies every one of them on its own and the grant component was
        // unconstrained. Review measured `Objects.equals(grant, that.grant)`
        // relaxed to `true` surviving the whole suite, and worse: that
        // relaxation PLUS a decode building the Grant with a constant url left
        // both golden cases green as well.
        SubscriptionEvent otherUrl = new SubscriptionEvent("sess-abc", 42L, 3L, KEY,
                "seg/2026/09/11/xyz", 1_000L, 128, FetchMode.DIRECT, new byte[0],
                new Grant("https://store.example/DIFFERENT?sig=abc",
                        java.time.Instant.ofEpochMilli(1_757_764_800_000L)),
                4_096L, 65_536L);
        SubscriptionEvent otherExpiry = new SubscriptionEvent("sess-abc", 42L, 3L, KEY,
                "seg/2026/09/11/xyz", 1_000L, 128, FetchMode.DIRECT, new byte[0],
                new Grant("https://store.example/seg/2026/09/11/xyz?sig=abc",
                        java.time.Instant.ofEpochMilli(1_757_764_800_001L)),
                4_096L, 65_536L);

        assertThat(v3DirectWithRange())
                .as("a different URL is a different event, whatever the range says")
                .isNotEqualTo(otherUrl);
        assertThat(v3DirectWithRange())
                .as("and so is a different expiry")
                .isNotEqualTo(otherExpiry);

        // ⚠️ hashCode TOO, AND THE EQUALS HALF ALONE LEFT IT FREE. Review
        // measured `grant` dropped from the `Objects.hash(...)` list surviving:
        // the hashCode comparison above is between a pair that ALSO differs in
        // the range, so the grant rode free there exactly as it did in equals.
        assertThat(v3DirectWithRange().hashCode())
                .as("a different grant is a different hash, with the range held equal")
                .isNotEqualTo(otherUrl.hashCode());

        // ⚠️ AND EACH RANGE COMPONENT SEPARATELY. Dropping `byteStart` or
        // `byteLen` from equals survived independently, because no pair in the
        // suite differed in exactly one of them.
        SubscriptionEvent otherStart = new SubscriptionEvent("sess-abc", 42L, 3L, KEY,
                "seg/2026/09/11/xyz", 1_000L, 128, FetchMode.DIRECT, new byte[0],
                new Grant("https://store.example/seg/2026/09/11/xyz?sig=abc",
                        java.time.Instant.ofEpochMilli(1_757_764_800_000L)),
                8_192L, 65_536L);
        SubscriptionEvent otherLen = new SubscriptionEvent("sess-abc", 42L, 3L, KEY,
                "seg/2026/09/11/xyz", 1_000L, 128, FetchMode.DIRECT, new byte[0],
                new Grant("https://store.example/seg/2026/09/11/xyz?sig=abc",
                        java.time.Instant.ofEpochMilli(1_757_764_800_000L)),
                4_096L, 32_768L);

        assertThat(v3DirectWithRange())
                .as("a different start alone is a different event")
                .isNotEqualTo(otherStart);
        assertThat(v3DirectWithRange())
                .as("and a different length alone is too")
                .isNotEqualTo(otherLen);
    }

    /**
     * A v2 {@code inline} event encodes to exactly the bytes on disk.
     *
     * <p>⚠️ BOTH DIRECTIONS, because they fail differently. Encoding proves the
     * WRITER has not drifted; decoding the stored bytes proves a READER built
     * today still understands what a writer produced before.
     */
    @Test
    void aV2INLINEEventMatchesItsGoldenBytes() throws Exception {
        byte[] stored = golden("subscription-event-inline-v2.bin");
        assertThat(v2Inline().encode()).as("the WRITER has not drifted").isEqualTo(stored);
        assertThat(SubscriptionEvent.decode(stored))
                .as("and a READER built today still parses what was written before")
                .isEqualTo(v2Inline());
    }

    /**
     * A v2 {@code direct} event -- no inline bytes -- matches its stored bytes.
     *
     * <p>⚠️ THE SECOND FILE PER VERSION IS NOT REDUNDANT: the empty-payload case
     * is where a length prefix and its absence are easiest to confuse, and it is
     * the only fixture pinning {@code via} to something other than
     * {@code INLINE}.
     */
    @Test
    void aV2DIRECTEventMatchesItsGoldenBytes() throws Exception {
        byte[] stored = golden("subscription-event-direct-v2.bin");
        assertThat(v2Direct().encode()).isEqualTo(stored);
        assertThat(SubscriptionEvent.decode(stored)).isEqualTo(v2Direct());
    }

    /**
     * A v1 event written before {@code sessionEpoch} existed still decodes, and
     * its missing field reads as ABSENT rather than as zero-the-number.
     *
     * <p>⚠️ THIS IS THE COMPATIBILITY CLAIM, and it is the only test in the tree
     * that makes it. Everything else about v1 could be deleted and the suite
     * would stay green while a v1 peer's events became unreadable.
     *
     * <p>⚠️ ABSENT IS NOT ZERO. Session epochs number from 1, so
     * {@code SESSION_EPOCH_ABSENT} cannot collide with a live one -- if it
     * could, a v1 event would be indistinguishable from a session at its very
     * first request, and a consumer resuming on it would claim a position it
     * was never given.
     */
    @Test
    void aV1EventStillDECODESAndItsSessionEpochIsABSENT() throws Exception {
        // ⚠️ THE WHOLE RECORD, not a handful of components. Round-1 review
        // measured five of eight asserted, so `via`, `inline` and `segmentKey`
        // were unchecked -- which left the `direct` fixture in the loop only
        // for the fields it shares with the `inline` one, i.e. contributing
        // nothing. An `isEqualTo` against a constructed expectation is both
        // stronger and shorter.
        assertThat(SubscriptionEvent.decode(golden("subscription-event-inline-v1.bin")))
                .as("a v1 inline event, whole")
                .isEqualTo(new SubscriptionEvent("sess-abc", 42L,
                        SubscriptionEvent.SESSION_EPOCH_ABSENT, KEY, "seg/2026/09/11/xyz",
                        1_000L, 128, FetchMode.INLINE, new byte[] {1, 2, 3, 4, 5}));
        assertThat(SubscriptionEvent.decode(golden("subscription-event-direct-v1.bin")))
                .as("and a v1 direct event, whole")
                .isEqualTo(new SubscriptionEvent("sess-abc", 42L,
                        SubscriptionEvent.SESSION_EPOCH_ABSENT, KEY, "seg/2026/09/11/xyz",
                        1_000L, 128, FetchMode.DIRECT, new byte[0]));

        // ⚠️ AND THEY RE-ENCODE TO THEIR OWN BYTES. The version now follows the
        // FIELD, so an absent session epoch means a v1 body -- which turns
        // decode-only compatibility into a genuine round trip for stored bytes.
        for (String name : new String[] {"subscription-event-inline-v1.bin",
                                         "subscription-event-direct-v1.bin"}) {
            assertThat(SubscriptionEvent.decode(golden(name)).encode())
                    .as("%s re-encodes byte-identically", name)
                    .isEqualTo(golden(name));
        }
    }

    /**
     * The two versions differ by exactly the appended field.
     *
     * <p>⚠️ A PREFIX COMPARISON, which is what makes "v2 is v1 plus one field"
     * checkable rather than a sentence in an ADR. If a later edit inserts the
     * session epoch mid-body instead, or reorders anything, this fails even
     * though both files would still round-trip against their own version.
     */
    @Test
    void aV2BodyIsAV1BodyPlusTheAppendedFIELD() throws Exception {
        byte[] v1 = golden("subscription-event-inline-v1.bin");
        byte[] v2 = golden("subscription-event-inline-v2.bin");

        assertThat(v2.length).as("one uvarint longer").isEqualTo(v1.length + 1);
        // ⚠️ FROM BYTE 8, skipping the header, because the VERSION differs by
        // design -- that is the discriminator doing its job, not drift.
        assertThat(java.util.Arrays.copyOfRange(v2, 8, v1.length))
                .as("every v1 field, in the same order, at the same offset")
                .isEqualTo(java.util.Arrays.copyOfRange(v1, 8, v1.length));
    }

    /**
     * The stored bytes carry the magic and versions the class documents.
     *
     * <p>⚠️ READ FROM THE FILES, not from the constants, so this fails if the
     * constants move away from what is already on disk rather than agreeing
     * with themselves.
     */
    @Test
    void theStoredBytesCarryTheDOCUMENTEDMagicAndVersions() throws Exception {
        var v1 = java.nio.ByteBuffer.wrap(golden("subscription-event-inline-v1.bin"))
                .order(java.nio.ByteOrder.BIG_ENDIAN);
        var v2 = java.nio.ByteBuffer.wrap(golden("subscription-event-inline-v2.bin"))
                .order(java.nio.ByteOrder.BIG_ENDIAN);

        assertThat(v1.getInt(0)).isEqualTo(SubscriptionEvent.MAGIC);
        assertThat(v2.getInt(0)).as("the magic does not move between versions")
                .isEqualTo(SubscriptionEvent.MAGIC);
        assertThat(v1.getInt(4)).isEqualTo(SubscriptionEvent.VERSION_1);
        assertThat(v2.getInt(4)).isEqualTo(SubscriptionEvent.VERSION_2);
        // ⚠️ THE CONSTANT, NOT A LITERAL. A first draft of this line asserted
        // against 0x424A4348, a number I invented -- ChainEntry.MAGIC is
        // 0x42444C54, so the assertion passed without comparing the two things
        // it named.
        assertThat(SubscriptionEvent.MAGIC)
                .as("and it is NOT ChainEntry's -- a different protocol, a different namespace")
                .isNotEqualTo(ChainEntry.MAGIC);
    }
}
