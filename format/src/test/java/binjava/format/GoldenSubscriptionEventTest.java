// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The subscription event's bytes, pinned (M5.14).
 *
 * <p>⚠️ WHAT A GOLDEN FILE BUYS THAT A ROUND TRIP DOES NOT: a round trip passes
 * for any self-consistent codec, so renaming a field, reordering two, widening
 * a varint or switching endianness all survive it. Every one of those changes
 * the bytes, and every one silently breaks a peer that is not recompiled with
 * you. These two files are the only thing in the tree that would notice.
 *
 * <p>⚠️ THERE IS NO OLD-SHAPE FILE BESIDE THESE, and that is deliberate rather
 * than an omission. {@code wire-format-change}'s checklist asks for golden files
 * for "the old and the new shape"; the subscription protocol has never been
 * serialized -- {@code SubscriptionTransport} is an in-process seam and the
 * production transport is still unbuilt -- so no bytes in this shape exist
 * anywhere outside this repository, and a v0 file would be a fixture invented to
 * satisfy a checklist. The version byte IS written, so the first real rollout
 * has its discriminator; when a v2 arrives, the file beside this one is what
 * makes the compatibility claim checkable, exactly as
 * {@code chain-delta-v0.bin} does for {@code chain-delta-v1.bin}.
 *
 * <p>⚠️ IF THIS TEST FAILS, THE FORMAT MOVED. That is not a reason to
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

    /**
     * An {@code inline} event encodes to exactly the bytes on disk.
     *
     * <p>⚠️ BOTH DIRECTIONS, because they fail differently. Encoding proves the
     * WRITER has not drifted; decoding the stored bytes proves a READER built
     * today still understands what a writer produced before. A codec that
     * changed both consistently passes only the first.
     */
    @Test
    void anINLINEEventMatchesItsGoldenBytes() throws Exception {
        SubscriptionEvent event = new SubscriptionEvent("sess-abc", 42L, KEY,
                "seg/2026/09/11/xyz", 1_000L, 128, FetchMode.INLINE,
                new byte[] {1, 2, 3, 4, 5});
        byte[] stored = golden("subscription-event-inline-v1.bin");

        assertThat(event.encode()).as("the WRITER has not drifted").isEqualTo(stored);
        assertThat(SubscriptionEvent.decode(stored))
                .as("and a READER built today still parses what was written before")
                .isEqualTo(event);
    }

    /**
     * A {@code direct} event -- no inline bytes -- encodes to exactly its
     * stored bytes.
     *
     * <p>⚠️ THE SECOND FILE IS NOT REDUNDANT. The empty-payload case is where a
     * length prefix and its absence are easiest to confuse, and it is the only
     * fixture that pins the {@code via} field to something other than
     * {@code INLINE} -- a codec writing the enum ORDINAL rather than its name
     * matches the inline file if {@code INLINE} happens to be ordinal 0.
     */
    @Test
    void aDIRECTEventMatchesItsGoldenBytes() throws Exception {
        SubscriptionEvent event = new SubscriptionEvent("sess-abc", 42L, KEY,
                "seg/2026/09/11/xyz", 1_000L, 128, FetchMode.DIRECT, new byte[0]);
        byte[] stored = golden("subscription-event-direct-v1.bin");

        assertThat(event.encode()).isEqualTo(stored);
        assertThat(SubscriptionEvent.decode(stored)).isEqualTo(event);
    }

    /**
     * The stored bytes carry the magic and version the class documents.
     *
     * <p>⚠️ READ FROM THE FILE, not from the constants, so this fails if the
     * constants move away from what is already on disk rather than agreeing
     * with themselves.
     */
    @Test
    void theStoredBytesCarryTheDOCUMENTEDMagicAndVersion() throws Exception {
        byte[] stored = golden("subscription-event-inline-v1.bin");
        var b = java.nio.ByteBuffer.wrap(stored).order(java.nio.ByteOrder.BIG_ENDIAN);
        assertThat(b.getInt(0)).isEqualTo(SubscriptionEvent.MAGIC);
        assertThat(b.getInt(4)).isEqualTo(SubscriptionEvent.VERSION_1);
        // ⚠️ THE CONSTANT, NOT A LITERAL. A first draft of this line asserted
        // against 0x424A4348, a number I invented -- ChainEntry.MAGIC is
        // 0x42444C54, so the assertion passed without comparing the two things
        // it named. Referencing the constant is what makes a future collision
        // fail here.
        assertThat(SubscriptionEvent.MAGIC)
                .as("and it is NOT ChainEntry's -- a different protocol, a different namespace")
                .isNotEqualTo(ChainEntry.MAGIC);
    }
}
