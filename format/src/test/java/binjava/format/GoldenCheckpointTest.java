// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.format.Checkpoint.StreamOffsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Checkpoints written by an earlier build must still parse (M4.8a).
 *
 * <p>⚠️ Do NOT regenerate this file to make the test pass — the discipline
 * {@link GoldenChainEntryTest} states in its own words. It was generated once
 * from the encoder, verified BY HAND against the layout, and committed. The
 * header really is {@code 42 43 4b 50} — the ASCII "BCKP" — then a big-endian
 * version 0; {@code 09} really is the sequence; the six streams really do carry
 * {@code e9 07} = 1001 and {@code f6 2e} = 6006 as uvarints; and the tail
 * {@code 04 70 6f 64 61 2a} really is "poda" with a {@code lastAppliedFlushSeq}
 * of 42. If this test fails, either the format changed — non-negotiable 8
 * applies, another version and another golden file — or a reader broke.
 *
 * <p>⚠️ THIS FILE DOES NOT PROVE THE ENCODER'S SORT, and an earlier draft of
 * this javadoc said it did. Measured: both delete-the-sort mutations leave all
 * three tests here GREEN, because nothing in this class builds a fixture -- it
 * reads committed bytes. `CheckpointTest`'s order tests are what constrain the
 * encoder, and deleting them as redundant would leave that hazard undetected.
 *
 * <p>⚠️ WHAT IT DOES PIN, which the same measurement found, is the record's
 * order-preserving copy: substituting `Map.copyOf` for the
 * `LinkedHashMap` copy fails {@link #theGoldenBytesCarryStreamsAndPodsInASCENDINGOrder}
 * in 6 of 6 fresh runs. Reading committed bytes makes it the more robust of the
 * two order detectors, not the weaker one.
 */
class GoldenCheckpointTest {

    private static byte[] golden() throws Exception {
        try (var in = GoldenCheckpointTest.class.getResourceAsStream("/golden/checkpoint-v0.bin")) {
            assertThat(in).as("golden/checkpoint-v0.bin must be on the test classpath").isNotNull();
            return in.readAllBytes();
        }
    }

    @Test
    void aV0CheckpointFromAnEarlierBuildStillParses() throws Exception {
        Checkpoint c = Checkpoint.decode(golden());

        // ⚠️ VALUES FIRST, hand-verified, before any byte comparison. A
        // re-encode check alone is satisfied by a codec that is
        // self-consistently wrong -- which is exactly what a consistent swap of
        // `nextOffset` and `oldestRetainedOffset` would be.
        assertThat(c.sequence()).isEqualTo(9);
        assertThat(c.streams()).hasSize(6);
        assertThat(c.pods()).containsExactly(
                java.util.Map.entry("poda", Checkpoint.PodState.bare(42L)),
                java.util.Map.entry("podb", Checkpoint.PodState.bare(77L)));

        RunKey first = new RunKey(new UUID(0x1111_2222_3333_4444L, 1), 1);
        assertThat(c.streams().get(first))
                .as("the first stream's own two offsets, not each other's")
                .isEqualTo(new StreamOffsets(1001, 100));
        RunKey sixth = new RunKey(new UUID(0x1111_2222_3333_4444L, 6), 6);
        assertThat(c.streams().get(sixth)).isEqualTo(new StreamOffsets(6006, 600));
    }

    @Test
    void theGoldenBytesCarryStreamsAndPodsInASCENDINGOrder() throws Exception {
        Checkpoint c = Checkpoint.decode(golden());

        // ⚠️ Decode preserves the encoded order, so reading the keys back in
        // order is reading the file's order.
        List<RunKey> keys = new ArrayList<>(c.streams().keySet());
        assertThat(keys).as("the file carries streams sorted, not as they were inserted")
                .isSorted();
        assertThat(new ArrayList<>(c.pods().keySet())).containsExactly("poda", "podb");
    }

    @Test
    void reEncodingTheGoldenCheckpointReproducesItByteForByte() throws Exception {
        byte[] bytes = golden();

        assertThat(Checkpoint.decode(bytes).encode())
                .as("a decode/encode round trip is byte-identical to what shipped")
                .isEqualTo(bytes);
    }
}
