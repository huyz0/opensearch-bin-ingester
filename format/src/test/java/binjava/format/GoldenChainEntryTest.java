// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Chain entries written by an earlier build must still parse (M4.5, ADR-0028).
 *
 * <p>⚠️ THREE FILES, ONE PER SHAPE, and the v0 delta is the one that matters
 * most: every delta already in a bucket is v0 and outlives this change by the
 * whole retention window. The {@code wire-format-change} checklist wants golden
 * files for the old shape AND the new, not the old one rewritten to look new.
 *
 * <p>⚠️ Do NOT regenerate these to make the test pass — the same discipline
 * {@code GoldenSegmentTest} and {@code GoldenSegmentV1Test} carry. They were
 * generated once from the encoders, verified BY HAND against the layout
 * (magic, version, kind, then uvarint fields — the {@code d2 09} in the
 * continue file really is 1234, and the seal's trailing {@code 08} really is
 * its {@code continuedAt}), and committed. If this test fails, either the
 * format changed again — non-negotiable 8 applies, another version and another
 * golden file — or a reader broke.
 */
class GoldenChainEntryTest {

    private static byte[] golden(String name) throws Exception {
        try (var in = GoldenChainEntryTest.class.getResourceAsStream("/golden/" + name)) {
            assertThat(in).as("golden/%s must be on the test classpath", name).isNotNull();
            return in.readAllBytes();
        }
    }

    @Test
    void aV0DeltaFromAnEarlierBuildStillParses() throws Exception {
        byte[] bytes = golden("chain-delta-v0.bin");
        ChainEntry entry = ChainEntry.decode(bytes);

        assertThat(entry).isInstanceOf(CommitDelta.class);
        CommitDelta delta = (CommitDelta) entry;
        assertThat(delta.sequence()).isEqualTo(5);
        assertThat(delta.segmentKey()).isEqualTo("bins/seg/000000000000002a.seg");
        assertThat(delta.runs()).containsExactly(
                new RunCommit(new RunKey(
                        UUID.fromString("00000000-0000-0000-0000-0000000000aa"), 3), 2, 10),
                new RunCommit(new RunKey(
                        UUID.fromString("00000000-0000-0000-0000-0000000000bb"), 0), 7, 0));

        assertThat(delta.encode())
                .as("and re-encoding is byte-identical -- a v0 delta is still written as v0")
                .isEqualTo(bytes);
    }

    @Test
    void aSealFromAnEarlierBuildStillParses() throws Exception {
        byte[] bytes = golden("chain-seal-v1.bin");
        assertThat(ChainEntry.decode(bytes)).isEqualTo(new Seal(42, 8));
        assertThat(new Seal(42, 8).encode()).isEqualTo(bytes);
        assertThat(((Seal) ChainEntry.decode(bytes)).continuedAt())
                .as("the forward link across the epoch boundary survives a round trip")
                .isEqualTo(8);
    }

    @Test
    void aContinueFromAnEarlierBuildStillParses() throws Exception {
        byte[] bytes = golden("chain-continue-v1.bin");
        assertThat(ChainEntry.decode(bytes)).isEqualTo(new Continue(0, 7, 1234));
        assertThat(new Continue(0, 7, 1234).encode()).isEqualTo(bytes);
    }

    @Test
    void theThreeShapesAreDistinguishableFromTheirFirstNineBytes() throws Exception {
        // ⚠️ The discriminator has to be in the bytes, not in the key or in
        // what the caller expected: a reader finds these by listing a chain and
        // must tell them apart with nothing but the object.
        // ⚠️ FRESHLY ENCODED, not read from the golden files. Comparing two
        // committed files to each other asserts a property of the fixtures and
        // nothing about the code -- no production mutation could fail it, which
        // makes it coverage rather than verification (testing.md 8-10). These
        // come from the encoders, so colliding two kinds fails here.
        byte[] delta = new CommitDelta(1, "s", List.of(new RunCommit(
                new RunKey(UUID.fromString("00000000-0000-0000-0000-0000000000aa"), 0), 1, 0)))
                .encode();
        byte[] seal = new Seal(1, 3).encode();
        byte[] cont = new Continue(0, 1, 2).encode();

        assertThat(List.of(delta[7], seal[7]).stream().distinct().count())
                .as("v0 and v1 differ in the version word")
                .isEqualTo(2L);
        assertThat(seal[8])
                .as("and the two v1 shapes differ in the kind byte")
                .isNotEqualTo(cont[8]);
        assertThat(seal[7]).as("both new shapes are v1").isEqualTo(cont[7]);
    }
}
