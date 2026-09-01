// SPDX-License-Identifier: Apache-2.0
package binjava.format;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The index-ordinal registry's content: pure encode/decode (M2.3). */
class IndexRegistryTest {

    private static final String A = "00000000-0000-0000-0000-0000000000aa";
    private static final String B = "00000000-0000-0000-0000-0000000000bb";

    @Test
    void encodeThenDecodeReproducesTheSameOrdinals() throws IOException {
        IndexRegistry r = new IndexRegistry(Map.of(A, 0, B, 1));
        IndexRegistry round = IndexRegistry.decode(r.encode());
        assertThat(round.ordinals()).isEqualTo(r.ordinals());
    }

    @Test
    void anEmptyRegistryEncodesToAnEmptyObject() throws IOException {
        assertThat(new String(IndexRegistry.EMPTY.encode(), java.nio.charset.StandardCharsets.UTF_8))
                .isEqualTo("{}");
        assertThat(IndexRegistry.decode("{}".getBytes(java.nio.charset.StandardCharsets.UTF_8)).ordinals())
                .isEmpty();
    }

    @Test
    void encodingIsDeterministicRegardlessOfInsertionOrder() {
        // ⚠️ round-1 test-review (M2.3): A's ordinal is numerically LARGER
        // than B's while A still sorts BEFORE B lexically. With the original
        // A->0, B->1 fixture, key order and ordinal order agreed, so a
        // production bug that sorted by ORDINAL instead of by KEY produced
        // byte-identical output and every assertion here still passed.
        java.util.Map<String, Integer> abOrder = new java.util.LinkedHashMap<>();
        abOrder.put(A, 5);
        abOrder.put(B, 2);
        java.util.Map<String, Integer> baOrder = new java.util.LinkedHashMap<>();
        baOrder.put(B, 2);
        baOrder.put(A, 5);
        // ⚠️ Same CONTENT, opposite insertion order -- must produce identical
        // bytes, or two readers of the same conceptual registry would compute
        // different filter payloads from what they consider "the same" input.
        assertThat(new IndexRegistry(abOrder).encode()).isEqualTo(new IndexRegistry(baOrder).encode());
        // ⚠️ AND pinned to the exact expected (key-sorted) byte sequence --
        // the equality check alone does not say WHICH order won, so a
        // value-sorted implementation (which would put B before A here,
        // since 2 < 5) is still distinguishable from the correct key-sorted
        // one (A before B, since "00...aa" < "00...bb").
        assertThat(new String(new IndexRegistry(abOrder).encode(), java.nio.charset.StandardCharsets.UTF_8))
                .isEqualTo("{\"" + A + "\":5,\"" + B + "\":2}");
    }

    @Test
    void ordinalForIsEmptyWhenTheIndexIsNotRegistered() {
        assertThat(IndexRegistry.EMPTY.ordinalFor(A)).isEmpty();
        assertThat(new IndexRegistry(Map.of(A, 0)).ordinalFor(B)).isEmpty();
        assertThat(new IndexRegistry(Map.of(A, 0)).ordinalFor(A)).hasValue(0);
    }

    @Test
    void withAddsOrReplacesOneEntryLeavingOthersUntouched() {
        IndexRegistry r = new IndexRegistry(Map.of(A, 0)).with(B, 1);
        assertThat(r.ordinalFor(A)).hasValue(0);
        assertThat(r.ordinalFor(B)).hasValue(1);
        // ⚠️ REPLACES, not adds a second entry for the same key.
        IndexRegistry replaced = r.with(A, 99);
        assertThat(replaced.ordinalFor(A)).hasValue(99);
        assertThat(replaced.ordinals()).hasSize(2);
    }

    @Test
    void nextOrdinalIsOnePastTheHighestAssignedOrZeroWhenEmpty() {
        assertThat(IndexRegistry.EMPTY.nextOrdinal()).isZero();
        assertThat(new IndexRegistry(Map.of(A, 0)).nextOrdinal()).isEqualTo(1);
        assertThat(new IndexRegistry(Map.of(A, 5, B, 2)).nextOrdinal()).isEqualTo(6);
    }

    @Test
    void aNegativeOrdinalIsRefused() {
        assertThatThrownBy(() -> new IndexRegistry(Map.of(A, -1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anEmptyIndexUUIDIsRefused() {
        assertThatThrownBy(() -> new IndexRegistry(Map.of("", 0)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void decodingRefusesContentThatIsNotAJsonObject() {
        assertThatThrownBy(() -> IndexRegistry.decode("[]".getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> IndexRegistry.decode("not json at all"
                .getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class);
    }

    @Test
    void decodingRefusesAMalformedEntry() {
        assertThatThrownBy(() -> IndexRegistry.decode("{\"no-colon-here\"}"
                .getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class);
        assertThatThrownBy(() -> IndexRegistry.decode(("{\"" + A + "\":not-a-number}")
                .getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class);
    }

    @Test
    void decodingRefusesADuplicateKey() {
        // ⚠️ Two ordinals for one index is not "last one wins" -- it is
        // corruption. A filter computed against whichever parsed last would
        // silently disagree with one computed by a reader that kept the first.
        assertThatThrownBy(() -> IndexRegistry.decode(
                ("{\"" + A + "\":0,\"" + A + "\":1}").getBytes(java.nio.charset.StandardCharsets.UTF_8)))
                .isInstanceOf(IOException.class);
    }
}
