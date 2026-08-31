// SPDX-License-Identifier: Apache-2.0
package binjava.plugin;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.document.LongPoint;
import org.junit.jupiter.api.Test;
import org.opensearch.index.IngestionShardPointer;

/** WARNING: the pointer OpenSearch stores in EVERY document and resumes from. */
class BinStoreOffsetTest {

    @Test
    void aPointerSerialisesAndComesBackTheSame() {
        for (long v : new long[] {0, 1, 42, Long.MAX_VALUE}) {
            BinStoreOffset p = new BinStoreOffset(v);
            assertThat(BinStoreOffset.deserialize(p.serialize()).offset()).isEqualTo(v);
        }
        assertThat(new BinStoreOffset(7).serialize()).hasSize(8);
    }

    @Test
    void theStringFormSortsLikeTheNumericOne() {
        // WARNING: OpenSearch persists asString() as batch_start and compares
        // pointers after a restart. Unpadded, "10" sorts before "9" and a resume
        // silently REWINDS -- replaying everything in between.
        List<String> strings = new ArrayList<>();
        for (long v : new long[] {9, 10, 100, 1_000_000, 0}) {
            strings.add(new BinStoreOffset(v).asString());
        }
        List<String> sorted = new ArrayList<>(strings);
        java.util.Collections.sort(sorted);
        assertThat(sorted).containsExactly(
                new BinStoreOffset(0).asString(),
                new BinStoreOffset(9).asString(),
                new BinStoreOffset(10).asString(),
                new BinStoreOffset(100).asString(),
                new BinStoreOffset(1_000_000).asString());
    }

    @Test
    void aStringRoundTripsBackToTheSameOffset() {
        BinStoreOffset p = new BinStoreOffset(123_456);
        assertThat(BinStoreOffset.fromString(p.asString()).offset()).isEqualTo(123_456);
    }

    @Test
    void theRangeQueryIsStrictlyGreaterThanThePointer() {
        // WARNING: including the start replays the last record of every batch on
        // every resume -- duplicates OpenSearch cannot detect, because they
        // carry the same _id and a later _version.
        var query = new BinStoreOffset(5).newRangeQueryGreaterThan("_offset");
        assertThat(query).isEqualTo(LongPoint.newRangeQuery("_offset", 6, Long.MAX_VALUE));
        assertThat(query).isNotEqualTo(LongPoint.newRangeQuery("_offset", 5, Long.MAX_VALUE));
    }

    @Test
    void thePointFieldCarriesTheOffsetItself() {
        var field = new BinStoreOffset(42).asPointField(IngestionShardPointer.OFFSET_FIELD);
        assertThat(field.name()).isEqualTo("_offset");
        assertThat(field.numericValue()).isEqualTo(42L);
    }

    @Test
    void pointersCompareByOffset() {
        assertThat(new BinStoreOffset(1)).isLessThan(new BinStoreOffset(2));
        assertThat(new BinStoreOffset(2)).isGreaterThan(new BinStoreOffset(1));
        assertThat(new BinStoreOffset(2).compareTo(new BinStoreOffset(2))).isZero();
        assertThat(new BinStoreOffset(2)).isEqualTo(new BinStoreOffset(2));
    }

    @Test
    void aNegativeOffsetOrAForeignPointerIsRefused() {
        assertThatThrownBy(() -> new BinStoreOffset(-1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> BinStoreOffset.deserialize(new byte[4]))
                .isInstanceOf(IllegalArgumentException.class);
        // WARNING: comparing against another plugin's pointer type would
        // silently order two incomparable things.
        IngestionShardPointer foreign = new IngestionShardPointer() {
            @Override public byte[] serialize() { return new byte[0]; }
            @Override public String asString() { return "x"; }
            @Override public org.apache.lucene.document.Field asPointField(String f) { return null; }
            @Override public org.apache.lucene.search.Query newRangeQueryGreaterThan(String f) { return null; }
            @Override public int compareTo(IngestionShardPointer o) { return 0; }
        };
        assertThatThrownBy(() -> new BinStoreOffset(1).compareTo(foreign))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
