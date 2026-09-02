// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import binjava.binstore.BinStore;
import binjava.binstore.Body;
import binjava.binstore.backend.LocalFsBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.FilterCandidates;
import binjava.format.MembershipFilter;
import binjava.format.SegmentKey;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * M2 acceptance criterion 10 / test-plan row "key round-trip through both
 * in-tree backends": a key round-trips through {@code MemoryBinStore} and
 * {@code LocalFsBinStore} byte-identically, unescaped -- specifically a key
 * whose FILTER component is base64url, since that alphabet's {@code -}/
 * {@code _} are unusual in some key-naming conventions (the wire format's
 * own hazard, per {@link SegmentKey}'s class javadoc) and are exactly the
 * characters M2.4-M2.8's real filter payloads use.
 *
 * <p>⚠️ Not exercised by {@code BinStoreConformance} (generic keys like
 * {@code "p/a"}) or by {@code plugin}'s own cluster tests ({@code
 * SearchableIT}/{@code ShardFanOutIT} build their `SegmentKey` via the
 * legacy 5-arg constructor, which always carries the literal filter {@code
 * N} -- no base64url payload at all). This is the one place a REAL,
 * non-trivial filter payload's key is proven to survive a real backend.
 */
class SegmentKeyBackendRoundTripTest {

    /**
     * ⚠️ MEASURED, not hand-crafted: this is the exact fixture
     * {@code PathologicalKeySizeTest} already uses for its Bloom case (500
     * distinct indices, sparse across 10,000 registered) -- reused here
     * rather than invented, and confirmed by running it to genuinely
     * contain BOTH {@code -} and {@code _} in its base64url payload, not
     * merely one or neither.
     */
    private static MembershipFilter aRealFilterContainingBothUnusualCharacters() {
        Set<Integer> members = new HashSet<>();
        for (int i = 0; i < 500; i++) {
            members.add(i * 17);
        }
        MembershipFilter filter = FilterCandidates.chooseShortestFitting(members, 10_000, 900);
        String encoded = filter.encode();
        assertThat(encoded).as("must genuinely exercise both unusual base64url characters")
                .contains("-").contains("_");
        return filter;
    }

    @Test
    void aKeyWithARealBase64urlFilterRoundTripsThroughMemoryBinStore() throws Exception {
        assertRoundTrips(new MemoryBinStore());
    }

    @Test
    void aKeyWithARealBase64urlFilterRoundTripsThroughLocalFsBinStore() throws Exception {
        Path dir = Files.createTempDirectory("binstore-localfs-keytest");
        dir.toFile().deleteOnExit();
        assertRoundTrips(new LocalFsBinStore(dir));
    }

    private static void assertRoundTrips(BinStore store) throws Exception {
        MembershipFilter filter = aRealFilterContainingBothUnusualCharacters();
        String key = new SegmentKey("bins/cluster-a", 1_700_000_000_000L, "pod7", 42, 96,
                filter.encode()).key();

        byte[] body = "segment bytes, however many".getBytes(StandardCharsets.UTF_8);
        store.put(key, new Body(body.length, () -> new ByteArrayInputStream(body)));

        // ⚠️ stat, not just get: proves the KEY ITSELF is unescaped -- a
        // backend that silently transformed '-'/'_' on write would make a
        // later stat/list/recovery walk unable to find the object at the
        // key its writer actually recorded.
        assertThat(store.stat(key)).as("findable at the exact key, unescaped").isPresent();
        try (var in = store.get(key)) {
            assertThat(in.readAllBytes()).as("byte-identical").isEqualTo(body);
        }
    }
}
