// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import binjava.binstore.Body;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The chain-building fixtures the invariant-checker tests share.
 *
 * <p>⚠️ SHARED RATHER THAN COPIED, for the reason M4.12's split established: the
 * two test classes exist to be read against each other -- one asserts what holds
 * WITHIN a chain, the other what holds ACROSS the join between two -- so a
 * {@code delta} that drifted in one would make the two halves disagree about what
 * a chain even looks like, with both suites green.
 */
final class InvariantFixtures {

    private InvariantFixtures() {
    }

    static final UUID A = UUID.fromString("00000000-0000-0000-0000-0000000000aa");
    static final String PREFIX = "bins/cluster-a";

    /**
     * ⚠️ A STOPPED CLOCK, not {@code systemUTC()}. No fixture here depends on
     * expiry, so a wall clock would only decide -- by whether the machine stalled
     * past the TTL -- WHICH failover branch a test exercised, while passing either
     * way.
     */
    static final java.time.Clock FIXED_CLOCK =
            java.time.Clock.fixed(java.time.Instant.parse("2026-09-04T00:00:00Z"),
                    java.time.ZoneOffset.UTC);

    static Map<RunKey, Integer> counts(int n) {
        Map<RunKey, Integer> m = new LinkedHashMap<>();
        m.put(new RunKey(A, 0), n);
        return m;
    }

    /** Writes {@code bytes} at {@code seq} in {@code log}'s chain, by key. */
    static void put(MemoryBinStore store, CommitLog log, long seq, byte[] bytes)
            throws IOException {
        store.putIfAbsent(log.keyFor(seq),
                new Body(bytes.length, () -> new ByteArrayInputStream(bytes)));
    }

    /** A delta at {@code seq} assigning {@code records} offsets from {@code firstOffset}. */
    static byte[] delta(long seq, int records, long firstOffset) {
        return new CommitDelta(seq, "seg/" + seq,
                List.of(new RunCommit(new RunKey(A, 0), records, firstOffset))).encode();
    }

    static LocalSequencer start(MemoryBinStore store, String pod) throws IOException {
        LeaseManager leases = new LeaseManager(store,
                new LeaseConfig(PREFIX, pod, "", java.time.Duration.ofSeconds(10),
                        java.time.Duration.ofSeconds(3)),
                FIXED_CLOCK);
        return LocalSequencer.start(store, PREFIX, leases, 8).orElseThrow();
    }
}
