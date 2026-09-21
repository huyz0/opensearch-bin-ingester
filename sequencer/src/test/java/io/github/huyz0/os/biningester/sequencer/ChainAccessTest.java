// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;

import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentCommit;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Who can hand a retention pass a chain, and who must not (M8.3).
 *
 * <p>⚠️ **GC IS THE LEASEHOLDER'S WORK** (M7.10's {@code LeasedGc}), so a
 * follower must answer no chain at all. One that produced its neighbour's
 * deltas would hand a pass segments it cannot fence, and a fenced GC that
 * deletes anyway is the silent data-loss path M7.10 measured: "compiles,
 * passes, and deletes unfenced".
 */
class ChainAccessTest {

    private static final String PREFIX = "chain-access/";
    private static final UUID INDEX = UUID.fromString("00000000-0000-0000-0000-0000000000aa");

    private static LeaseConfig config(String podId, String endpoint) {
        return new LeaseConfig(PREFIX, podId, endpoint,
                Duration.ofSeconds(10), Duration.ofSeconds(3));
    }

    private static CommitRequest flush(String segment) {
        return new CommitRequest("poda", "i1", 0, segment,
                Map.of(new RunKey(INDEX, 0), 3));
    }

    @Test
    void aLEADERHandsOutTheCHAINItsOWNLogHolds() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            LeaseManager leases = new LeaseManager(store, config("poda", "pod-a:9000"),
                    Clock.systemUTC());
            InProcessTransport transport = new InProcessTransport();
            FleetSequencer fleet = new FleetSequencer(store, config("poda", "pod-a:9000"),
                    transport, () -> LocalSequencer.start(store, PREFIX, leases, 8));
            try {
                assertThat(fleet.leading()).isTrue();
                fleet.commitAll(List.of(flush("seg-1")));

                Optional<ChainMemory> chain = fleet.chain();

                assertThat(chain).isPresent();
                assertThat(chain.orElseThrow().snapshot().deltas().stream()
                        .flatMap(d -> d.segments().stream())
                        .map(SegmentCommit::segmentKey))
                        .as("⚠️ THE LOG'S OWN CHAIN, not a fresh one: a leader that answered "
                                + "an EMPTY ChainMemory here would make every retention pass "
                                + "condemn nothing, for ever, with no error anywhere")
                        .containsExactly("seg-1");
            } finally {
                fleet.close();
            }
        }
    }

    @Test
    void aBATCHEDTermStillHandsOutItsChain() throws Exception {
        // ⚠️ M8.50: the elected term is wrapped in a BatchingSequencer, so
        // `instanceof LocalSequencer` on it is false. A chain() that did not
        // look through the wrapper answers EMPTY on the leader, and every
        // retention pass then does nothing, for ever, with no error anywhere.
        try (MemoryBinStore store = new MemoryBinStore()) {
            LeaseManager leases = new LeaseManager(store, config("poda", "pod-a:9000"),
                    Clock.systemUTC());
            FleetSequencer fleet = new FleetSequencer(store, config("poda", "pod-a:9000"),
                    new InProcessTransport(), () -> LocalSequencer.start(store, PREFIX, leases, 8)
                            .map(term -> new BatchingSequencer(term, Duration.ofMillis(1))));
            try {
                fleet.commitAll(List.of(flush("seg-1")));

                assertThat(fleet.chain().orElseThrow().snapshot().deltas().stream()
                        .flatMap(d -> d.segments().stream())
                        .map(SegmentCommit::segmentKey))
                        .as("the chain UNDER the batcher").containsExactly("seg-1");
            } finally {
                fleet.close();
            }
        }
    }

    @Test
    void aFOLLOWERHasNOChainAndASKINGDoesNOTTakeATerm() throws Exception {
        try (MemoryBinStore store = new MemoryBinStore()) {
            LeaseManager held = new LeaseManager(store, config("podc", "pod-c:9000"),
                    Clock.systemUTC());
            InProcessTransport transport = new InProcessTransport();
            LocalSequencer holder = LocalSequencer.start(store, PREFIX, held, 8).orElseThrow();
            transport.at("pod-c:9000", holder);
            java.util.concurrent.atomic.AtomicInteger elections =
                    new java.util.concurrent.atomic.AtomicInteger();
            try {
                FleetSequencer follower = new FleetSequencer(store,
                        config("poda", "pod-a:9000"), transport, () -> {
                            elections.incrementAndGet();
                            return Optional.empty();
                        });
                try {
                    assertThat(follower.leading()).isFalse();
                    int atConstruction = elections.get();

                    assertThat(follower.chain())
                            .as("⚠️ EMPTY. GC belongs to the leaseholder; a follower handing "
                                    + "a pass the deltas of a term it does not hold would "
                                    + "condemn segments it cannot fence, which is M7.10's "
                                    + "measured \"compiles, passes, and deletes unfenced\"")
                            .isEmpty();

                    assertThat(elections.get())
                            .as("⚠️ AND ASKING TRIED NO ELECTION. `Leadership.sequencer()` "
                                    + "elects into a vacancy -- acquiring a lease, sealing an "
                                    + "ancestor and replaying a chain -- so a question about "
                                    + "whether this pod has a chain would cost it a TERM, and "
                                    + "the retention loop asks on every tick")
                            .isEqualTo(atConstruction);
                } finally {
                    follower.close();
                }
            } finally {
                holder.close();
            }
        }
    }
}
