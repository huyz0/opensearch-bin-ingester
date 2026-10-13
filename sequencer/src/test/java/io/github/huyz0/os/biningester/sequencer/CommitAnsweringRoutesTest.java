// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.A;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.PREFIX;
import static io.github.huyz0.os.biningester.sequencer.DedupFixtures.RA;
import static io.github.huyz0.os.biningester.sequencer.FastTermStartTest.SELF;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitAnswer;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.FastLeaseFenceHolderTest.Mono;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The commit answer reaches a writer through every sequencer between it and
 * the leader (M13.34k; ADR-0082 §7): the fleet's routing, a forward to the
 * leaseholder, and the leader's batching window -- each passing the leader's
 * redirects through rather than committing the request whole, which a fast
 * stream's run refuses.
 */
@Timeout(30)
class CommitAnsweringRoutesTest {

    private static final RunKey OTHER = new RunKey(new UUID(7, 7), 0);
    private static final CommitRequest BOTH =
            new CommitRequest("podx", "i1", 0, "seg/0", Map.of(RA, 3, OTHER, 2));
    private static final String LEADER = "pod-a:9000";

    private static LeaseConfig config(String podId, String endpoint) {
        return new LeaseConfig(PREFIX, podId, endpoint, Duration.ofSeconds(10),
                Duration.ofSeconds(3));
    }

    /** A fast term with A fast in its catalog. */
    private static FastLeaderTerm fastA() throws Exception {
        MemoryBinStore rosters = new MemoryBinStore();
        FastTermOpening.Opened opened = new FastTermOpening(new FastTermStart(rosters, "p"),
                new EmptyTermCloser(rosters, "p")::close,
                new FastLeaseFence(Duration.ofSeconds(10), new SimulatedClock(1_000_000L),
                        new Mono()), SELF, Map::of)
                .open(1, () -> { }).orElseThrow();
        FastLeaderTerm fast = new FastLeaderTerm(opened,
                new JoinDesk(new RosterJoins(rosters, "p", 1, new Mono(), Duration.ofMillis(250)),
                        nanos -> { }),
                key -> 0L, rosters, "p", new Mono(), Duration.ofMillis(250));
        fast.catalogSays(Map.of(A, 1));
        return fast;
    }

    private static LocalSequencer leader(MemoryBinStore store) throws Exception {
        LocalSequencer leader = LocalSequencer.start(store, PREFIX,
                new LeaseManager(store, config("poda", LEADER), Clock.systemUTC()), 8,
                BoundedRecoveryFixture.noRenew()).orElseThrow();
        leader.attach(fastA());
        return leader;
    }

    @Test
    void aFORWARDINGPodIsAnsweredWithTheLeadersRedirects() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        LocalSequencer leader = leader(store);
        try (RemoteSequencer remote = new RemoteSequencer(store, config("podb", "pod-b:9000"),
                new InProcessTransport().at(LEADER, leader))) {
            CommitAnswer answer = remote.commitAnswering(BOTH);

            assertThat(answer.redirected()).containsExactly(RA);
            assertThat(leader.committedNext(OTHER)).isEqualTo(2);
        } finally {
            leader.close();
        }
    }

    @Test
    void aFLEETThatLeadsAnswersWithItsTermsRedirects() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        FleetSequencer fleet = new FleetSequencer(store, config("poda", LEADER),
                new InProcessTransport(), () -> {
                    try {
                        return java.util.Optional.of(leader(store));
                    } catch (Exception failed) {
                        throw new java.io.UncheckedIOException(new java.io.IOException(failed));
                    }
                });
        try {
            CommitAnswer answer = fleet.commitAnswering(BOTH);

            assertThat(answer.redirected()).containsExactly(RA);
        } finally {
            fleet.close();
        }
    }

    @Test
    void aBATCHEDLeaderAnswersARedirectAloneAndBatchesTheRest() throws Exception {
        LocalSequencer leader = leader(new MemoryBinStore());
        try (BatchingSequencer batched = new BatchingSequencer(leader, Duration.ofMillis(5))) {
            CommitAnswer redirecting = batched.commitAnswering(BOTH);
            CommitAnswer plain = batched.commitAnswering(
                    new CommitRequest("podx", "i1", 1, "seg/1", Map.of(OTHER, 4)));

            assertThat(redirecting.redirected()).containsExactly(RA);
            assertThat(plain.redirected()).isEmpty();
            assertThat(plain.deltas()).singleElement().satisfies(d -> assertThat(d.segments())
                    .singleElement().satisfies(s -> assertThat(s.segmentKey())
                            .isEqualTo("seg/1")));
            assertThat(leader.committedNext(OTHER)).isEqualTo(6);
        } finally {
            leader.close();
        }
    }

    @Test
    void plainANSWERINGRequestsStillShareOneBatchingWindow() throws Exception {
        // ⚠️ its review round 1, T1: the window is the commit rate (non-negotiable 6).
        LocalSequencer leader = leader(new MemoryBinStore());
        try (BatchingSequencer batched = new BatchingSequencer(leader, Duration.ofMillis(300));
                var pool = java.util.concurrent.Executors.newFixedThreadPool(3)) {
            java.util.concurrent.CountDownLatch go = new java.util.concurrent.CountDownLatch(1);
            java.util.List<java.util.concurrent.Future<CommitAnswer>> answers =
                    new java.util.ArrayList<>();
            for (int i = 0; i < 3; i++) {
                CommitRequest plain = new CommitRequest("pod" + i, "i1", 0, "seg/" + i,
                        Map.of(OTHER, 1));
                answers.add(pool.submit(() -> {
                    go.await();
                    return batched.commitAnswering(plain);
                }));
            }
            go.countDown();

            java.util.Set<Long> sequences = new java.util.HashSet<>();
            for (var answer : answers) {
                sequences.add(answer.get().deltas().get(0).sequence());
            }
            assertThat(sequences).as("three requests, one delta").hasSize(1);
        } finally {
            leader.close();
        }
    }
}
