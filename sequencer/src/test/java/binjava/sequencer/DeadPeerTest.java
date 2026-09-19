// SPDX-License-Identifier: Apache-2.0
package binjava.sequencer;

import static binjava.sequencer.DedupFixtures.PREFIX;
import static binjava.sequencer.DedupFixtures.counts;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.BinStore;
import binjava.binstore.backend.MemoryBinStore;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * What a pod does when a peer says NOTHING (M5.6j, ADR-0039).
 *
 * <p>⚠️ A SEPARATE FILE UNDER code-structure.md rule 1, and NO LINE-COUNT
 * TRIGGER IS CLAIMED -- see M5.54: the gate makes an over-cap version
 * unrepresentable, so committed history can neither confirm nor refute one.
 * The seam is real:
 * every test in that file drives peers that ANSWER, and this one exists because
 * that was the whole of the coverage.
 */
@Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DeadPeerTest {

    private static final String A = "pod-a:9000";
    private static final String B = "pod-b:9000";

    /**
     * ⚠️ A CLOCK THE TEST MOVES, shared by both pods. The file this was split
     * from injects one three methods away, and here it is load-bearing twice
     * over: one case needs the lease STILL UNEXPIRED and the other needs it
     * LAPSED, and a wall clock can give neither on demand.
     */
    private static final class MovableClock extends Clock {
        private volatile long millis = 1_000_000L;

        @Override public long millis() {
            return millis;
        }

        @Override public java.time.Instant instant() {
            return java.time.Instant.ofEpochMilli(millis);
        }

        @Override public java.time.ZoneId getZone() {
            return java.time.ZoneOffset.UTC;
        }

        @Override public Clock withZone(java.time.ZoneId zone) {
            return this;
        }
    }

    private static LeaseConfig config(String podId, String endpoint) {
        return new LeaseConfig(PREFIX, podId, endpoint,
                Duration.ofSeconds(10), Duration.ofSeconds(3));
    }

    private static FleetSequencer pod(BinStore store, String podId, String endpoint,
            InProcessTransport transport, Clock clock) throws Exception {
        LeaseManager manager = new LeaseManager(store, config(podId, endpoint), clock);
        // ⚠️ THE RENEWER IS PARKED, and it has to be here. This test moves an
        // INJECTED clock 11 s to lapse a lease, while the shipping ticker is a
        // real `Thread.sleep(3 s)` -- so a JVM stall between construction and
        // the commit lets the leader renew against the already-advanced clock
        // and reds the guard with no bug present.
        FleetSequencer fleet = new FleetSequencer(store, config(podId, endpoint),
                transport, () -> LocalSequencer.start(store, PREFIX, manager, 8,
                        BoundedRecoveryFixture.noRenew()));
        if (fleet.leading()) {
            transport.at(endpoint, fleet);
        }
        return fleet;
    }

    private static CommitRequest flush(String pod, long seq, String segment) {
        return new CommitRequest(pod, "i1", seq, segment, counts(2));
    }

    /**
     * A follower TAKES a term whose holder is dead and whose lease has lapsed
     * (M5.6j, ADR-0039).
     *
     * <p>⚠️ THIS IS THE GUARD against ADR-0039's rejected design — promote a
     * follower on a peer's REFUSAL — and it was written only after the first
     * attempt did not guard. Review MEASURED that one: it asserted a follower
     * does NOT promote when a dead peer's forward fails, which is the right
     * answer under the rejected design too, because it keeps the leader's lease
     * valid. A test both worlds agree on guards neither.
     *
     * <p>⚠️ WHAT SEPARATES THEM is a leader that is dead AND a lease that has
     * lapsed: the term is genuinely going begging and nobody is alive to refuse
     * anything. Under the rejected design no refusal ever arrives, so no
     * election runs — and this test then fails on the COMMIT rather than on an
     * assertion, which is the outage itself rather than a proxy for it.
     */
    @Test
    void aFollowerTAKESTheTermWhenTheLeaderIsDEADAndTheLeaseHasLAPSED() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        MovableClock clock = new MovableClock();
        InProcessTransport transport = new InProcessTransport();
        try (FleetSequencer leader = pod(store, "poda", A, transport, clock);
                FleetSequencer follower = pod(store, "podb", B, transport, clock)) {
            assertThat(leader.leading()).isTrue();
            follower.commit(flush("podb", 0, "seg/warm"));

            // ⚠️ DEAD, NOT POLITE: it answers nothing and releases nothing, so
            // the lease it holds can only lapse.
            transport.unreachable(A);
            clock.millis += Duration.ofSeconds(11).toMillis();

            follower.commit(flush("podb", 1, "seg/after-death"));

            assertThat(follower.leading())
                    .as("the term was going begging and nobody was alive to say so -- a "
                            + "follower that only promotes on a peer's refusal never takes it")
                    .isTrue();
        }
    }

    /**
     * A peer that ANSWERS NOTHING is not a peer that refuses (M5.6j).
     *
     * <p>⚠️ IT PINS THE FIXTURE DISTINCTION THE GUARD ABOVE RESTS ON, and that
     * makes it the guard's foundation rather than a sibling of it. Review
     * MEASURED both halves: emptying {@code unreachable()} reds THIS test and
     * leaves the guard green, because on today's eager-election path the guard
     * promotes off the lapsed lease whether or not the peer answers. Delete
     * this test and {@code unreachable()} is free to become {@code gone()}
     * again — with the guard still green and the liveness regression invisible
     * a second time.
     *
     * <p>⚠️ AND THE DISTINCTION IS A REAL ONE, not a fixture detail: nothing
     * answered, so the commit's outcome is UNKNOWN and re-sending it elsewhere
     * could duplicate. A {@code NotTheLeaseholderException} here would be a
     * claim about the lease that no dead pod is in a position to make.
     */
    @Test
    void aPeerThatANSWERSNOTHINGIsNotAPeerThatREFUSES() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        MovableClock clock = new MovableClock();
        // ⚠️ WITH THE INBOX (M8.14a): the dead-peer flush now defers, and the
        // peer put back must drain it before this pod forwards again.
        InProcessTransport transport = new InProcessTransport().withInbox(store, PREFIX);
        try (FleetSequencer leader = pod(store, "poda", A, transport, clock);
                FleetSequencer follower = pod(store, "podb", B, transport, clock)) {
            assertThat(leader.leading()).isTrue();
            follower.commit(flush("podb", 0, "seg/warm"));

            // ⚠️ THE LEADER DIES WITHOUT SAYING SO. Its lease still names it and
            // is still unexpired; it simply stops answering.
            transport.unreachable(A);

            assertThatThrownBy(() -> follower.commit(flush("podb", 1, "seg/dead-peer")))
                    .as("a dead peer's forward is AMBIGUOUS, not a refusal")
                    .isInstanceOf(IOException.class)
                    // ⚠️ ON THE MESSAGE, because the TYPE cannot tell them
                    // apart here: `RemoteSequencer` catches a refusal, re-reads
                    // the lease, finds it unchanged and rethrows a plain
                    // `IOException` -- so both paths arrive as one. MEASURED:
                    // with the fixture's unreachable branch disabled this test
                    // passed on the type assertions alone, because `gone()`
                    // empties the peer and the refusal was merely wrapped.
                    // ⚠️ SINCE M8.14a THE COMMIT DEFERS TO THE INBOX, and the
                    // ambiguous failure is what it deferred ON: the cause.
                    .isInstanceOf(CommitDeferredException.class)
                    .satisfies(deferred -> assertThat(deferred.getCause())
                            .hasMessageContaining("not reachable"))
                    .isNotInstanceOf(SequencerTransport.NotTheLeaseholderException.class)
                    .isNotInstanceOf(FencedException.class);

            // ⚠️ AND PUTTING A PEER BACK MAKES IT ANSWER AGAIN. Nothing else in
            // the tree calls `at()` after `unreachable()`, so without this the
            // flag-clearing is a five-line comment promising an undo the tree
            // never checks -- which is the defect the deleted `reachable()` had.
            transport.at(A, leader);
            assertThat(follower.commit(flush("podb", 2, "seg/back")).segments())
                    .as("a peer put back answers again")
                    .isNotEmpty();
            assertThat(follower.leading())
                    .as("and it did NOT promote on it: nothing answered, so nothing said the "
                            + "term was going -- which is ADR-0039's whole point")
                    .isFalse();
        }
    }

}
