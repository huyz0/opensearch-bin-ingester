// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * The fallback ladder keeps a LIST off every automatic path (M5.18, FR-10,
 * cost.md R15).
 *
 * <p>⚠️ THE PROPERTY IS A TYPE, NOT A COUNT, AND THAT IS WHY THE CASES LOOK
 * LIKE THIS. gate-design's ladder puts "make the bad state unrepresentable"
 * above "count it in a test": {@link FallbackLadder#tierFor} returns an
 * {@link FallbackLadder.AutomaticTier}, which HAS no tier-4 constant, so no
 * degradation path can answer "scan the data prefix" however wrong it is about
 * the health it was given. What the cases below pin is that the type stayed
 * that shape, that every health is classified, and the arithmetic the ceiling
 * is actually about.
 */
class FallbackLadderTest {

    @Test
    void everyHEALTHHasAnAutomaticTier() {
        for (FallbackLadder.Health health : FallbackLadder.Health.values()) {
            assertThat(FallbackLadder.tierFor(health))
                    .as("a health nothing classifies is a consumer with no answer at the "
                            + "moment it has lost the ingester -- %s", health)
                    .isNotNull();
        }
    }

    @Test
    void theAUTOMATICLadderHasNoTierThatLISTs() {
        for (FallbackLadder.AutomaticTier tier : FallbackLadder.AutomaticTier.values()) {
            assertThat(tier.listsPerNodePerInterval())
                    .as("cost.md R15 caps LIST at ~1/s sustained and 300 plugin nodes "
                            + "scanning breaches it at fan-out -- %s", tier)
                    .isZero();
        }
        SubscriptionTransport bare = new SubscriptionTransport() {
            @Override
            public AutoCloseable subscribe(io.github.huyz0.os.biningester.format.RunKey key,
                    Listener listener) {
                return () -> { };
            }
        };
        assertThat(bare.ingesterAnswers())
                .as("a transport without a reachability signal must not claim an ingester answered")
                .isFalse();
    }

    /**
     * Each health enters the tier doc 04 section 3 names for it (M5.18, round
     * 1's test major).
     *
     * <p>⚠️ THE MAPPING ITSELF WAS UNCONSTRAINED, and review measured three
     * surviving mutations: {@code CONNECTION_LOST -> PUSH},
     * {@code NO_NODE_REACHABLE -> RECOVER} and, the damaging one,
     * {@code CHAIN_UNREADABLE -> POLL_CHAIN} -- a consumer that cannot follow
     * the chain told to go on polling it, with the whole suite green. Nothing
     * saw them: a total switch over an enum cannot return null, the ordinals
     * stayed non-decreasing, and the 300-node case reads only the GET figure,
     * which {@code RECOVER} shared. So the mapping is asserted PAIR BY PAIR
     * rather than through a property that happens to hold.
     */
    @Test
    void eachHEALTHEntersTheTierTheCorpusNamesForIt() {
        assertThat(FallbackLadder.tierFor(FallbackLadder.Health.PUSHING))
                .as("tier 0: a delivering subscription costs no request at all")
                .isEqualTo(FallbackLadder.AutomaticTier.PUSH);
        assertThat(FallbackLadder.tierFor(FallbackLadder.Health.CONNECTION_LOST))
                .as("tier 1: a dropped connection is answered by reconnecting to another "
                        + "same-AZ node, NOT by falling to the object store -- a consumer "
                        + "that polled the chain on every reconnect would put the read rate "
                        + "on the disconnection rate")
                .isEqualTo(FallbackLadder.AutomaticTier.RECONNECT);
        assertThat(FallbackLadder.tierFor(FallbackLadder.Health.NO_NODE_REACHABLE))
                .as("tier 2: with no ingester node answering, the chain is polled -- "
                        + "recovering instead would cost a replay burst per interval for an "
                        + "outage a single speculative GET rides out")
                .isEqualTo(FallbackLadder.AutomaticTier.POLL_CHAIN);
        assertThat(FallbackLadder.tierFor(FallbackLadder.Health.CHAIN_UNREADABLE))
                .as("tier 3: a consumer that cannot follow the chain must NOT be told to go "
                        + "on polling it -- that is a stuck consumer reporting itself healthy, "
                        + "and it is the mutation that survived round 1")
                .isEqualTo(FallbackLadder.AutomaticTier.RECOVER);
    }

    /** Tier 3's representative replay count is measured by M9.45, not guessed. */
    @Test
    void theRECOVERTierCostsTENSOfGetsRatherThanOne() {
        assertThat(FallbackLadder.AutomaticTier.RECOVER.getsPerNodePerInterval())
                .as("M9.45 measures checkpoint + three delta + one shared segment GET; "
                        + "TierThreeRecovery separately caps a full episode at 30 GETs")
                .isEqualTo(5);
    }

    @Test
    void escalationNeverSTEPSBackToACheaperTier() {
        FallbackLadder.AutomaticTier previous = null;
        for (FallbackLadder.Health health : FallbackLadder.Health.values()) {
            FallbackLadder.AutomaticTier tier = FallbackLadder.tierFor(health);
            if (previous != null) {
                assertThat(tier.ordinal())
                        .as("Health is declared worst-last, so answering a worse health with "
                                + "a cheaper tier is a consumer that stops trying as the "
                                + "outage deepens -- %s", health)
                        .isGreaterThanOrEqualTo(previous.ordinal());
            }
            previous = tier;
        }
    }

    @Test
    void threeHUNDREDDegradedNodesIssueZEROListsAndAFlatGetRate() {
        int nodes = 300;
        Duration interval = Duration.ofSeconds(5);
        long gets = 0;
        long lists = 0;
        for (int node = 0; node < nodes; node++) {
            FallbackLadder.AutomaticTier tier =
                    FallbackLadder.tierFor(FallbackLadder.Health.NO_NODE_REACHABLE);
            gets += tier.getsPerNodePerInterval();
            lists += tier.listsPerNodePerInterval();
        }

        assertThat(lists)
                .as("the whole of criterion 13: a fleet in the degraded tier issues no LIST "
                        + "at all, so R15's ~1/s ceiling is not approached rather than merely "
                        + "respected")
                .isZero();
        assertThat(gets / (double) interval.toSeconds())
                .as("one speculative GET per NODE per interval -- doc 04 section 3 prices 9 "
                        + "nodes at 1.8 GET/s, and 300 nodes is the same number per node. A "
                        + "rate per SHARD or per STREAM here is what non-negotiable 6 forbids")
                .isEqualTo(nodes / (double) interval.toSeconds());
    }

    @Test
    void aSCANIsNOTREACHABLEWithoutAnExplicitRecoveryAction() {
        assertThatThrownBy(() -> FallbackLadder.scan(null, Duration.ofHours(1)))
                .as("tier 4 behind an explicit recovery action is the whole of R15's "
                        + "mitigation -- a null token is the automatic path arriving here")
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void aSCANMustNameABOUNDEDWindow() {
        FallbackLadder.RecoveryAction action = FallbackLadder.RecoveryAction.requestedBy("sre");

        assertThatThrownBy(() -> FallbackLadder.scan(action, Duration.ofSeconds(-1)))
                .as("a NEGATIVE window is the same unbounded scan arrived at by subtraction "
                        + "-- and dropping the isNegative conjunct left the suite green")
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> FallbackLadder.scan(action, Duration.ZERO))
                .as("doc 04 section 3 says tier 4 LISTs the data prefix over a BOUNDED time "
                        + "window; an unbounded one is O(uptime) keys at 1 LIST per 1000")
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void aSCANCarriesTheActionThatAskedForIt() {
        FallbackLadder.RecoveryAction action = FallbackLadder.RecoveryAction.requestedBy("sre");

        FallbackLadder.Scan scan = FallbackLadder.scan(action, Duration.ofHours(6));

        assertThat(scan.requestedBy())
                .as("a LIST that cannot say who asked for it is indistinguishable in the bill "
                        + "from one a hot path started")
                .isEqualTo(action);
    }

    @Test
    void aRECOVERYActionNeedsANonBlankRequester() {
        assertThatThrownBy(() -> FallbackLadder.RecoveryAction.requestedBy("  "))
                .as("an operator-facing token nobody signed is the automatic path wearing a "
                        + "costume")
                .isInstanceOf(IllegalArgumentException.class);
    }
}
