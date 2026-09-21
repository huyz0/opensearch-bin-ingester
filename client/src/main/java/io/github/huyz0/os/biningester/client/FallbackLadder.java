// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.client;

import java.time.Duration;
import java.util.Objects;

/**
 * How a consumer degrades when the ingester is unreachable (M5.18, FR-10).
 *
 * <p>⚠️ AN INGESTER OUTAGE DEGRADES LATENCY, NEVER CORRECTNESS OR
 * AVAILABILITY. Doc 04 § 3 gives the ladder five tiers: push, reconnect, poll
 * the commit chain, recover from the newest checkpoint, and — only in a full
 * disaster — LIST the data prefix. The first four are automatic; the fifth is
 * not, and this class exists to make that structural.
 *
 * <p>⚠️ TIER 4 IS NOT A CONSTANT OF {@link AutomaticTier}, which is the whole
 * design. cost.md R15 caps LIST at ~1 per second sustained, and M4's SPEC
 * warns that up to 300 plugin nodes scanning breaches that ceiling AT FAN-OUT
 * rather than at any one node's scale. A tier-4 constant on the automatic
 * ladder would make "never run it on a hot path" a rule an implementer has to
 * remember; gate-design ranks making the state unrepresentable above every
 * form of remembering, so {@link #tierFor} cannot name a scan at all and
 * {@link #scan} takes a {@link RecoveryAction} no automatic path holds.
 *
 * <p>⚠️ IT IS A POLICY AND IT PERFORMS NOTHING. There is no request here, no
 * socket and no store — {@code client} may not import {@code io.github.huyz0.os.biningester.binstore}
 * at all (ADR-0023), and the byte paths live behind {@link SegmentSource} and
 * {@link SubscriptionTransport}. What this answers is WHICH tier a consumer is
 * in and what one interval of it costs per node.
 *
 * <p>⚠️ TIERS 0 AND 1 EXECUTE SINCE M8.28: {@code HttpSubscriptionTransport}
 * asks {@link #tierFor} at every transition it observes and counts the tiers
 * it enters. Tiers 2 and 3 read the store, which a plugin holding no cloud SDK
 * cannot yet do, and re-reading a gap needs a catch-up read path that does not
 * exist -- both are M9's, by ADR-0057.
 */
public final class FallbackLadder {

    private FallbackLadder() {
    }

    /**
     * How much of the path to the ingester is working, WORST LAST.
     *
     * <p>⚠️ THE ORDER IS LOAD-BEARING: {@link #tierFor} is required never to
     * answer a worse health with an EARLIER rung of the ladder, and that
     * property is expressed over {@link AutomaticTier}'s ordinals. ⚠️ Rung,
     * not cost: the ladder's positions are 0-3 and its per-interval GETs are
     * 0, 0, 1 and a replay burst, so "further down" and "more expensive" are
     * different statements and only the first is asserted.
     */
    public enum Health {

        /** A subscription is delivering. */
        PUSHING,

        /** The connection dropped; other ingester nodes are presumed reachable. */
        CONNECTION_LOST,

        /** No ingester node answers, in this AZ or any other. */
        NO_NODE_REACHABLE,

        /** The commit chain cannot be followed from where this consumer is. */
        CHAIN_UNREADABLE
    }

    /**
     * The tiers a consumer may enter BY ITSELF, and every one of them costs
     * zero LISTs.
     *
     * <p>⚠️ THE MISSING CONSTANT IS THE POINT. Tier 4 — LIST the data prefix,
     * filter by the key's membership bloom, read segment headers — has no
     * member here, so no amount of misclassifying a health can produce one.
     * {@link Scan} is how it is named, and {@link #scan} is the only entry
     * point that takes a {@link RecoveryAction}.
     */
    public enum AutomaticTier {

        /** Tier 0: parked on a push subscription. No request at all. */
        PUSH(0, 0),

        /**
         * Tier 1: reconnect with jittered backoff, same AZ first. Still no
         * object-store request — the retry is against an ingester node.
         */
        RECONNECT(0, 0),

        /**
         * Tier 2: one speculative {@code GET} of the next chain entry per NODE
         * per interval, a 404 meaning nothing new.
         *
         * <p>⚠️ PER NODE, NEVER PER SHARD OR PER STREAM. Doc 04 § 3 prices 9
         * nodes at a 5 s interval as 1.8 GET/s, about $2 a month, and calls it
         * a perfectly acceptable degraded mode. Per stream it would be ~400x
         * that on one catch-up node, which is the scaling non-negotiable 6
         * forbids by name.
         */
        POLL_CHAIN(1, 0),

        /**
         * Tier 3: resolve the lease, read the newest checkpoint, replay the
         * deltas — tens of GETs, ONCE, not per interval.
         *
         * <p>⚠️ {@link #TENS_OF_GETS} IS A MODEL AND NOT A MEASUREMENT
         * (performance.md rule 7). An earlier draft carried 1 here, which is
         * conservative for the LIST question this class exists for and wrong
         * in the cheap direction for the GET question: doc 04 section 3 prices
         * tier 3 at "tens of GETs, once", so a reader adding up a fleet's
         * recovery would have been told a thirtieth of it. What falsifies the
         * number is a real replay counting its own GETs, which ADR-0057 moved
         * to M9 with the catch-up read path it needs.
         *
         * <p>⚠️ ONCE, NOT PER INTERVAL, and the accessor's name overstates
         * this one tier. The burst is bounded by the replay rather than by the
         * poll, and a node that recovers leaves this tier. M4 made this path
         * find the newest checkpoint WITHOUT a list precisely because 300
         * nodes recovering would otherwise breach R15 here rather than at tier
         * 4.
         */
        RECOVER(Model.TENS_OF_GETS, 0);

        /**
         * Doc 04 section 3's "tens of GETs" for one node's replay, as a
         * number.
         *
         * <p>⚠️ MODELLED, and the corpus says "tens" rather than a figure.
         * Thirty is the middle of that word and nothing measures it yet.
         */
        static final int TENS_OF_GETS = Model.TENS_OF_GETS;

        /**
         * ⚠️ A HOLDER, because an enum constant's argument may not read a
         * static field of its own enum -- the constants are initialised first.
         */
        private static final class Model {
            static final int TENS_OF_GETS = 30;

            private Model() {
            }
        }

        private final int gets;
        private final int lists;

        AutomaticTier(int gets, int lists) {
            this.gets = gets;
            this.lists = lists;
        }

        /** Object-store GETs one node in this tier issues per poll interval. */
        public int getsPerNodePerInterval() {
            return gets;
        }

        /**
         * LISTs one node in this tier issues per poll interval, which is zero
         * for every tier and is a method rather than a constant so that adding
         * a tier has to state it.
         */
        public int listsPerNodePerInterval() {
            return lists;
        }
    }

    /**
     * An operator asking for tier 4, by name.
     *
     * <p>⚠️ IT CARRIES A REQUESTER BECAUSE THE BILL CANNOT TELL OTHERWISE. A
     * LIST that arrives with nothing attached is indistinguishable from one a
     * hot path started, and R15's ceiling is about sustained rate rather than
     * any single request — so the question an operator asks afterwards is who
     * else is scanning, and only the token can answer it.
     */
    public record RecoveryAction(String requestedBy) {

        public RecoveryAction {
            Objects.requireNonNull(requestedBy, "requestedBy");
            if (requestedBy.isBlank()) {
                throw new IllegalArgumentException(
                        "a recovery action names who asked for it; got a blank string");
            }
        }

        /** The operator, runbook or break-glass tool asking for the scan. */
        public static RecoveryAction requestedBy(String who) {
            return new RecoveryAction(who);
        }
    }

    /**
     * Tier 4: LIST the data prefix over a bounded window.
     *
     * <p>⚠️ WHAT HOLDS BY CONSTRUCTION IS THE SHAPE, NOT THE CALLER. This is
     * a public record with a public canonical constructor, so anything may
     * build one; an earlier draft of this paragraph claimed a scan was
     * reachable only through {@link #scan}, which is false. What cannot be
     * built is a scan with no named requester or an unbounded window -- and
     * what no AUTOMATIC path can do is name a scan at all, because
     * {@link #tierFor}'s return type has no member for it.
     *
     * <p>⚠️ THE WINDOW IS PART OF THE TYPE. Doc 04 § 3 costs the scan at one
     * LIST per 1,000 keys, so an unbounded window is O(uptime) requests from
     * one command — the shape R15's ceiling exists to keep off a sustained
     * path. Naming it here means a caller cannot forget it.
     */
    public record Scan(RecoveryAction requestedBy, Duration window) {

        public Scan {
            Objects.requireNonNull(requestedBy, "requestedBy");
            Objects.requireNonNull(window, "window");
            if (window.isZero() || window.isNegative()) {
                throw new IllegalArgumentException(
                        "tier 4 LISTs a BOUNDED window of the data prefix; got " + window);
            }
        }
    }

    /**
     * The tier a consumer in this state enters on its own.
     *
     * <p>⚠️ IT CANNOT RETURN A SCAN, by the return type. See {@link
     * AutomaticTier}.
     */
    public static AutomaticTier tierFor(Health health) {
        Objects.requireNonNull(health, "health");
        return switch (health) {
            case PUSHING -> AutomaticTier.PUSH;
            case CONNECTION_LOST -> AutomaticTier.RECONNECT;
            case NO_NODE_REACHABLE -> AutomaticTier.POLL_CHAIN;
            case CHAIN_UNREADABLE -> AutomaticTier.RECOVER;
        };
    }

    /**
     * Tier 4, which only an explicit recovery action reaches.
     *
     * @throws NullPointerException if no recovery action is named -- the
     *     automatic path arriving here is the defect this signature exists to
     *     make impossible
     */
    public static Scan scan(RecoveryAction action, Duration window) {
        return new Scan(Objects.requireNonNull(action, "action"), window);
    }
}
