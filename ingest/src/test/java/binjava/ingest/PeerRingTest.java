// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/**
 * Who fetches a segment for an AZ, and why exactly one pod does (M5.8).
 *
 * <p>⚠️ FR-12 MANDATES AT LEAST TWO INGESTER NODES PER AZ, so without a rule
 * naming WHICH pod fetches, each fetches for its own consumers: 2 pods x 2
 * non-writing AZs is 4 GETs against criterion 9's 2, and ADR-0004's arithmetic
 * -- the difference between $25 and $3,732 a month -- fails. The ring is that
 * rule, and ADR-0012 requires placement COMPUTED rather than discovered.
 *
 * <p>⚠️ ONE TEST HERE WAS DELETED AND THEN RESTORED, and the reasoning changed
 * rather than the code. Against a bare {@code List<Peer>} it could not fail:
 * the parameter was the same list either way, so it asserted
 * {@code x.equals(x)} and its red would not record. Now that
 * {@link PeerRing#ownerOf} takes an {@link AzPeers} the parameter still cannot
 * carry another AZ -- but review MEASURED an implementation reading a STATIC
 * fleet, which observes az-b through it and stays green. So the property is
 * falsifiable after all, just not through the parameter.
 */
class PeerRingTest {

    private static final Peer A1 = new Peer("pod-a1", "10.0.1.1:9000", "az-a");
    private static final Peer A2 = new Peer("pod-a2", "10.0.1.2:9000", "az-a");
    private static final Peer A3 = new Peer("pod-a3", "10.0.1.3:9000", "az-a");
    private static final Peer B1 = new Peer("pod-b1", "10.0.2.1:9000", "az-b");
    private static final Peer B2 = new Peer("pod-b2", "10.0.2.2:9000", "az-b");

    private static AzPeers azA(Peer... peers) {
        return new AzPeers("az-a", List.of(peers));
    }

    /**
     * Another AZ scaling does not move this AZ's owner.
     *
     * <p>⚠️ THE LOOP BELOW NO LONGER GUARDS ANYTHING, and saying so is better
     * than implying otherwise. Since M5.9 both views are the same VALUE --
     * review MEASURED {@code before.localAz().equals(after.localAz())} -- so
     * the 200 iterations are f(x) == f(x). The property moved to
     * {@link AzPeers}'s constructor, which is the right layer: an
     * implementation looking outside its argument now dies there rather than
     * here. What still earns its place is the pair of readbacks at the end,
     * which pin that the view filters by AZ at all.
     */
    @Test
    void anotherAZSCALINGDoesNotMoveThisAZSOwner() {
        // ⚠️ ONE PAIR OF FLEET LISTS, USED FOR BOTH HALVES. Review MEASURED
        // that building the anti-vacuity readbacks from their own literals let
        // `after`'s fleet lose B2 -- or az-b entirely -- with this method still
        // green: the guard could no longer see the change it exists to prove.
        List<Peer> smallFleet = List.of(A1, A2, A3, B1);
        List<Peer> grownFleet = List.of(A1, A2, A3, B1, B2);
        Membership before = new StaticMembership(A1, smallFleet);
        Membership after = new StaticMembership(A1, grownFleet);
        for (int i = 0; i < 200; i++) {
            String segment = "seg/" + i;
            assertThat(PeerRing.ownerOf(segment, after.localAz()))
                    .as("az-b gaining a pod must not move az-a's owner for %s", segment)
                    .isEqualTo(PeerRing.ownerOf(segment, before.localAz()));
        }
        // ⚠️ AND THE TWO FLEETS MUST ACTUALLY DIFFER, or this compares a view
        // with itself. Review MEASURED that a STATIC fleet shared between
        // instances survives the loop above -- az-a filters identically out of
        // either fleet. Since M5.9 removed the cross-AZ query, the difference
        // is read from a pod that LIVES in az-b rather than by asking for it.
        assertThat(new StaticMembership(B1, smallFleet).localAz().peers())
                .containsExactly(B1);
        assertThat(new StaticMembership(B1, grownFleet).localAz().peers())
                .containsExactlyInAnyOrder(B1, B2);
    }

    /**
     * A MIXED AZ view cannot be built at all.
     *
     * <p>⚠️ THIS IS NOT THE WHOLE OF CRITERION 10. It makes a mixed view
     * unrepresentable -- review MEASURED that a bare {@code List<Peer>}
     * answered an az-b peer for an az-a call on 13 of 20 segments. It does NOT
     * stop a caller asking for another AZ outright, which needs a notion of
     * "me" this layer lacks and is M5.9's.
     *
     * <p>⚠️ THE FOREIGN PEER SITS IN THE MIDDLE, which kills a first-only, a
     * last-only and any other single-position check at once. Review MEASURED
     * that validating the last element alone passed the earlier fixture -- the
     * same tail-blindness the removal case had, one class over.
     */
    @Test
    void aMIXEDAZViewCannotBeBuilt() {
        assertThatThrownBy(() -> new AzPeers("az-a", List.of(A1, B1, A2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pod-b1")
                .hasMessageContaining("intra-AZ only");
        assertThatThrownBy(() -> new AzPeers("az-a", List.of(B1, A1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("intra-AZ only");
        // ⚠️ AND A LONE FOREIGN PEER, because review MEASURED that skipping the
        // check when `peers.size() == 1` survives both fixtures above.
        assertThatThrownBy(() -> new AzPeers("az-a", List.of(B1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("intra-AZ only");
    }

    /** An AZ view needs an AZ to be a view of. */
    @Test
    void anAZViewWITHOUTAnAZIsRefused() {
        assertThatThrownBy(() -> new AzPeers(" ", List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("needs an az");
        // ⚠️ NULL AS WELL AS BLANK -- the same asymmetry `PeerTest` closed for
        // `Peer`, left open here one class over. (An earlier version of this
        // comment cited `Membership.inAz`'s contract; M5.9 deleted that method.)
        assertThatThrownBy(() -> new AzPeers(null, List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("needs an az");
    }

    /**
     * Two peers sharing a podId still get ONE agreed owner.
     *
     * <p>⚠️ THIS IS WHAT EVERY ROLLING RESTART PRODUCES: `podShortId` is stable
     * across a restart (ADR-0036) under a StatefulSet (ADR-0031), so the old
     * address sits beside the new one for the reconciliation window. On a
     * podId-only tie-break review MEASURED two pods holding that list in
     * different orders disagreeing on 102 of 200 segments; on
     * {@code (podId, endpoint)} it is 0.
     *
     * <p>⚠️ AN EARLIER VERSION REFUSED THE DUPLICATE and that was worse: it
     * threw from {@code inAz} on EVERY call -- measured, 100 of 100 -- taking
     * out the whole AZ including the peer that was not duplicated, which turns
     * a transient membership disagreement into a hard failure. ADR-0012 and
     * architecture.md both say such a disagreement costs an extra GET, never
     * correctness.
     */
    @Test
    void TWOPeersSharingAPodIdStillAgreeOnONEOwner() {
        Peer oldAddress = new Peer("ingester-0", "10.0.1.1:9000", "az-a");
        Peer newAddress = new Peer("ingester-0", "10.0.1.7:9000", "az-a");
        Peer other = new Peer("ingester-1", "10.0.1.2:9000", "az-a");
        AzPeers oneOrder = new AzPeers("az-a", List.of(oldAddress, newAddress, other));
        AzPeers another = new AzPeers("az-a", List.of(newAddress, other, oldAddress));

        for (int i = 0; i < 200; i++) {
            String segment = "seg/" + i;
            assertThat(PeerRing.ownerOf(segment, another))
                    .as("a duplicated podId must not make two pods disagree about %s", segment)
                    .isEqualTo(PeerRing.ownerOf(segment, oneOrder));
        }
        // ⚠️ CONTENTS, NOT SIZE. Review MEASURED that `hasSize(3)` stayed green
        // while an interim version of `StaticMembership` put self into its own
        // view twice and erased the other entry -- the count was still 3 and
        // two pods disagreed on 96 of 200 segments.
        assertThat(new StaticMembership(oldAddress, List.of(oldAddress, newAddress, other))
                .localAz().peers())
                .as("every pod in the AZ sees the same list, duplicates included")
                .containsExactlyInAnyOrder(oldAddress, newAddress, other);
    }

    /** An AZ view cannot be widened after it is built. */
    @Test
    void anAZViewCannotBeWIDENEDAfterItIsBuilt() {
        List<Peer> mutable = new ArrayList<>(List.of(A1, A2));
        AzPeers view = new AzPeers("az-a", mutable);
        mutable.add(A3);

        assertThat(view.peers())
                .as("the view copies, so the caller's later add is not in it")
                .containsExactlyInAnyOrder(A1, A2);
        assertThatThrownBy(() -> view.peers().add(B1))
                .as("and it cannot be added to, which is where a cross-AZ peer would get in")
                .isInstanceOf(UnsupportedOperationException.class);
    }

    /**
     * The owner is a VALUE two processes agree on, not merely a stable one.
     *
     * <p>⚠️ PINNED AGAINST LITERALS, and that is the whole point. Review
     * MEASURED that a per-JVM seed mixed into the score passes every "these two
     * calls agree" assertion -- determinism, consistency and balance all hold
     * -- while making every pod in the fleet choose a different owner, so all
     * of them fetch. Only a golden value catches it. Same discipline as
     * {@code format}'s golden files, for the same reason.
     */
    @Test
    void theOwnerIsAGOLDENValueThatTwoProcessesMustAGREEOn() {
        AzPeers three = azA(A1, A2, A3);
        assertThat(PeerRing.ownerOf("seg/2026/09/10/0", three)).contains(A1);
        assertThat(PeerRing.ownerOf("seg/2026/09/10/1", three)).contains(A3);
        assertThat(PeerRing.ownerOf("seg/2026/09/10/4", three)).contains(A2);
    }

    /**
     * A non-ASCII segment key hashes the same whatever the platform charset is.
     *
     * <p>⚠️ THIS PINS A VALUE; IT DOES NOT KILL THE CHARSET MUTATION, and the
     * difference is stated because claiming otherwise would be false. Review
     * found that dropping the explicit {@code UTF_8} survives every ASCII case
     * here. It survives THIS one too -- MEASURED -- because JEP 400 makes UTF-8
     * the platform default from Java 18 and this build is on 25, so
     * {@code getBytes()} and {@code getBytes(UTF_8)} agree unless
     * {@code file.encoding} is overridden, which the build does not do. The
     * explicit charset stays as defence against that override and against a
     * future runtime; what this case buys is a non-ASCII golden owner, so any
     * change in how key bytes are derived reds here.
     */
    @Test
    void aNONASCIISegmentKeyIsHashedAsUTF8Regardless() {
        assertThat(PeerRing.ownerOf("seg/2026/09/10/caf\u00e9-\u00fc", azA(A1, A2, A3)))
                .contains(A3);
    }

    /** The answer does not depend on the order the caller built the list in. */
    @Test
    void theAnswerDoesNotDependOnLISTORDER() {
        AzPeers forwards = azA(A1, A2, A3);
        AzPeers backwards = azA(A3, A2, A1);
        for (int i = 0; i < 500; i++) {
            String segment = "seg/" + i;
            assertThat(PeerRing.ownerOf(segment, backwards))
                    .as("two pods listing the same AZ differently must still agree on %s", segment)
                    .isEqualTo(PeerRing.ownerOf(segment, forwards));
        }
    }

    /**
     * Ties are broken by podId, so a collision does not become order-dependent.
     *
     * <p>⚠️ REACHABLE, NOT THEORETICAL: {@code avalanche} is a bijection over a
     * 32-bit CRC, so a score carries 32 bits and collisions are about
     * n(n-1)/2 over 2^32. Review found this pair by search -- both score
     * 1977658691747965201 for this segment.
     */
    @Test
    void aSCORETIEIsBrokenByPodIdRatherThanByListOrder() {
        Peer low = new Peer("pod-1371838", "10.0.1.9:9000", "az-a");
        Peer high = new Peer("pod-2000402", "10.0.1.8:9000", "az-a");
        String segment = "seg/2026/09/10/abc";

        assertThat(PeerRing.ownerOf(segment, azA(low, high)))
                .as("the tie must resolve to the lower podId whichever way round they are listed")
                .contains(low)
                .isEqualTo(PeerRing.ownerOf(segment, azA(high, low)));
    }

    /**
     * Removing a member moves ONLY its own segments, at the head as well as the
     * tail.
     *
     * <p>⚠️ THE DEPARTURE IS AT THE HEAD ON PURPOSE. Review MEASURED that a
     * score of {@code podId + listIndex} -- consistent-looking, but really
     * position-dependent -- survives a tail-only test at 664 == 664, and reds
     * at the head with 669 owned against 1338 moved.
     */
    @Test
    void REMOVINGAMemberMovesONLYItsOwnSegments() {
        AzPeers three = azA(A1, A2, A3);
        AzPeers withoutHead = azA(A2, A3);
        int moved = 0;
        int ownedByGone = 0;
        for (int i = 0; i < 2000; i++) {
            String segment = "seg/" + i;
            Peer was = PeerRing.ownerOf(segment, three).orElseThrow();
            Peer now = PeerRing.ownerOf(segment, withoutHead).orElseThrow();
            if (was.equals(A1)) {
                ownedByGone++;
            }
            if (!was.equals(now)) {
                moved++;
            }
        }
        // MEASURED: 660 of 2,000 owned by the departing head, and exactly 660
        // move. Rendezvous makes that an equality rather than a bound.
        assertThat(ownedByGone).as("the departing member must have owned something").isPositive();
        assertThat(moved)
                .as("only the departed member's segments move -- a modulo ring reshuffles "
                        + "nearly all 2,000")
                .isEqualTo(ownedByGone);
    }

    /**
     * ADDING a member takes only its own share, and disturbs nothing else.
     *
     * <p>⚠️ CRITERION (4) SAYS "ADDING OR REMOVING" and only removal was
     * tested. MEASURED: 523 of 2,000 move when a fourth pod joins, against an
     * ideal quarter of 500.
     */
    @Test
    void ADDINGAMemberMovesOnlyItsOwnShare() {
        AzPeers three = azA(A1, A2, A3);
        Peer joined = new Peer("pod-a4", "10.0.1.4:9000", "az-a");
        AzPeers four = azA(A1, A2, A3, joined);
        int moved = 0;
        int toTheNewcomer = 0;
        for (int i = 0; i < 2000; i++) {
            String segment = "seg/" + i;
            Peer was = PeerRing.ownerOf(segment, three).orElseThrow();
            Peer now = PeerRing.ownerOf(segment, four).orElseThrow();
            if (!was.equals(now)) {
                moved++;
            }
            if (now.equals(joined)) {
                toTheNewcomer++;
            }
        }
        assertThat(moved)
                .as("everything that moved went to the newcomer, and nothing else shifted")
                .isEqualTo(toTheNewcomer);
        assertThat(moved).as("near a quarter of 2,000, measured 523").isBetween(400, 650);
    }

    /** Ownership is spread across members rather than piled on one. */
    @Test
    void ownershipIsBALANCEDAcrossMembers() {
        List<Peer> three = List.of(A1, A2, A3);
        AzPeers view = azA(A1, A2, A3);
        int[] counts = new int[3];
        for (int i = 0; i < 3000; i++) {
            counts[three.indexOf(PeerRing.ownerOf("seg/2026/09/10/" + i, view).orElseThrow())]++;
        }
        // ⚠️ THE BOUND IS SET FROM MEASUREMENT, not from taste. MEASURED with
        // THESE ids (`pod-a1..pod-a3`, 1-based): [1007, 1008, 985], and skew
        // (max over ideal) 1.010 / 1.010 / 1.013 / 1.033 at 2, 3, 5 and 8
        // members over 30,000 segments. ⚠️ AN EARLIER NOTE QUOTED 1.000 /
        // 1.018 / 1.019 / 1.037, which came from 0-BASED ids; review could not
        // reproduce it from what was recorded, so the ids are named here.
        // +-15% leaves room for that without admitting the failure this exists
        // to catch: CRC32C without an avalanche finalizer gave [760, 740,
        // 1500], one pod owning half the AZ's fetches.
        for (int c : counts) {
            assertThat(c)
                    .as("each of 3 members owns near a third of 3,000, %s", Arrays.toString(counts))
                    .isBetween(850, 1150);
        }
    }

    /** A ring answers more than one peer. */
    @Test
    void aRingThatAnswersONEPeerForEverythingIsNotARing() {
        AzPeers three = azA(A1, A2, A3);
        assertThat(IntStream.range(0, 50)
                .mapToObj(i -> PeerRing.ownerOf("seg/" + i, three).orElseThrow())
                .distinct().count())
                .isGreaterThan(1);
    }

    /**
     * An AZ with no members yields no owner, and that is not an error.
     *
     * <p>⚠️ EMPTY IS A LADDER RUNG, NOT A BUG. ADR-0012's miss ladder ends at
     * the object store, so no ring owner means fetch there -- correct, one
     * extra GET. An earlier version threw, which would have crashed on a
     * routine single-pod AZ.
     */
    @Test
    void anEMPTYAZYieldsNOOwnerRatherThanThrowing() {
        assertThat(PeerRing.ownerOf("seg/x", new AzPeers("az-empty", List.of()))).isEmpty();
        // ⚠️ AND A ONE-PEER AZ HAS AN OWNER, which is the case the paragraph
        // above calls routine. Review MEASURED that `isEmpty()` widened to
        // `size() <= 1` survives every other case here -- no fixture passes an
        // AZ of exactly one -- and a single-pod AZ would then send every
        // consumer to the object store for every segment.
        assertThat(PeerRing.ownerOf("seg/x", azA(A1))).contains(A1);
    }
}
