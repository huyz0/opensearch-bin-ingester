// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A cross-AZ peer is not addressable, because there is nothing to address it
 * WITH (M5.9).
 *
 * <p>⚠️ ADR-0012 MEASURES A CROSS-AZ PEER FETCH AT 419x the object-store GET it
 * would replace, and requires the rule "enforced in code, not just documented".
 * M5.8 got half of that: {@link AzPeers} made a MIXED view unrepresentable, but
 * {@code inAz(String)} was public, and review MEASURED an az-a caller naming
 * az-b's peers 20 of 20 times. The remaining half is removing the query.
 */
class MembershipSeamTest {

    private static final Peer A1 = new Peer("pod-a1", "10.0.1.1:9000", "az-a");
    private static final Peer A2 = new Peer("pod-a2", "10.0.1.2:9000", "az-a");
    private static final Peer B1 = new Peer("pod-b1", "10.0.2.1:9000", "az-b");

    private static final List<Peer> FLEET = List.of(A1, A2, B1);

    /**
     * The seam offers no way to name an AZ.
     *
     * <p>⚠️ ABSENCE NEEDS A TEST OR IT IS NOT A PROPERTY. "There is no cross-AZ
     * accessor" cannot be observed from any call, so this reads the seam's
     * declared surface instead: adding one back reds here, which is the only
     * way this criterion can fail rather than quietly erode.
     */
    @Test
    void theSeamDeclaresNOMethodThatTakesAnAZ() {
        // ⚠️ AIMED AT `String`, NOT AT ARITY. An earlier version asserted every
        // method takes ZERO arguments, which is broader than the property: M8's
        // watch (`watch(listener)`, `onPeerLeft(Peer)`) takes arguments that
        // are not AZs, and the cheapest way to green that test would be to
        // loosen the assertion carrying the headline criterion. An AZ is a
        // String, so that is what to forbid.
        // ⚠️ AND THE IMPLEMENTATION IS SCANNED TOO. Review MEASURED that a
        // public `inAz(String)` on the final, public `StaticMembership`
        // restores full cross-AZ addressability with the seam untouched and
        // everything green -- the criterion's letter met at the interface while
        // the door stands open one class over.
        // ⚠️ NON-PRIVATE, NOT PUBLIC, AND ASSIGNABLE-TO-CharSequence, NOT
        // `String`. Review MEASURED three ways past a narrower scan, each
        // restoring full cross-AZ addressability with every test green: a
        // PACKAGE-PRIVATE `inAz(String)` -- callable from every future
        // fetch-path class, since they all live in this package, and the drift
        // an author gets by reading the test and complying with its letter; a
        // VARARGS `inAz(String...)`, whose parameter type is `String[]`; and a
        // `CharSequence` parameter on the seam itself.
        // ⚠️ CONSTRUCTORS TOO. Review MEASURED a
        // `StaticMembership(Peer, List<Peer>, String az)` restoring full
        // cross-AZ addressability with every test green, because
        // `getDeclaredMethods()` does not see constructors.
        // ⚠️ WHAT THIS CANNOT CATCH, recorded rather than papered over: a
        // wrapper type. Review MEASURED `record Az(String)` plus
        // `inAz(Az)` surviving, and no reflection over parameter types can see
        // that -- it is a deliberate act rather than the drift this guards.
        // ⚠️ THREE DIMENSIONS THIS SCAN CANNOT SEE, recorded rather than
        // implied away, because a guard that reads as exhaustive and is not is
        // worse than one whose edges are written down. Review MEASURED each:
        //   - a SECOND implementation carrying `inAz(String)` -- the list below
        //     names the types it checks, so a new class is simply not read, and
        //     that is exactly the shape M8's `EndpointSlice` version will have.
        //     Closing it needs classpath scanning, which is machinery this test
        //     does not carry;
        //   - a RETURN type: a `byAz()` accessor handing back the whole fleet,
        //     measured nameable 20 of 20, which also falsifies the "the seam no
        //     longer retains the fleet" sentence in `AzPeers`;
        //   - a FIELD, which is not scanned at all.
        // Parameters of the named types are the whole of what this pins.
        List<java.lang.reflect.Executable> surface = new java.util.ArrayList<>();
        for (Class<?> type : List.of(Membership.class, StaticMembership.class)) {
            surface.addAll(List.of(type.getDeclaredMethods()));
            surface.addAll(List.of(type.getDeclaredConstructors()));
        }
        for (java.lang.reflect.Executable member : surface) {
            {
                java.lang.reflect.Executable method = member;
                if (Modifier.isPrivate(method.getModifiers()) || method.isSynthetic()) {
                    continue;
                }
                for (Class<?> parameter : method.getParameterTypes()) {
                    Class<?> element = parameter.isArray() ? parameter.getComponentType() : parameter;
                    assertThat(CharSequence.class.isAssignableFrom(element))
                            .as("`%s.%s` takes a %s, and an AZ is text -- which is how a "
                                    + "cross-AZ fetch becomes addressable again",
                                    method.getDeclaringClass().getSimpleName(), method.getName(),
                                    parameter.getSimpleName())
                            .isFalse();
                }
            }
        }
        // ⚠️ `contains`, NOT `containsExactly`: review MEASURED that freezing
        // the surface reds on a benign zero-argument addition, which is the
        // pressure that gets an assertion loosened later. This still reds if
        // either query is removed.
        assertThat(Arrays.stream(Membership.class.getDeclaredMethods()).map(Method::getName))
                .contains("self", "localAz");
    }

    /** The local view is this pod's AZ, and holds this pod. */
    @Test
    void theLOCALViewIsThisPodsAZAndHoldsThisPod() {
        Membership here = new StaticMembership(A1, FLEET);

        assertThat(here.self()).isEqualTo(A1);
        assertThat(here.localAz().az()).isEqualTo("az-a");
        assertThat(here.localAz().peers()).containsExactlyInAnyOrder(A1, A2);

        // ⚠️ AND A POD THAT IS NOT FIRST IN ITS OWN AZ STILL REPORTS ITSELF.
        // Review's discipline applied before review: `self()` returning
        // `localAz().peers().get(0)` passed the assertions above, because A1
        // happens to be both. A pod that answered "self" with whichever peer
        // sorted first would own the wrong share of fetches and never know.
        assertThat(new StaticMembership(A2, FLEET).self()).isEqualTo(A2);
    }

    /**
     * The same fleet answers differently to a pod in another AZ.
     *
     * <p>⚠️ THE VIEW FOLLOWS IDENTITY, NOT AN ARGUMENT, which is the whole
     * difference from M5.8: there is no argument to get wrong.
     */
    @Test
    void theSameFleetAnswersTheOTHERAZToAPodThatLivesThere() {
        Membership there = new StaticMembership(B1, FLEET);

        assertThat(there.localAz().az()).isEqualTo("az-b");
        assertThat(there.localAz().peers()).containsExactly(B1);
    }

    /**
     * A restarted pod sees the fleet's entry for itself, not itself.
     *
     * <p>⚠️ THE FLEET WINS, AND THAT IS THE DECISION rather than an accident.
     * {@code podShortId} is stable across a restart (ADR-0036) while the
     * address changes, so the list still holds the old one. Substituting self
     * in would make this pod's view differ from its neighbours' -- review
     * MEASURED 96 of 200 segments -- and two pods disagreeing about the owner
     * both fetch, which is the arithmetic the ring exists to protect.
     *
     * <p>⚠️ SO "DO I OWN THIS" IS A podId COMPARISON. Review MEASURED that
     * {@code owner.equals(self())} matches 0 of 200 segments for a restarted
     * pod, which would silently stop it prefetching across its whole share.
     */
    @Test
    void aRestartedPodSeesTheFLEETSEntryForItselfAndOwnsByPodId() {
        Peer restarted = new Peer("pod-a1", "10.0.1.99:9000", "az-a");
        Membership here = new StaticMembership(restarted, FLEET);

        assertThat(here.self()).isEqualTo(restarted);
        assertThat(here.localAz().peers())
                .as("the fleet's entry, not self -- so every pod in the AZ sees the same list")
                .containsExactlyInAnyOrder(A1, A2);
        assertThat(here.localAz().peers()).doesNotContain(restarted);

        Peer owner = PeerRing.ownerOf("seg/2026/09/10/0", here.localAz()).orElseThrow();
        assertThat(owner.podId())
                .as("ownership is decided on podId, which survives the address change")
                .isEqualTo(restarted.podId());
        assertThat(owner).isNotEqualTo(restarted);
    }

    /**
     * Two pods compute the SAME owner while the list is stale about one of
     * them.
     *
     * <p>⚠️ THIS IS THE PROPERTY THE RING EXISTS FOR, and an earlier version of
     * this class broke it: with self substituted into its own view, review
     * MEASURED two pods disagreeing on 96 of 200 segments where the correct
     * answer is 0 -- and no assertion here could see it, because the only one
     * touching that view counted its size.
     */
    @Test
    void TWOPodsAGREEOnTheOwnerEvenWhenTheListIsStaleAboutOne() {
        Membership restarted =
                new StaticMembership(new Peer("pod-a1", "10.0.1.99:9000", "az-a"), FLEET);
        Membership neighbour = new StaticMembership(A2, FLEET);

        for (int i = 0; i < 200; i++) {
            String segment = "seg/" + i;
            assertThat(PeerRing.ownerOf(segment, restarted.localAz()))
                    .as("a stale entry for one pod must not split the AZ's view of %s", segment)
                    .isEqualTo(PeerRing.ownerOf(segment, neighbour.localAz()));
        }
    }

    /**
     * A pod the fleet has not caught up with is NOT refused.
     *
     * <p>⚠️ REFUSING CRASH-LOOPS A SCALE-UP REPLICA, which review measured
     * against an earlier version. A membership disagreement costs an extra GET,
     * never correctness (ADR-0012, ADR-0040): this pod does not appear in
     * anyone's ring -- its own included -- so it prefetches nothing and its
     * consumers fall to the object store, the ladder's last rung.
     */
    @Test
    void aPodTheFLEETHasNotCaughtUpWithIsNotRefused() {
        Peer scaledUp = new Peer("pod-a9", "10.0.9.9:9000", "az-a");
        Membership here = new StaticMembership(scaledUp, FLEET);

        assertThat(here.self()).isEqualTo(scaledUp);
        assertThat(here.localAz().peers()).containsExactlyInAnyOrder(A1, A2);
        assertThat(PeerRing.ownerOf("seg/0", here.localAz()).orElseThrow().podId())
                .as("it owns nothing, and the AZ still has an owner")
                .isNotEqualTo(scaledUp.podId());
    }

    /**
     * A pod in an AZ the fleet does not mention gets an EMPTY view, and that
     * is a ladder rung rather than an error.
     *
     * <p>⚠️ THIS COVERAGE WAS LOST AND PUT BACK. M5.8's
     * {@code anAZSCOPEDViewHoldsONLYThatAZSPeers} asserted
     * {@code inAz("az-c").isEmpty()} -- the only assertion in the tree ever
     * touching an empty AZ view -- and it went out with the query it used, on
     * my claim that the replacement was equivalent. Review MEASURED two wrong
     * implementations passing every remaining case: throwing here, which is the
     * crash-loop the two cases above forbid, in the configuration where it is
     * likeliest (the first pod of a new AZ); and falling back to the fleet's
     * first AZ, which hands this pod another zone's peers and restores
     * ADR-0012's 419x.
     */
    @Test
    void aPodInAnAZTheFleetDoesNotMentionGetsANEMPTYView() {
        Peer lonely = new Peer("pod-c1", "10.0.3.1:9000", "az-c");
        Membership here = new StaticMembership(lonely, FLEET);

        assertThat(here.localAz().az()).isEqualTo("az-c");
        assertThat(here.localAz().peers())
                .as("no peers here, and no borrowing from another zone")
                .isEmpty();
        assertThat(PeerRing.ownerOf("seg/0", here.localAz()))
                .as("no ring owner, which sends the fetch to the object store -- the last rung")
                .isEmpty();
    }

    /**
     * A pod whose AZ the fleet disagrees with follows ITS OWN az for scoping.
     *
     * <p>⚠️ NOT REFUSED, and this was decided rather than defaulted. An earlier
     * version threw, and review measured the premise wrong: membership here IS
     * configuration, so the FLEET is the stale side, and refusing kills a
     * correctly-running pod on the say-so of a hand-maintained list. The pod
     * scopes to the AZ it is actually in and finds whoever the list places
     * there.
     */
    @Test
    void aPodWhoseAZTheFleetDisagreesWithScopesToItsOWNAz() {
        Peer movedZone = new Peer("pod-a1", "10.0.1.1:9000", "az-b");
        Membership here = new StaticMembership(movedZone, FLEET);

        assertThat(here.localAz().az()).isEqualTo("az-b");
        assertThat(here.localAz().peers())
                .as("it fetches within the zone it is in, never across")
                .containsExactly(B1);
    }
}
