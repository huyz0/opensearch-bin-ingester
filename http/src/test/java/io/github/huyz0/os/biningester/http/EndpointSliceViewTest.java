// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.Lease;
import org.junit.jupiter.api.Test;

/**
 * What counts as evidence that a leaseholder is gone (M8.13, NFR-9).
 */
class EndpointSliceViewTest {

    private static final Lease POD0 = new Lease(3, "pod0", "http://10.0.0.1:8080",
            Long.MAX_VALUE);

    /** One watch event for slice {@code s1}, with one endpoint. */
    static String event(String type, String pod, String address, Boolean ready,
            Boolean terminating) {
        String conditions = (ready == null ? "" : "\"ready\":" + ready)
                + (terminating == null ? "" : (ready == null ? "" : ",")
                        + "\"terminating\":" + terminating);
        return "{\"type\":\"" + type + "\",\"object\":{\"kind\":\"EndpointSlice\","
                + "\"metadata\":{\"name\":\"s1\"},\"endpoints\":[{\"addresses\":[\""
                + address + "\"],\"conditions\":{" + conditions + "},\"targetRef\":"
                + "{\"kind\":\"Pod\",\"name\":\"" + pod + "\"}}]}}";
    }

    static String empty(String type) {
        return "{\"type\":\"" + type + "\",\"object\":{\"kind\":\"EndpointSlice\","
                + "\"metadata\":{\"name\":\"s1\"},\"endpoints\":[]}}";
    }

    @Test
    void readyPeerEndpointsExposeTheirAZAndExcludeDrainingNodes() {
        EndpointSliceView view = new EndpointSliceView();
        view.apply("{\"type\":\"ADDED\",\"object\":{\"metadata\":{\"name\":\"s1\"},"
                + "\"endpoints\":["
                + "{\"addresses\":[\"10.0.0.9\"],\"zone\":\"az-z\","
                + "\"conditions\":{\"ready\":true},\"targetRef\":{\"name\":\"pod-z\"}},"
                + "{\"addresses\":[\"10.0.0.1\"],\"zone\":\"az-a\","
                + "\"conditions\":{\"ready\":true},\"targetRef\":{\"name\":\"pod-a\"}},"
                + "{\"addresses\":[\"10.0.0.2\"],\"zone\":\"az-b\","
                + "\"conditions\":{\"ready\":false},\"targetRef\":{\"name\":\"pod-b\"}},"
                + "{\"addresses\":[\"10.0.0.4\"],\"zone\":\"az-d\","
                + "\"conditions\":{\"ready\":true,\"terminating\":true},"
                + "\"targetRef\":{\"name\":\"pod-d\"}},"
                + "{\"addresses\":[\"10.0.0.3\"],\"conditions\":{\"ready\":true},"
                + "\"targetRef\":{\"name\":\"pod-c\"}}]}} ");

        assertThat(view.readyEndpoints()).containsExactly(
                new EndpointSliceView.Endpoint("pod-a", "10.0.0.1", "az-a"),
                new EndpointSliceView.Endpoint("pod-z", "10.0.0.9", "az-z"));
    }

    @Test
    void aHolderNEVERSeenReadyIsNOTGone() {
        // ⚠️ No evidence either way: challenging it would flap leadership for
        // as long as the view lagged the lease.
        EndpointSliceView view = new EndpointSliceView();
        assertThat(view.holderGone(POD0)).isFalse();
        view.apply(empty("ADDED"));
        assertThat(view.holderGone(POD0)).as("absent, but never seen ready").isFalse();
    }

    @Test
    void aHolderSeenREADYAndThenREMOVEDIsGone() {
        EndpointSliceView view = new EndpointSliceView();
        view.apply(event("ADDED", "pod0", "10.0.0.1", true, false));
        assertThat(view.holderGone(POD0)).as("ready: not gone").isFalse();

        view.apply(empty("MODIFIED"));

        assertThat(view.holderGone(POD0)).as("⚠️ REMOVED FROM THE SLICE: GONE").isTrue();
    }

    @Test
    void aSliceDELETEDTakesItsMembersWithIt() {
        EndpointSliceView view = new EndpointSliceView();
        view.apply(event("ADDED", "pod0", "10.0.0.1", true, false));
        view.apply(empty("DELETED"));
        assertThat(view.holderGone(POD0)).isTrue();
    }

    @Test
    void notReadyANDNotTerminatingIsGONEButTERMINATINGIsNot() {
        // ⚠️ A crashed container, restarting, is not ready and not
        // terminating. A pod draining under research 08 §7 is terminating,
        // and it will RELEASE the lease; challenging it would fence its last
        // flush.
        EndpointSliceView view = new EndpointSliceView();
        view.apply(event("ADDED", "pod0", "10.0.0.1", true, false));

        view.apply(event("MODIFIED", "pod0", "10.0.0.1", false, true));
        assertThat(view.holderGone(POD0)).as("⚠️ TERMINATING: NOT GONE").isFalse();

        view.apply(event("MODIFIED", "pod0", "10.0.0.1", false, false));
        assertThat(view.holderGone(POD0)).as("⚠️ NOT READY, NOT TERMINATING: GONE").isTrue();

        view.apply(event("MODIFIED", "pod0", "10.0.0.1", true, false));
        assertThat(view.holderGone(POD0)).as("and ready again: not gone").isFalse();
    }

    @Test
    void anABSENTReadyConditionIsREAD() {
        // ⚠️ The EndpointSlice API reads a nil `ready` as true.
        EndpointSliceView view = new EndpointSliceView();
        view.apply(event("ADDED", "pod0", "10.0.0.1", null, null));
        assertThat(view.holderGone(POD0)).isFalse();
        view.apply(empty("MODIFIED"));
        assertThat(view.holderGone(POD0)).as("it had been seen ready").isTrue();
    }

    @Test
    void theHolderIsMatchedByPODNameORByTheADDRESSInItsEndpoint() {
        EndpointSliceView byAddress = new EndpointSliceView();
        byAddress.apply(event("ADDED", "some-other-name", "10.0.0.1", true, false));
        assertThat(byAddress.holderGone(POD0)).isFalse();
        byAddress.apply(empty("MODIFIED"));
        assertThat(byAddress.holderGone(POD0)).as("⚠️ MATCHED BY ADDRESS").isTrue();

        EndpointSliceView byName = new EndpointSliceView();
        byName.apply(event("ADDED", "pod0", "10.9.9.9", true, false));
        byName.apply(empty("MODIFIED"));
        assertThat(byName.holderGone(POD0)).as("⚠️ MATCHED BY POD NAME").isTrue();
    }

    @Test
    void anUNREADABLELineChangesNOTHINGAndSaysSo() {
        EndpointSliceView view = new EndpointSliceView();
        view.apply(event("ADDED", "pod0", "10.0.0.1", true, false));

        assertThat(view.apply("{not json")).isFalse();
        assertThat(view.apply("{\"type\":\"MODIFIED\"}")).isFalse();

        assertThat(view.holderGone(POD0)).as("the last readable state stands").isFalse();
    }

    @Test
    void aHolderSeenOnlyNOTREADYIsNOTGoneWhenItLEAVES() {
        // ⚠️ A STARTING POD, whose readiness has not passed yet, is not
        // evidence of anything: it was never up, so it cannot have gone down.
        EndpointSliceView view = new EndpointSliceView();
        view.apply(event("ADDED", "pod0", "10.0.0.1", false, false));
        assertThat(view.holderGone(POD0)).isFalse();
        view.apply(empty("MODIFIED"));
        assertThat(view.holderGone(POD0)).as("⚠️ NEVER SEEN READY: NO EVIDENCE").isFalse();
    }

    @Test
    void anADDRESSReusedByANewPodDoesNOTCarryTheDeadPodsEvidence() {
        // ⚠️ pod0 died at 10.0.0.1; podX now has that IP and holds the term,
        // and is not ready yet. Matched by address, it would inherit pod0's
        // "seen ready" and be challenged before its own readiness passed.
        EndpointSliceView view = new EndpointSliceView();
        view.apply(event("ADDED", "pod0", "10.0.0.1", true, false));
        view.apply(empty("MODIFIED"));
        view.apply(event("MODIFIED", "podX", "10.0.0.1", false, false));

        assertThat(view.holderGone(new Lease(4, "podX", "http://10.0.0.1:8080", Long.MAX_VALUE)))
                .as("⚠️ podX IS NAMED, AND WAS NEVER SEEN READY").isFalse();
    }
}
