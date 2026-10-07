// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.http;

import static io.github.huyz0.os.biningester.http.EndpointSliceViewTest.empty;
import static io.github.huyz0.os.biningester.http.EndpointSliceViewTest.event;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Whether the view lists a pod incarnation, ready or not (M13.71): the live
 * set a fast frame's sender is checked against before it may raise a fence.
 */
class EndpointSliceViewUidTest {

    @Test
    void aMemberNOTREADYIsStillListed() {
        EndpointSliceView view = new EndpointSliceView();
        view.apply(event("ADDED", "pod-1", "10.0.0.1", false, null));

        assertThat(view.lists("uid-pod-1"))
                .as("⚠️ A STARTING POD's JOIN COMES BEFORE IT IS READY").isTrue();
        assertThat(view.lists("uid-pod-9")).isFalse();
        assertThat(view.hasMembers()).isTrue();
    }

    @Test
    void aREPLACEDIncarnationIsNotListed() {
        EndpointSliceView view = new EndpointSliceView();
        view.apply(event("ADDED", "pod-1", "10.0.0.1", true, null));
        view.apply(event("MODIFIED", "pod-2", "10.0.0.2", true, null));

        assertThat(view.lists("uid-pod-1")).as("its slice entry was replaced").isFalse();
        assertThat(view.lists("uid-pod-2")).isTrue();
    }

    @Test
    void anEMPTYViewListsNothingAndSaysSo() {
        EndpointSliceView view = new EndpointSliceView();
        assertThat(view.hasMembers()).isFalse();
        view.apply(event("ADDED", "pod-1", "10.0.0.1", true, null));
        view.apply(empty("MODIFIED"));
        assertThat(view.hasMembers()).isFalse();
        assertThat(view.lists("uid-pod-1")).isFalse();
    }
}
