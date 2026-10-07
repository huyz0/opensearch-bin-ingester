// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.CrossAzBytes;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.http.EndpointSliceView;
import io.github.huyz0.os.biningester.sequencer.EpochFence;
import io.github.huyz0.os.biningester.sequencer.FastFrameRouter;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * The node's fast route refuses a fence raise from a UID its membership view
 * does not list (M13.71), and judges nothing it has no evidence for.
 */
class FastPeerLivenessTest {

    private static String member(String pod) {
        return "{\"type\":\"ADDED\",\"object\":{\"kind\":\"EndpointSlice\","
                + "\"metadata\":{\"name\":\"s1\"},\"endpoints\":[{\"addresses\":[\"10.0.0.1\"],"
                + "\"conditions\":{\"ready\":true},\"targetRef\":{\"kind\":\"Pod\",\"name\":\""
                + pod + "\",\"uid\":\"uid-" + pod + "\"}}]}}";
    }

    @Test
    void aViewThatListsOthersRefusesAREPLACEDPodsRaise() throws Exception {
        EndpointSliceView view = new EndpointSliceView();
        view.apply(member("pod-2"));
        EpochFence fence = EpochFence.start(new MemoryBinStore(), "k", Optional.empty());
        fence.raise(3);
        FastFrameRouter router = FastPeer.router("uid-self", fence, CrossAzBytes.untracked(),
                new PeerZones(), view);
        router.handle(FastFrame.KIND_JOIN, (header, body) ->
                new FastFrame.Refused(FastFrame.Reason.NOT_FAST, Optional.empty(), ""));

        router.answer(FastFrame.encode(Long.MAX_VALUE, "uid-pod-1", "uid-self",
                new FastFrame.Join(new Roster.Incarnation("pod1", "uid-pod-1", "az-a", ""),
                        FastFrame.Held.NONE)));

        assertThat(fence.highest()).as("⚠️ A LEAKED KEY FOR A GONE POD MOVES NOTHING")
                .isEqualTo(3);
    }

    @Test
    void noEVIDENCEJudgesNothing() {
        assertThat(FastPeer.liveness(null).live("uid-any"))
                .as("no membership configured").isTrue();
        assertThat(FastPeer.liveness(new EndpointSliceView()).live("uid-any"))
                .as("⚠️ A VIEW NOT YET SYNCED LISTS NOBODY, AND MUST NOT FENCE EVERYONE OUT")
                .isTrue();
        EndpointSliceView view = new EndpointSliceView();
        view.apply(member("pod-2"));
        assertThat(FastPeer.liveness(view).live("uid-pod-2")).isTrue();
        assertThat(FastPeer.liveness(view).live("uid-pod-1")).isFalse();
    }
}
