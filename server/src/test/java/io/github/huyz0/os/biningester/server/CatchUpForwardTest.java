// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.format.RunKey;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * Where a follower forwards a catch-up (M13.32i): the newest joined term's
 * leader, else the lease's holder -- never itself, where it is not forwarded
 * at all -- with the answer's fast
 * epoch admitted only at or above this pod's fence.
 */
class CatchUpForwardTest {

    private static final CatchUpRequestFrame ASKED = new CatchUpRequestFrame(new UUID(5, 5),
            List.of(new CatchUpRequestFrame.Stream(new RunKey(new UUID(1, 1), 0), 0)));

    private static FastDisk disk() throws Exception {
        return FastDisk.open(new MemoryBinStore(), "lease", Optional.empty(),
                new FastDiskTest.Files());
    }

    @Test
    void theJOINEDTermsLeaderIsAskedItsAnswerAdmittedFromThisPodsFence() throws Exception {
        try (FastDisk disk = disk()) {
            disk.joined().joined(4, 0, "uid-l", "l:1");
            disk.fence().admit(4);
            List<String> sent = new ArrayList<>();
            List<Boolean> admitted = new ArrayList<>();

            CatchUpForward.of("uid-me", disk, () -> new Lease(9, "x", "uid-x", "x:1", 0),
                    (endpoint, request, sink, admit) -> {
                        sent.add(endpoint);
                        admitted.add(admit.test(3));
                        admitted.add(admit.test(4));
                    }).forwarded(ASKED, frame -> { });

            assertThat(sent).as("in memory, not the lease").containsExactly("l:1");
            assertThat(admitted).as("below the fence, then at it").containsExactly(false, true);
        }
    }

    @Test
    void withNOJoinedTermTheLeasesHolderIsAskedNeverThisPod() throws Exception {
        try (FastDisk disk = disk()) {
            List<String> sent = new ArrayList<>();
            CatchUpForward.Send recording = (endpoint, request, sink, admit) -> sent.add(endpoint);

            assertThat(CatchUpForward.of("uid-me", disk,
                    () -> new Lease(9, "l", "uid-l", "l:9", 0), recording)
                    .forwarded(ASKED, frame -> { })).isTrue();
            assertThat(CatchUpForward.of("uid-me", disk,
                    () -> new Lease(9, "me", "uid-me", "me:1", 0), recording)
                    .forwarded(ASKED, frame -> { })).as("this pod answers itself").isFalse();

            assertThat(sent).as("never to itself").containsExactly("l:9");
        }
    }
}
