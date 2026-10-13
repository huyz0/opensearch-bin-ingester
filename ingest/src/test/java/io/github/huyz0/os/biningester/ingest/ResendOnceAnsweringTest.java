// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.CommitAnswer;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.AmbiguousAppendException;
import io.github.huyz0.os.biningester.sequencer.CommitRequest;
import io.github.huyz0.os.biningester.sequencer.Sequencer;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The commit answer is asked for through the writer's resend-once decorator
 * (M13.34k): an ambiguous append resent once, as a commit's is, and the
 * answer -- its redirects with it -- handed back, never the bare delta.
 */
class ResendOnceAnsweringTest {

    private static final RunKey FAST = new RunKey(new UUID(2, 2), 0);

    @Test
    void anAMBIGUOUSAppendIsResentOnceAndItsAnswerHandedBack() throws Exception {
        CommitAnswer answer = new CommitAnswer(List.of(), List.of(FAST));
        int[] asked = {0};
        Sequencer leader = new Sequencer() {
            @Override
            public CommitDelta commitAll(List<CommitRequest> requests) {
                throw new AssertionError("committed whole, not answered");
            }

            @Override
            public CommitAnswer commitAnswering(CommitRequest request) throws IOException {
                if (asked[0]++ == 0) {
                    throw new AmbiguousAppendException(1, 3, new IOException("lost"));
                }
                return answer;
            }

            @Override
            public void close() {
            }
        };

        CommitAnswer got = new ResendOnceSequencer(leader).commitAnswering(
                new CommitRequest("pod", "i1", 0, "seg/0", Map.of(FAST, 2)));

        assertThat(got).isEqualTo(answer);
        assertThat(asked[0]).isEqualTo(2);
    }
}
