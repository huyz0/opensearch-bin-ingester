// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.ALL;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.B;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.S2;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.commit;
import static io.github.huyz0.os.biningester.sequencer.FastWriteLeaderTest.roster;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastWriteFrame;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.sequencer.QuorumFrontier.EntryId;
import io.github.huyz0.os.biningester.sequencer.QuorumFrontier.Holder;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * An upload asked for on the loss of a holder of an unreleased entry
 * (ADR-0081 §7; M13.31e): at once, before the {@code flush_timer}, when the
 * lost pod holds a copy the chain has not committed -- and not for a pod
 * holding none, nor once the chain committed what it held.
 */
class CommitDeskHolderLostTest {

    private static final Duration FIVE_SECONDS = Duration.ofSeconds(5);
    private static final long SECOND = 1_000_000_000L;

    @Test
    void theLOSSOfAHolderOfAnUnreleasedEntryIsDueAtOnce() throws Exception {
        FastWriteLeader leader = FastWriteLeaderTest.leader(1 << 20, 1_000).leader();
        leader.commit(B, commit(1, S2), roster(), ALL);
        leader.flushGroup();
        leader.confirm(B, new FastWriteFrame.Confirm(commit(1, S2).key(), 3,
                List.of(new FastWriteFrame.RunAt(S2, 0)), true));
        CommitDesk desk = new CommitDesk(leader, stream -> 0L, Duration.ofSeconds(1));
        desk.uploaded(SECOND);
        assertThat(desk.uploadDue(2 * SECOND, FIVE_SECONDS)).as("the premise").isFalse();

        assertThat(desk.holderLost("uid-stranger")).as("holds nothing").isFalse();
        assertThat(desk.uploadDue(2 * SECOND, FIVE_SECONDS)).isFalse();
        assertThat(desk.holderLost(B.podUid())).isTrue();

        assertThat(desk.uploadDue(2 * SECOND, FIVE_SECONDS)).as("before the flush timer")
                .isTrue();
    }

    @Test
    void aCOPYTheChainCommittedIsHeldByNoOne() {
        RunKey a = new RunKey(new UUID(1, 1), 0);
        QuorumFrontier f = new QuorumFrontier("uid-l", "az-a");
        f.open(a, 0);
        EntryId first = new EntryId(7, 0, a, 0);
        EntryId second = new EntryId(7, 0, a, 5);
        f.assigned(first, 5, 2);
        f.assigned(second, 5, 2);
        f.copied(first, new Holder("uid-b", "az-b"));
        f.copied(second, new Holder("uid-c", "az-c"));
        f.expose();

        assertThat(f.holders()).as("exposed or not, the leader aside")
                .containsExactly("uid-b", "uid-c");

        f.committed(a, 5);

        assertThat(f.holders()).as("[0, 5) released").containsExactly("uid-c");
    }
}
