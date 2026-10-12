// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static io.github.huyz0.os.biningester.sequencer.TakeoverCollectionTest.entry;
import static io.github.huyz0.os.biningester.sequencer.TakeoverCollectionTest.incarnation;
import static io.github.huyz0.os.biningester.sequencer.TakeoverCollectionTest.roster;
import static io.github.huyz0.os.biningester.sequencer.TakeoverCollectionTest.rostered;
import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastTakeoverFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The collection's edges (M13.33d review round 1, T1-T3, P1): an answer
 * addressed to another pod, an entry exactly at a decision's resume offset,
 * a member silent between two of its pages, and a pod departed in a later
 * unclosed term.
 */
class TakeoverCollectionEdgesTest {

    private static final Roster.Incarnation M = incarnation("m");
    private static final Roster.Incarnation N = incarnation("n");
    private static final RunKey S = new RunKey(new UUID(1, 1), 0);

    /** Answers {@code m}'s two pages; scripted per call. */
    private static final class Script implements TermJoiner.Transport {
        final List<String> calls = new ArrayList<>();
        String target = "uid-s";
        int silentOnCall = -1;
        List<List<FastJournalRecord.Entry>> pages =
                List.of(List.of(entry(7, 1)), List.of(entry(7, 2)));

        @Override
        public byte[] exchange(String endpoint, byte[] frame) throws IOException {
            FastFrame.Frame f = FastFrame.decode(frame);
            FastTakeoverFrame.Fence fence = (FastTakeoverFrame.Fence) f.body();
            calls.add(f.header().targetUid() + "#" + fence.page());
            if (calls.size() == silentOnCall) {
                throw new IOException("silent mid-answer");
            }
            int p = (int) fence.page();
            return FastFrame.encode(9, f.header().targetUid(), target,
                    new FastTakeoverFrame.Collected(fence.page(), p == pages.size() - 1,
                            pages.get(p)));
        }
    }

    private static TakeoverCollection collecting(Script script) {
        return new TakeoverCollection("uid-s", 9, script, PodGone.NEVER, nanos -> { },
                Duration.ofSeconds(1));
    }

    @Test
    void anANSWERAddressedToAnotherPodDoesNotCount() throws Exception {
        Script script = new Script();
        script.target = "uid-x";

        assertThat(collecting(script).collect(List.of(roster(7, M, List.of(), rostered(M))), 6, 1)
                .waiting()).containsExactly("uid-m");
    }

    @Test
    void anENTRYExactlyAtADecisionsResumeOffsetIsSuperseded() throws Exception {
        Script script = new Script();
        script.pages = List.of(List.of(entry(7, 9), entry(7, 10)));
        List<Roster> open = List.of(roster(8, N, List.of(new Roster.Decision(0, S, 10)),
                rostered(N)), roster(7, M, List.of(), rostered(M)));

        TakeoverCollection.Collected got = collecting(script).collect(open, 6, 1);

        assertThat(got.byMember().get("uid-m")).extracting(FastJournalRecord.Entry::firstOffset)
                .containsExactly(9L);
    }

    @Test
    void aMEMBERSilentBetweenItsPagesGivesNoHalfAnswerAndNoDuplicateLater() throws Exception {
        Script script = new Script();
        script.silentOnCall = 2;

        TakeoverCollection.Collected first = collecting(script)
                .collect(List.of(roster(7, M, List.of(), rostered(M))), 6, 1);
        assertThat(first.byMember()).as("page 0 alone is no answer").doesNotContainKey("uid-m");
        assertThat(first.waiting()).containsExactly("uid-m");

        Script again = new Script();
        again.silentOnCall = 2;
        TakeoverCollection.Collected retried = collecting(again)
                .collect(List.of(roster(7, M, List.of(), rostered(M))), 6, 2);
        assertThat(retried.byMember().get("uid-m")).extracting(FastJournalRecord.Entry::firstOffset)
                .as("asked again from page 0, each page once").containsExactly(1L, 2L);
    }

    @Test
    void aPODDepartedInALaterUnclosedTermIsNotAsked() throws Exception {
        Script script = new Script();
        List<Roster> open = List.of(
                roster(8, N, List.of(), rostered(N), new Roster.Member(M, Roster.State.DEPARTED)),
                roster(7, M, List.of(), rostered(M)));

        TakeoverCollection.Collected got = collecting(script).collect(open, 6, 1);

        assertThat(script.calls).as("only n is asked").allMatch(c -> c.startsWith("uid-n"));
        assertThat(got.waiting()).doesNotContain("uid-m");
    }
}
