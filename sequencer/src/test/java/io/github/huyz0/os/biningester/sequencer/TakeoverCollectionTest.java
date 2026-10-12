// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastJournalRecord;
import io.github.huyz0.os.biningester.format.FastTakeoverFrame;
import io.github.huyz0.os.biningester.format.OpType;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SegmentRecord;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The successor collects what the members of every unclosed term hold
 * (M13.33d; ADR-0081 §5 step 4): a FENCE to every ROSTERED member, each
 * member's pages asked in order and counted only once its last arrives; a
 * member that does not answer retried until it answers or is GONE; and
 * entries a walked roster supersedes dropped.
 */
class TakeoverCollectionTest {

    private static final RunKey S = new RunKey(new UUID(1, 1), 0);
    private static final Roster.Incarnation SELF = incarnation("s");
    private static final Roster.Incarnation L = incarnation("l");
    private static final Roster.Incarnation M = incarnation("m");
    private static final Roster.Incarnation D = incarnation("d");
    private static final Roster.Incarnation N = incarnation("n");

    static Roster.Incarnation incarnation(String name) {
        return new Roster.Incarnation(name, "uid-" + name, "az-" + name, "http://" + name + ":1");
    }

    static FastJournalRecord.Entry entry(long epoch, long first) {
        return new FastJournalRecord.Entry(epoch, S, first, 2, 0,
                new FastJournalRecord.IdempotencyKey("w", new UUID(9, 9), first),
                List.of(new SegmentRecord("d" + first, OpType.INDEX, OptionalLong.empty(),
                        new byte[] {1})));
    }

    static Roster roster(long epoch, Roster.Incarnation leader, List<Roster.Decision> decisions,
            Roster.Member... members) {
        return new Roster(epoch, epoch - 1, leader, List.of(members),
                List.of(new Roster.TermRecord(0, new TreeMap<>(Map.of(S.indexId(), 2)))),
                decisions, 0, 0, false);
    }

    static Roster.Member rostered(Roster.Incarnation pod) {
        return new Roster.Member(pod, Roster.State.ROSTERED);
    }

    /** Answers each member's pages from {@code pages}; fails a member listed in {@code silent}. */
    static final class Members implements TermJoiner.Transport {
        final Map<String, List<List<FastJournalRecord.Entry>>> pages = new HashMap<>();
        final Map<String, Integer> silentFor = new HashMap<>();
        final List<String> asked = new ArrayList<>();
        final List<FastTakeoverFrame.Fence> fences = new ArrayList<>();
        String answerAs;
        long answerEpoch = 9;
        long pageShift;

        @Override
        public byte[] exchange(String endpoint, byte[] frame) throws IOException {
            FastFrame.Frame f = FastFrame.decode(frame);
            String uid = f.header().targetUid();
            FastTakeoverFrame.Fence fence = (FastTakeoverFrame.Fence) f.body();
            asked.add(uid + "#" + fence.page());
            fences.add(fence);
            int silent = silentFor.getOrDefault(uid, 0);
            if (silent != 0) {
                silentFor.put(uid, silent - 1);
                throw new IOException(uid + " does not answer");
            }
            List<List<FastJournalRecord.Entry>> mine = pages.getOrDefault(uid, List.of(List.of()));
            int p = (int) fence.page();
            return FastFrame.encode(answerEpoch, answerAs != null ? answerAs : uid, "uid-s",
                    new FastTakeoverFrame.Collected(fence.page() + pageShift,
                            p == mine.size() - 1, mine.get(p)));
        }
    }

    private static TakeoverCollection collection(Members members, PodGone gone, int[] sleeps) {
        return new TakeoverCollection(SELF.podUid(), 9, members, gone,
                nanos -> sleeps[0]++, java.time.Duration.ofSeconds(1));
    }

    private static List<Long> offsets(List<FastJournalRecord.Entry> entries) {
        return entries.stream().map(FastJournalRecord.Entry::firstOffset).toList();
    }

    @Test
    void everyROSTEREDMemberOfEveryUnclosedTermIsFencedAndItsPagesCollected() throws Exception {
        Members members = new Members();
        members.pages.put("uid-m", List.of(List.of(entry(7, 10)), List.of(entry(8, 20))));
        members.pages.put("uid-l", List.of(List.of(entry(7, 11))));
        List<Roster> open = List.of(
                roster(8, N, List.of(), rostered(N), rostered(M), rostered(SELF)),
                roster(7, L, List.of(), rostered(L), rostered(M),
                        new Roster.Member(D, Roster.State.DEPARTED)));

        TakeoverCollection.Collected got = collection(members, PodGone.NEVER, new int[1])
                .collect(open, 6, 3);

        assertThat(offsets(got.byMember().get("uid-m"))).containsExactly(10L, 20L);
        assertThat(offsets(got.byMember().get("uid-l"))).containsExactly(11L);
        assertThat(got.byMember().get("uid-n")).isEmpty();
        assertThat(got.waiting()).isEmpty();
        assertThat(got.gone()).isEmpty();
        assertThat(members.asked).as("the departed and the successor itself are not asked")
                .containsExactlyInAnyOrder("uid-n#0", "uid-m#0", "uid-m#1", "uid-l#0");
        assertThat(members.fences).allSatisfy(f -> {
            assertThat(f.unclosedEpochs()).containsExactly(7L, 8L);
            assertThat(f.closedThrough()).isEqualTo(6);
        });
    }

    @Test
    void anENTRYAWalkedRosterSupersedesIsDropped() throws Exception {
        Members members = new Members();
        members.pages.put("uid-m", List.of(List.of(entry(7, 5), entry(7, 12))));
        // term 8's decision on S resumes at 10: term 7's entry at 12 is superseded
        List<Roster> open = List.of(roster(8, N, List.of(new Roster.Decision(0, S, 10)),
                rostered(N)), roster(7, M, List.of(), rostered(M)));

        TakeoverCollection.Collected got = collection(members, PodGone.NEVER, new int[1])
                .collect(open, 6, 1);

        assertThat(offsets(got.byMember().get("uid-m"))).containsExactly(5L);
    }

    @Test
    void aSILENTMemberIsRetriedUntilItAnswers() throws Exception {
        Members members = new Members();
        members.silentFor.put("uid-m", 1);
        int[] sleeps = {0};

        TakeoverCollection.Collected got = collection(members, PodGone.NEVER, sleeps)
                .collect(List.of(roster(7, M, List.of(), rostered(M))), 6, 3);

        assertThat(got.byMember()).containsKey("uid-m");
        assertThat(got.waiting()).isEmpty();
        assertThat(sleeps[0]).isEqualTo(1);
    }

    @Test
    void aSILENTMemberWhosePodIsDeletedIsGoneAndNotAskedAgain() throws Exception {
        Members members = new Members();
        members.silentFor.put("uid-m", 99);

        TakeoverCollection.Collected got = collection(members, pod -> true, new int[1])
                .collect(List.of(roster(7, M, List.of(), rostered(M))), 6, 3);

        assertThat(got.gone()).containsExactly("uid-m");
        assertThat(got.waiting()).isEmpty();
        assertThat(members.asked).hasSize(1);
    }

    @Test
    void aSILENTLiveMemberIsStillWaitingAfterTheRounds() throws Exception {
        Members members = new Members();
        members.silentFor.put("uid-m", 99);
        int[] sleeps = {0};

        TakeoverCollection.Collected got = collection(members, PodGone.NEVER, sleeps)
                .collect(List.of(roster(7, M, List.of(), rostered(M))), 6, 3);

        assertThat(got.waiting()).containsExactly("uid-m");
        assertThat(got.byMember()).doesNotContainKey("uid-m");
        assertThat(members.asked).hasSize(3);
        assertThat(sleeps[0]).as("between rounds, not after the last").isEqualTo(2);
    }

    @Test
    void aPODLookupWithoutAnAnswerLeavesItWaiting() throws Exception {
        Members members = new Members();
        members.silentFor.put("uid-m", 99);

        TakeoverCollection.Collected got = collection(members, pod -> {
            throw new IOException("the API server is down");
        }, new int[1]).collect(List.of(roster(7, M, List.of(), rostered(M))), 6, 2);

        assertThat(got.waiting()).containsExactly("uid-m");
        assertThat(got.gone()).isEmpty();
    }

    @Test
    void anANSWERFromAnotherPodTermOrPageDoesNotCount() throws Exception {
        List<Roster> open = List.of(roster(7, M, List.of(), rostered(M)));
        Members stranger = new Members();
        stranger.answerAs = "uid-x";
        assertThat(collection(stranger, PodGone.NEVER, new int[1]).collect(open, 6, 1).waiting())
                .containsExactly("uid-m");

        Members otherTerm = new Members();
        otherTerm.answerEpoch = 8;
        assertThat(collection(otherTerm, PodGone.NEVER, new int[1]).collect(open, 6, 1).waiting())
                .containsExactly("uid-m");

        Members otherPage = new Members();
        otherPage.pageShift = 1;
        assertThat(collection(otherPage, PodGone.NEVER, new int[1]).collect(open, 6, 1).waiting())
                .containsExactly("uid-m");
    }
}
