// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastTakeoverFrame;
import io.github.huyz0.os.biningester.format.Roster;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A takeover's last step (M13.33i; ADR-0081 §5 step 7): each holder of the
 * unclosed terms sent one RELEASE of every decided stream, and each term
 * closed, oldest first, once every stream it recorded is decided -- never
 * past the first that is not (invariant a).
 */
class TakeoverCloseTest {

    private static final RunKey S = new RunKey(new UUID(1, 1), 0);
    private static final RunKey T = new RunKey(new UUID(2, 2), 0);

    /** Roster {@code epoch}, fenced by 7, recording S's index, led by {@code leader}. */
    private static Roster recording(long epoch, String leader, Roster.Member... others) {
        Roster.Incarnation l = FastTermStartTest.incarnation(leader);
        List<Roster.Member> members = new ArrayList<>();
        members.add(new Roster.Member(l, Roster.State.ROSTERED));
        members.addAll(List.of(others));
        return new Roster(epoch, epoch - 1, l, members,
                List.of(new Roster.TermRecord(0, new TreeMap<>(Map.of(S.indexId(), 2)))),
                List.of(), 0, 7, false);
    }

    @Test
    void decidedTERMSCloseOldestFirstStoppingAtTheFirstUndecided() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Roster five = recording(5, "a");
        Roster six = recording(6, "b");
        FastTermStartTest.put(store, five);
        FastTermStartTest.put(store, six);

        int closed = new EmptyTermCloser(store, "p").close(7, List.of(six, five),
                r -> r.epoch() == 5);

        assertThat(closed).isEqualTo(1);
        assertThat(FastTermStartTest.read(store, 5).closed()).isTrue();
        assertThat(FastTermStartTest.read(store, 6).closed()).isFalse();
    }

    @Test
    void anUNDECIDEDOldestTermKeepsEveryLaterOneOpen() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Roster five = recording(5, "a");
        Roster six = recording(6, "b");
        FastTermStartTest.put(store, five);
        FastTermStartTest.put(store, six);

        int closed = new EmptyTermCloser(store, "p").close(7, List.of(six, five),
                r -> r.epoch() == 6);

        assertThat(closed).isZero();
        assertThat(FastTermStartTest.read(store, 6).closed()).isFalse();
    }

    @Test
    void theTWOArgumentCloseStillClosesOnlyWhatRecordedNothing() throws Exception {
        MemoryBinStore store = new MemoryBinStore();
        Roster five = recording(5, "a");
        FastTermStartTest.put(store, five);

        assertThat(new EmptyTermCloser(store, "p").close(7, List.of(five))).isZero();
    }

    /** Records every frame sent; refuses or fails the listed targets. */
    private static final class Holders implements TermJoiner.Transport {
        final List<FastFrame.Frame> sent = new ArrayList<>();
        Set<String> refusing = Set.of();
        Set<String> failing = Set.of();
        /** Holders that miss only the FIRST frame sent them. */
        Set<String> missingFirst = Set.of();

        @Override
        public byte[] exchange(String endpoint, byte[] frame) throws IOException {
            FastFrame.Frame f = FastFrame.decode(frame);
            String uid = f.header().targetUid();
            boolean first = sent.stream().noneMatch(s -> s.header().targetUid().equals(uid));
            sent.add(f);
            if (failing.contains(uid) || first && missingFirst.contains(uid)) {
                throw new IOException("unreachable");
            }
            FastFrame.Body answer = refusing.contains(uid)
                    ? new FastFrame.Refused(FastFrame.Reason.NOT_ROSTERED,
                            java.util.Optional.empty(), "no")
                    : new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE);
            return FastFrame.encode(7, uid, "uid-self", answer);
        }
    }

    @Test
    void eachHOLDERIsSentOneReleaseOfEveryDecidedStream() throws Exception {
        Roster.Member m = new Roster.Member(FastTermStartTest.incarnation("m"),
                Roster.State.ROSTERED);
        Roster.Member gone = new Roster.Member(FastTermStartTest.incarnation("d"),
                Roster.State.DEPARTED);
        Roster.Member self = new Roster.Member(FastTermStartTest.SELF, Roster.State.ROSTERED);
        Holders holders = new Holders();

        int acked = new ReleaseSender("uid-self", 7, holders).send(List.of(
                recording(6, "b", m, self), recording(5, "a", m, gone)),
                Map.of(T, 4L, S, 12L));

        assertThat(acked).isEqualTo(3);
        assertThat(holders.sent).extracting(f -> f.header().targetUid())
                .as("each once; the departed and the successor not")
                .containsExactlyInAnyOrder("uid-b", "uid-m", "uid-a");
        assertThat(holders.sent).allSatisfy(f -> {
            assertThat(f.header().epoch()).isEqualTo(7);
            assertThat(f.body()).isEqualTo(new FastTakeoverFrame.Release(List.of(
                    new FastTakeoverFrame.ReleaseAt(S, 12), new FastTakeoverFrame.ReleaseAt(T, 4))));
        });
    }

    @Test
    void aRELEASETooLongForOneFrameIsSplitAcrossFrames() throws Exception {
        // ⚠️ M13.33q: one RELEASE carries at most `perFrame` streams.
        Roster.Member m = new Roster.Member(FastTermStartTest.incarnation("m"),
                Roster.State.ROSTERED);
        RunKey u = new RunKey(new UUID(3, 3), 0);
        Holders holders = new Holders();

        int acked = new ReleaseSender("uid-self", 7, holders, 2).send(
                List.of(recording(6, "b", m)), Map.of(T, 4L, S, 12L, u, 1L));

        assertThat(acked).as("each holder answered every frame").isEqualTo(2);
        for (String holder : List.of("uid-b", "uid-m")) {
            assertThat(holders.sent).filteredOn(f -> f.header().targetUid().equals(holder))
                    .flatExtracting(f -> ((FastTakeoverFrame.Release) f.body()).streams())
                    .as(holder + ": two frames, every stream once")
                    .containsExactly(new FastTakeoverFrame.ReleaseAt(S, 12),
                            new FastTakeoverFrame.ReleaseAt(T, 4),
                            new FastTakeoverFrame.ReleaseAt(u, 1));
        }
        assertThat(holders.sent).hasSize(4);
    }

    @Test
    void aHOLDERThatMissedAnEarlierFrameIsNotAcknowledged() throws Exception {
        // ⚠️ M13.33q review round 1, T1: the LAST frame answered is not enough.
        Roster.Member m = new Roster.Member(FastTermStartTest.incarnation("m"),
                Roster.State.ROSTERED);
        Holders holders = new Holders();
        holders.missingFirst = Set.of("uid-m");

        int acked = new ReleaseSender("uid-self", 7, holders, 1).send(
                List.of(recording(6, "b", m)), Map.of(T, 4L, S, 12L));

        assertThat(acked).as("b answered both frames; m only the second").isEqualTo(1);
    }

    @Test
    void anEMPTYReleaseIsSentToNobodyAndAcknowledgedByNobody() throws Exception {
        Roster.Member m = new Roster.Member(FastTermStartTest.incarnation("m"),
                Roster.State.ROSTERED);
        Holders holders = new Holders();

        int acked = new ReleaseSender("uid-self", 7, holders).send(
                List.of(recording(6, "b", m)), Map.of());

        assertThat(acked).isZero();
        assertThat(holders.sent).isEmpty();
    }

    @Test
    void aHOLDERThatFailsOrRefusesIsCountedNeverThrown() throws Exception {
        // ⚠️ a missed RELEASE costs one HELD report (§5 step 7)
        Roster.Member m = new Roster.Member(FastTermStartTest.incarnation("m"),
                Roster.State.ROSTERED);
        Holders holders = new Holders();
        holders.failing = Set.of("uid-m");
        holders.refusing = Set.of("uid-a");

        int acked = new ReleaseSender("uid-self", 7, holders).send(
                List.of(recording(5, "a", m)), Map.of(S, 12L));

        assertThat(acked).isZero();
        assertThat(holders.sent).hasSize(2);
    }
}
