// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.sequencer;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.Lease;
import io.github.huyz0.os.biningester.format.Roster;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Every pod learns its leader from the lease and joins each newer term led by
 * another incarnation (ADR-0081 §1, amended by M13.27n).
 */
class LeaderWatchTest {

    private static final Roster.Incarnation SELF =
            new Roster.Incarnation("me", "uid-me", "az-b", "http://me:1");

    /** A leader answering every JOIN; refusing while {@code refuse} is set. */
    static final class Leader implements TermJoiner.Transport {
        final List<String> sent = new ArrayList<>();
        boolean refuse;
        boolean down;

        @Override
        public byte[] exchange(String endpoint, byte[] frame) throws IOException {
            FastFrame.Header asked = FastFrame.header(frame);
            sent.add(endpoint + "#" + asked.epoch());
            if (down) {
                throw new IOException("unreachable");
            }
            FastFrame.Body answer = refuse
                    ? new FastFrame.Refused(FastFrame.Reason.NOT_ROSTERED, Optional.empty(), "")
                    : new FastFrame.Joined(0, FastFrame.HeldStatus.NONE);
            return FastFrame.encode(asked.epoch(), asked.targetUid(), asked.senderUid(), answer);
        }
    }

    private static final class Watched {
        Lease lease;
        boolean unreadable;
        int reads;
    }

    private static LeaderWatch watch(Watched watched, Leader leader) throws IOException {
        return watch(watched, leader, () -> false);
    }

    private static LeaderWatch watch(Watched watched, Leader leader,
            java.util.function.BooleanSupplier leading) throws IOException {
        TermJoiner joiner = new TermJoiner(SELF, leader,
                EpochFence.start(new MemoryBinStore(), "k", Optional.empty()), new JoinedTerms(),
                () -> FastFrame.Held.NONE);
        return new LeaderWatch(SELF.podUid(), () -> {
            if (watched.unreadable) {
                throw new IOException("no lease yet");
            }
            watched.reads++;
            return watched.lease;
        }, joiner, leading);
    }

    private static Lease lease(long epoch, String holder) {
        return new Lease(epoch, holder, "uid-" + holder, "http://" + holder + ":1", 1_000L);
    }

    @Test
    void aTERMLedByAnotherPodIsJoinedAtItsLeadersEndpoint() throws Exception {
        Watched watched = new Watched();
        watched.lease = lease(3, "l");
        Leader leader = new Leader();

        Optional<TermJoiner.Outcome> outcome = watch(watched, leader).look();

        assertThat(outcome).containsInstanceOf(TermJoiner.Joined.class);
        assertThat(leader.sent).containsExactly("http://l:1#3");
    }

    @Test
    void aTERMIsJoinedOnceAndANewerOneAgain() throws Exception {
        Watched watched = new Watched();
        watched.lease = lease(3, "l");
        Leader leader = new Leader();
        LeaderWatch watch = watch(watched, leader);

        watch.look();
        watch.look();
        watched.lease = lease(4, "m");
        watch.look();

        assertThat(leader.sent).containsExactly("http://l:1#3", "http://m:1#4");
    }

    @Test
    void itsOWNTermIsNotJoined() throws Exception {
        Watched watched = new Watched();
        watched.lease = lease(3, "me");
        Leader leader = new Leader();

        assertThat(watch(watched, leader).look()).isEmpty();
        assertThat(leader.sent).isEmpty();
    }

    @Test
    void aREFUSEDOrUnreachableJoinIsAskedAgainAtTheNextLook() throws Exception {
        Watched watched = new Watched();
        watched.lease = lease(3, "l");
        Leader leader = new Leader();
        LeaderWatch watch = watch(watched, leader);
        leader.refuse = true;
        watch.look();
        leader.refuse = false;
        leader.down = true;
        watch.look();
        leader.down = false;

        assertThat(watch.look()).containsInstanceOf(TermJoiner.Joined.class);
        assertThat(leader.sent).hasSize(3);
    }

    @Test
    void anUNREADABLELeaseJoinsNothing() throws Exception {
        Watched watched = new Watched();
        watched.unreadable = true;
        Leader leader = new Leader();

        assertThat(watch(watched, leader).look()).isEmpty();
        assertThat(leader.sent).isEmpty();
    }

    @Test
    void aLEGACYLeaseNamingNoUidJoinsNothing() throws Exception {
        Watched watched = new Watched();
        watched.lease = new Lease(3, "l", "", "http://l:1", 1_000L);
        Leader leader = new Leader();

        assertThat(watch(watched, leader).look()).isEmpty();
        assertThat(leader.sent).isEmpty();
    }

    @Test
    void aLEADINGPodReadsNothing() throws Exception {
        Watched watched = new Watched();
        watched.lease = lease(3, "l");
        Leader leader = new Leader();

        assertThat(watch(watched, leader, () -> true).look()).isEmpty();
        assertThat(watched.reads).as("a leader's idle cost is its renewals alone").isZero();
        assertThat(leader.sent).isEmpty();
    }
}
