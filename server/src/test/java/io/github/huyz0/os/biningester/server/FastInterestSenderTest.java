// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.FastPublishFrame;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import io.github.huyz0.os.biningester.sequencer.JoinedTerms;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * A pod tells its leader which fast streams it has subscribers on (M13.30g;
 * ADR-0081 §6): as an INTEREST to the leader of the newest term it joined,
 * naming what changed since that leader took it -- in full to a new leader,
 * again after a miss once the retry interval passes, never when nothing
 * changed, and never by the pod that leads.
 */
class FastInterestSenderTest {

    private static final long PAST_RETRY = FastInterestSender.RETRY_INTERVAL.toNanos();
    private static final String ORDERS_UUID = "nVzgup36TLqWp7VBBREj1w";
    private static final String LOGS_UUID = "Qk1rUnfdRyCrk1L0DLaXEg";
    private static final RunKey ORDERS_0 = RunKey.ofIndexUuid(ORDERS_UUID, 0);
    private static final RunKey ORDERS_1 = RunKey.ofIndexUuid(ORDERS_UUID, 1);
    private static final RunKey LOGS_0 = RunKey.ofIndexUuid(LOGS_UUID, 0);

    /** The monotonic clock the sender reads. */
    private final long[] now = {0};

    /** One INTEREST as it went out. */
    private record Sent(String endpoint, FastFrame.Header header,
            FastPublishFrame.Interest what) {
    }

    private static final class Leader {
        final List<Sent> sent = new ArrayList<>();
        boolean failing;
        boolean refusing;

        byte[] exchange(String endpoint, byte[] frame) throws IOException {
            FastFrame.Frame f = FastFrame.decode(frame);
            sent.add(new Sent(endpoint, f.header(), (FastPublishFrame.Interest) f.body()));
            if (failing) {
                throw new IOException("the leader did not answer");
            }
            FastFrame.Body answer = refusing
                    ? new FastFrame.Refused(FastFrame.Reason.NOT_ROSTERED, Optional.empty(), "no")
                    : new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE);
            return FastFrame.encode(f.header().epoch(), "uid-l", "uid-w", answer);
        }
    }

    private static JoinedTerms joined(long epoch) {
        JoinedTerms joined = new JoinedTerms();
        joined.joined(epoch, 0, "uid-l", "l:1");
        return joined;
    }

    private FastInterestSender sender(Leader leader) {
        return new FastInterestSender("uid-w", leader::exchange, () -> now[0]);
    }

    @Test
    void onlyTheFASTIndicesStreamsAreWanted() {
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(IndexRegistration.unsplit(ORDERS_UUID, "orders", 1)
                .withFastSettings(250, true, 2));
        catalog.register(IndexRegistration.unsplit(LOGS_UUID, "logs", 1));

        assertThat(FastInterestSender.fastStreams(Set.of(ORDERS_0, ORDERS_1, LOGS_0), catalog))
                .containsExactlyInAnyOrder(ORDERS_0, ORDERS_1);
    }

    @Test
    void theWANTEDStreamsAreSentOnceToTheLeaderOfTheTermJoined() {
        Leader leader = new Leader();
        FastInterestSender sender = sender(leader);

        sender.send(Set.of(ORDERS_1, ORDERS_0), joined(3)::latest, false);
        sender.send(Set.of(ORDERS_1, ORDERS_0), joined(3)::latest, false);

        assertThat(leader.sent).as("once").singleElement().satisfies(s -> {
            assertThat(s.endpoint()).isEqualTo("l:1");
            assertThat(s.header().epoch()).isEqualTo(3);
            assertThat(s.header().senderUid()).isEqualTo("uid-w");
            assertThat(s.header().targetUid()).isEqualTo("uid-l");
            assertThat(s.what()).isEqualTo(new FastPublishFrame.Interest(
                    List.of(ORDERS_0, ORDERS_1), List.of()));
        });
    }

    @Test
    void aCHANGEIsSentAsItsDifferenceAndANewLeaderIsSentEverything() {
        Leader leader = new Leader();
        FastInterestSender sender = sender(leader);
        sender.send(Set.of(ORDERS_0), joined(3)::latest, false);

        sender.send(Set.of(ORDERS_1), joined(3)::latest, false);
        // ⚠️ review round 1, T1: the removal taken is not sent again.
        sender.send(Set.of(ORDERS_1), joined(3)::latest, false);
        sender.send(Set.of(ORDERS_1), joined(4)::latest, false);

        assertThat(leader.sent).extracting(s -> s.header().epoch() + " " + s.what())
                .containsExactly(
                        "3 " + new FastPublishFrame.Interest(List.of(ORDERS_0), List.of()),
                        "3 " + new FastPublishFrame.Interest(List.of(ORDERS_1),
                                List.of(ORDERS_0)),
                        "4 " + new FastPublishFrame.Interest(List.of(ORDERS_1), List.of()));
    }

    @Test
    void aMISSIsSentAgainOnlyOnceTheRetryIntervalPasses() {
        Leader leader = new Leader();
        FastInterestSender sender = sender(leader);
        leader.failing = true;
        sender.send(Set.of(ORDERS_0), joined(3)::latest, false);
        leader.failing = false;
        leader.refusing = true;
        now[0] += PAST_RETRY - 1;
        sender.send(Set.of(ORDERS_0), joined(3)::latest, false);
        assertThat(leader.sent).as("nothing inside the retry interval").hasSize(1);
        now[0] += 1;
        sender.send(Set.of(ORDERS_0), joined(3)::latest, false);
        leader.refusing = false;
        now[0] += PAST_RETRY;

        sender.send(Set.of(ORDERS_0), joined(3)::latest, false);
        sender.send(Set.of(ORDERS_0), joined(3)::latest, false);

        assertThat(leader.sent).as("failed, refused, then taken -- and no more")
                .extracting(Sent::what)
                .containsOnly(new FastPublishFrame.Interest(List.of(ORDERS_0), List.of()))
                .hasSize(3);
    }

    @Test
    void aPODThatLeadsOrJoinedNoTermOrWantsNothingSendsNothing() {
        Leader leader = new Leader();
        FastInterestSender sender = sender(leader);

        sender.send(Set.of(ORDERS_0), joined(3)::latest, true);
        sender.send(Set.of(ORDERS_0), new JoinedTerms()::latest, false);
        sender.send(Set.of(), joined(3)::latest, false);

        assertThat(leader.sent).isEmpty();
    }
}
