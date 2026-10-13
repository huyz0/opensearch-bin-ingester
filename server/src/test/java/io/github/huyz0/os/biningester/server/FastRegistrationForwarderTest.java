// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import io.github.huyz0.os.biningester.sequencer.JoinedTerms;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * A writer forwards its fast indices' registrations to its leader (M13.83b;
 * ADR-0081 §4, ADR-0082 §2): every {@code wal=true} registration in this
 * pod's catalog, sent as a REGISTER to the leader of the newest term it
 * joined -- again on a change, a leader change or a failed send, never
 * otherwise, and never by the pod that leads.
 */
class FastRegistrationForwarderTest {

    private static final long PAST_RETRY = FastRegistrationForwarder.RETRY_INTERVAL.toNanos();

    /** The monotonic clock the forwarder reads. */
    private final long[] now = {0};

    private static final IndexRegistration ORDERS = IndexRegistration.unsplit(
            "nVzgup36TLqWp7VBBREj1w", "orders", 1).withFastSettings(250, true, 2);
    private static final IndexRegistration LOGS = IndexRegistration.unsplit(
            "Qk1rUnfdRyCrk1L0DLaXEg", "logs", 1);

    /** One REGISTER as it went out: where, under which term, from and to whom, and what. */
    private record Sent(String endpoint, FastFrame.Header header, IndexRegistration what) {
    }

    private static final class Leader {
        final List<Sent> sent = new ArrayList<>();
        boolean failing;
        boolean refusing;

        byte[] exchange(String endpoint, byte[] frame) throws IOException {
            FastFrame.Frame f = FastFrame.decode(frame);
            sent.add(new Sent(endpoint, f.header(),
                    ((FastFrame.Register) f.body()).registration()));
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

    @Test
    void eachFASTRegistrationIsSentOnceToTheLeaderOfTheTermJoined() throws Exception {
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(ORDERS);
        catalog.register(LOGS);
        Leader leader = new Leader();
        FastRegistrationForwarder forwarder = new FastRegistrationForwarder("uid-w",
                leader::exchange, () -> now[0]);

        forwarder.forward(catalog, joined(3)::latest, false);
        forwarder.forward(catalog, joined(3)::latest, false);

        assertThat(leader.sent).as("the fast index only, and once").singleElement()
                .satisfies(s -> {
                    assertThat(s.endpoint()).isEqualTo("l:1");
                    assertThat(s.header().epoch()).isEqualTo(3);
                    assertThat(s.header().senderUid()).isEqualTo("uid-w");
                    assertThat(s.header().targetUid()).isEqualTo("uid-l");
                    assertThat(s.what()).isEqualTo(ORDERS);
                });
    }

    @Test
    void aCHANGEDRegistrationOrANewLeaderIsSentAgain() throws Exception {
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(ORDERS);
        Leader leader = new Leader();
        FastRegistrationForwarder forwarder = new FastRegistrationForwarder("uid-w",
                leader::exchange, () -> now[0]);
        forwarder.forward(catalog, joined(3)::latest, false);

        IndexRegistration raised = ORDERS.withFastSettings(250, true, 3);
        catalog.register(raised);
        forwarder.forward(catalog, joined(3)::latest, false);
        forwarder.forward(catalog, joined(4)::latest, false);

        assertThat(leader.sent).extracting(s -> s.header().epoch() + ":" + s.what().walQuorum())
                .containsExactly("3:2", "3:3", "4:3");
    }

    @Test
    void aSENDThatFailedOrWasRefusedIsSentAgain() throws Exception {
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(ORDERS);
        Leader leader = new Leader();
        FastRegistrationForwarder forwarder = new FastRegistrationForwarder("uid-w",
                leader::exchange, () -> now[0]);
        leader.failing = true;
        forwarder.forward(catalog, joined(3)::latest, false);
        leader.failing = false;
        leader.refusing = true;
        now[0] += PAST_RETRY;
        forwarder.forward(catalog, joined(3)::latest, false);
        leader.refusing = false;
        now[0] += PAST_RETRY;

        forwarder.forward(catalog, joined(3)::latest, false);
        forwarder.forward(catalog, joined(3)::latest, false);

        assertThat(leader.sent).as("failed, refused, then taken -- and no more").hasSize(3);
    }

    @Test
    void aPODThatLeadsOrJoinedNoTermForwardsNothing() throws Exception {
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(ORDERS);
        Leader leader = new Leader();
        FastRegistrationForwarder forwarder = new FastRegistrationForwarder("uid-w",
                leader::exchange, () -> now[0]);

        forwarder.forward(catalog, joined(3)::latest, true);
        forwarder.forward(catalog, new JoinedTerms()::latest, false);

        assertThat(leader.sent).isEmpty();
    }

    @Test
    void aMISSStopsThePassAndNothingIsSentUntilTheRetryInterval() throws Exception {
        // ⚠️ review round 1, P1/P2/T1: the drain runs per batch ack, and each
        // miss can wait a whole exchange timeout.
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(ORDERS);
        catalog.register(LOGS.withFastSettings(250, true, 2));
        Leader leader = new Leader();
        FastRegistrationForwarder forwarder = new FastRegistrationForwarder("uid-w",
                leader::exchange, () -> now[0]);
        leader.failing = true;

        forwarder.forward(catalog, joined(3)::latest, false);
        assertThat(leader.sent).as("the first miss ends the pass").hasSize(1);
        now[0] += PAST_RETRY - 1;
        forwarder.forward(catalog, joined(3)::latest, false);
        assertThat(leader.sent).as("nothing inside the retry interval").hasSize(1);

        leader.failing = false;
        now[0] += 1;
        forwarder.forward(catalog, joined(3)::latest, false);
        forwarder.forward(catalog, joined(3)::latest, false);

        assertThat(leader.sent.subList(1, leader.sent.size()))
                .extracting(s -> s.what().indexName())
                .as("both, once each, after the interval")
                .containsExactlyInAnyOrder("orders", "logs");
    }

    @Test
    void aREGISTRATIONTooLargeForAREGISTERIsNeverSentAndHoldsBackNoOther() throws Exception {
        // ⚠️ review round 1, T2.
        List<String> aliases = new ArrayList<>();
        for (int i = 0; i < 5_000; i++) {
            aliases.add("alias-number-" + i);
        }
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(new IndexRegistration("Qk1rUnfdRyCrk1L0DLaXEg", "large", aliases,
                3, 3, 1, 1).withFastSettings(250, true, 2));
        catalog.register(ORDERS);
        Leader leader = new Leader();
        FastRegistrationForwarder forwarder = new FastRegistrationForwarder("uid-w",
                leader::exchange, () -> now[0]);

        forwarder.forward(catalog, joined(3)::latest, false);

        assertThat(leader.sent).extracting(s -> s.what().indexName()).containsExactly("orders");
    }
}
