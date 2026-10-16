// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.FastFrame;
import io.github.huyz0.os.biningester.format.IndexRegistration;
import io.github.huyz0.os.biningester.ingest.IndexCatalog;
import io.github.huyz0.os.biningester.sequencer.JoinedTerms;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * A writer tells its leader when a fast index it forwarded leaves fast mode
 * (M13.34ad; ADR-0082 §2): the {@code wal=false} registration is sent once,
 * to the leader that took the fast one -- and a default index never told
 * as fast is still never sent.
 */
class FastRegistrationForwarderSwitchTest {

    private static final IndexRegistration DEFAULT = IndexRegistration.unsplit(
            "nVzgup36TLqWp7VBBREj1w", "orders", 1);
    private static final IndexRegistration FAST = DEFAULT.withFastSettings(250, true, 2);

    private static JoinedTerms joined(long epoch) {
        JoinedTerms joined = new JoinedTerms();
        joined.joined(epoch, 0, "uid-l", "l:1");
        return joined;
    }

    private final long[] now = {0};
    private final List<IndexRegistration> sent = new ArrayList<>();
    private boolean refusing;

    private FastRegistrationForwarder forwarder() {
        return new FastRegistrationForwarder("uid-w", (endpoint, frame) -> {
            FastFrame.Frame f = FastFrame.decode(frame);
            sent.add(((FastFrame.Register) f.body()).registration());
            FastFrame.Body answer = refusing
                    ? new FastFrame.Refused(FastFrame.Reason.NOT_FAST, java.util.Optional.empty(),
                            "no")
                    : new FastFrame.HeldStatusReport(FastFrame.HeldStatus.NONE);
            return FastFrame.encode(f.header().epoch(), "uid-l", "uid-w", answer);
        }, () -> now[0]);
    }

    @Test
    void aREFUSEDSwitchIsSentAgainOnlyAfterTheRetryInterval() throws Exception {
        FastRegistrationForwarder forwarder = forwarder();
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(FAST);
        forwarder.forward(catalog, joined(3)::latest, false);
        catalog.register(DEFAULT);
        refusing = true;
        forwarder.forward(catalog, joined(3)::latest, false);
        forwarder.forward(catalog, joined(3)::latest, false);
        assertThat(sent).as("refused once, held back").containsExactly(FAST, DEFAULT);

        refusing = false;
        now[0] += FastRegistrationForwarder.RETRY_INTERVAL.toNanos();
        forwarder.forward(catalog, joined(3)::latest, false);
        forwarder.forward(catalog, joined(3)::latest, false);

        assertThat(sent).as("sent again, then taken").containsExactly(FAST, DEFAULT, DEFAULT);
    }

    @Test
    void anINDEXBackToFastIsSentAgain() throws Exception {
        FastRegistrationForwarder forwarder = forwarder();
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(FAST);
        forwarder.forward(catalog, joined(3)::latest, false);
        catalog.register(DEFAULT);
        forwarder.forward(catalog, joined(3)::latest, false);

        catalog.register(FAST);
        forwarder.forward(catalog, joined(3)::latest, false);

        assertThat(sent).containsExactly(FAST, DEFAULT, FAST);
    }

    @Test
    void aNEWLeaderIsNeverSentASwitchOfAnIndexItNeverTookAsFast() throws Exception {
        FastRegistrationForwarder forwarder = forwarder();
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(FAST);
        forwarder.forward(catalog, joined(3)::latest, false);
        catalog.register(DEFAULT);

        forwarder.forward(catalog, joined(4)::latest, false);

        assertThat(sent).as("term 4's leader took nothing fast").containsExactly(FAST);
    }

    @Test
    void aFORWARDEDFastIndexsSwitchToTheDefaultPathIsSentOnce() throws Exception {
        FastRegistrationForwarder forwarder = forwarder();
        IndexCatalog catalog = new IndexCatalog();
        catalog.register(FAST);
        forwarder.forward(catalog, joined(3)::latest, false);

        catalog.register(DEFAULT);
        forwarder.forward(catalog, joined(3)::latest, false);
        forwarder.forward(catalog, joined(3)::latest, false);

        assertThat(sent).containsExactly(FAST, DEFAULT);
    }
}
