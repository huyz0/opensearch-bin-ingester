// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.server;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.huyz0.os.biningester.format.CatchUpEndFrame;
import io.github.huyz0.os.biningester.format.CatchUpEventFrame;
import io.github.huyz0.os.biningester.format.CatchUpRequestFrame;
import io.github.huyz0.os.biningester.format.FetchMode;
import io.github.huyz0.os.biningester.format.RunKey;
import io.github.huyz0.os.biningester.format.SubscriptionEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A catch-up past the committed chain (M13.32c; ADR-0081 §6): after the chain's
 * replay, each stream's exposed journal entries from the offset that replay
 * reached -- the next after its last run, or the request's start when it sent
 * none -- before the end frame; and none once the lease forbids exposing.
 */
class FastCatchUpTest {

    private static final UUID REQUEST = new UUID(7, 7);
    private static final RunKey A = new RunKey(new UUID(1, 1), 0);
    private static final RunKey B = new RunKey(new UUID(1, 1), 1);

    private static SubscriptionEvent event(RunKey key, long first, int count) {
        return new SubscriptionEvent(REQUEST.toString(), 0, 1, key, "k" + first, first, count,
                FetchMode.INLINE, new byte[] {1}, null, SubscriptionEvent.RANGE_ABSENT,
                SubscriptionEvent.RANGE_ABSENT, SubscriptionEvent.CHAIN_SEQUENCE_ABSENT);
    }

    /** Each frame written, as "chain:A@10", "journal:A@12" or "end". */
    private static String read(byte[] frame) throws Exception {
        try {
            CatchUpEventFrame f = CatchUpEventFrame.decode(frame);
            // ⚠️ its review round 1, T4: under the request's own id, or the
            // plugin drops it.
            assertThat(f.requestId()).isEqualTo(REQUEST);
            SubscriptionEvent e = f.event();
            return e.segmentKey().startsWith("j") ? "journal:" + (e.key().equals(A) ? "A" : "B")
                    + "@" + e.firstOffset() : "chain:" + (e.key().equals(A) ? "A" : "B") + "@"
                    + e.firstOffset();
        } catch (java.io.IOException notAnEvent) {
            CatchUpEndFrame.decode(frame);
            return "end";
        }
    }

    private static List<String> respond(boolean exposable, List<String> asked) throws Exception {
        return respond(() -> exposable, asked, 1);
    }

    private static List<String> respond(java.util.function.BooleanSupplier exposable,
            List<String> asked, int perStream) throws Exception {
        CatchUpRequestFrame request = new CatchUpRequestFrame(REQUEST,
                List.of(new CatchUpRequestFrame.Stream(A, 10),
                        new CatchUpRequestFrame.Stream(B, 30)));
        List<String> written = new ArrayList<>();
        FastCatchUp.respond(request, frame -> {
            try {
                written.add(read(frame));
            } catch (Exception unreadable) {
                throw new java.io.IOException(unreadable);
            }
        }, (r, sink) -> {
            sink.write(new CatchUpEventFrame(REQUEST, event(A, 10, 2)).encode());
            sink.write(new CatchUpEndFrame(REQUEST).encode());
        }, (session, key, from) -> {
            asked.add((key.equals(A) ? "A" : "B") + "@" + from);
            List<SubscriptionEvent> js = new ArrayList<>();
            for (int i = 0; i < perStream; i++) {
                js.add(new SubscriptionEvent(session, 0, 1, key, "j" + (from + i), from + i, 1,
                        FetchMode.INLINE, new byte[] {2}, null, SubscriptionEvent.RANGE_ABSENT,
                        SubscriptionEvent.RANGE_ABSENT, SubscriptionEvent.CHAIN_SEQUENCE_ABSENT));
            }
            return js;
        }, exposable);
        return written;
    }

    @Test
    void theJOURNALsEventsFollowTheChainsFromWhereItEndedBeforeTheEndFrame() throws Exception {
        List<String> asked = new ArrayList<>();

        List<String> written = respond(true, asked);

        assertThat(asked).as("A after its run [10, 12); B from its start, sent nothing")
                .containsExactlyInAnyOrder("A@12", "B@30");
        assertThat(written.get(0)).isEqualTo("chain:A@10");
        assertThat(written.subList(1, 3)).containsExactlyInAnyOrder("journal:A@12",
                "journal:B@30");
        assertThat(written.get(3)).isEqualTo("end");
    }

    @Test
    void noJOURNALEventIsWrittenOnceTheLeaseForbidsAndTheEndFrameStill() throws Exception {
        List<String> written = respond(false, new ArrayList<>());

        assertThat(written).containsExactly("chain:A@10", "end");
    }

    @Test
    void theLEASEIsAskedBeforeEachJournalEventAndNoneFollowsANo() throws Exception {
        // ⚠️ its review round 1, T1: yes, then no, then yes -- the event after
        // the no is never written, though the lease says yes again.
        java.util.Iterator<Boolean> answers = List.of(true, false, true, true, true).iterator();

        List<String> written = respond(answers::next, new ArrayList<>(), 2);

        assertThat(written).filteredOn(w -> w.startsWith("journal:")).hasSize(1);
        assertThat(written.get(written.size() - 1)).isEqualTo("end");
    }
}
