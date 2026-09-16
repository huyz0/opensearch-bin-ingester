// SPDX-License-Identifier: Apache-2.0
package binjava.ingest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import binjava.binstore.CountingBinStore;
import binjava.binstore.backend.MemoryBinStore;
import binjava.format.CommitDelta;
import binjava.format.RunCommit;
import binjava.format.RunKey;
import binjava.format.SegmentCommit;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * The server-side RESET signal (M5.15c, SPEC criterion 11's second half).
 *
 * <p>⚠️ THE SERVER SAYS IT, WHICH IS THE WHOLE POINT. A consumer whose session
 * the ingester no longer holds cannot tell that from a slow reply. Guessing
 * costs it either a full re-send it did not need or a resume against a session
 * that is gone -- and the second is a skip, silently.
 *
 * <p>⚠️ AND THE INTERESTING ASSERTION IS THE NEGATIVE ONE criterion 12 pairs
 * with it: a reset must not be readable as a sequencer failover. They are
 * different counters, and an operator whose failover metric counted resets
 * would see a failover every time a consumer reconnected after a restart.
 */
class SessionResetTest {

    private static final UUID INDEX = UUID.fromString("0b1e5f2a-1111-4222-8333-444455556666");
    private static final RunKey A = new RunKey(INDEX, 0);
    private static final RunKey B = new RunKey(INDEX, 1);

    private static final class Consumer implements SubscriptionHub.Subscriber {
        private final List<SubscriptionHub.Push> completed = new ArrayList<>();

        @Override
        public SegmentSink open(List<SubscriptionHub.Push> pushes) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            return (buffer, offset, length) -> out.write(buffer, offset, length);
        }

        @Override
        public void complete(List<SubscriptionHub.Push> pushes, SegmentSink sink) {
            completed.addAll(pushes);
        }
    }

    @Test
    void aResumeOnAResetSessionSaysRESENDFULLSTATE() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        Consumer consumer = new Consumer();

        try (SubscriptionHub.Session session = hub.openSession(consumer, List.of(A))) {
            session.reset();

            assertThatThrownBy(() -> session.resume(1L, List.of(B), List.of()))
                    .as("there is nothing left to apply a delta to, so the only answer that "
                            + "leaves the consumer correct is to re-establish -- refusing "
                            + "instead leaves it retrying a resume nothing can honour")
                    .isInstanceOf(SessionResetException.class)
                    .hasMessageContaining("FULL state");
        }
    }

    @Test
    void aRESETIsNotAPROTOCOLError() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        Consumer consumer = new Consumer();

        try (SubscriptionHub.Session session = hub.openSession(consumer, List.of(A))) {
            session.resume(5L, List.of(), List.of());

            // ⚠️ THE SESSION IS INTACT HERE and the REQUEST is wrong, which is
            // the opposite of a reset. Answering a client defect with "re-send
            // full state" would hide it: the consumer rebuilds, sends the same
            // malformed request again, and the loop looks like ordinary state
            // loss to everyone watching.
            assertThatThrownBy(() -> session.resume(4L, List.of(), List.of()))
                    .as("an epoch behind what the session served is a client defect, refused, "
                            + "not a reset")
                    .isInstanceOf(IllegalStateException.class)
                    .isNotInstanceOf(SessionResetException.class);
            assertThat(session.keys())
                    .as("and the session the refusal was about is still here")
                    .containsExactly(A);
        }
    }

    @Test
    void aRESETNamesASESSIONAndNOTHINGAboutTheSEQUENCER() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        Consumer consumer = new Consumer();

        SubscriptionHub.Session session = hub.openSession(consumer, List.of(A));
        session.reset();

        SessionResetException reset = (SessionResetException) org.assertj.core.api.Assertions
                .catchThrowable(() -> session.resume(1L, List.of(), List.of()));

        assertThat(reset.session())
                .as("it names the session to re-establish, which is what a client acts on")
                .isEqualTo(session.id());
        assertThat(reset.getMessage())
                .as("and it says nothing about the chain -- no epoch, no term, no lease. An "
                        + "operator whose failover metric matched on this text would count a "
                        + "failover every time a consumer reconnected after a restart")
                .doesNotContainIgnoringCase("epoch")
                .doesNotContainIgnoringCase("lease")
                .doesNotContainIgnoringCase("leaseholder")
                .doesNotContainIgnoringCase("term");
        session.close();
    }

    @Test
    void aSEQUENCERFailoverDoesNOTResetASession() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        Consumer consumer = new Consumer();

        try (SubscriptionHub.Session session = hub.openSession(consumer, List.of(A))) {
            hub.publish(delta(1, A, 3, 10L), "seg-1", segment(), serving(store));
            long epochBefore = session.epoch();

            // ⚠️ A FAILOVER IS A NEW LEASEHOLDER WRITING THE SAME CHAIN, and
            // what reaches this hub is simply the next delta -- a different
            // sequence, from a different pod. Nothing about it is a session
            // event, which is the property: if a failover reset sessions, every
            // consumer would re-send full state on every leadership change.
            hub.publish(delta(9, A, 2, 13L), "seg-9", segment(), serving(store));

            assertThat(session.epoch())
                    .as("the session epoch is untouched by anything the sequencer does -- they "
                            + "are different counters (criterion 12)")
                    .isEqualTo(epochBefore);
            assertThat(session.resume(1L, List.of(), List.of()).nextOffset())
                    .as("and the session resumes normally across it, carrying the offsets "
                            + "delivered on BOTH sides of the failover")
                    .isEqualTo(Map.of(A, 15L));
            assertThat(consumer.completed)
                    .as("PREMISE: both deltas really were delivered to this session")
                    .hasSize(2);
            // ⚠️ AND A RESET IS STILL AVAILABLE AND STILL DIFFERENT, which is
            // what stops this case surviving the deletion of the whole signal:
            // without it, "a failover does not reset" is satisfied by a tree in
            // which nothing can reset anything.
            session.reset();
            assertThatThrownBy(() -> session.resume(2L, List.of(), List.of()))
                    .as("the thing a failover did NOT do is a thing that exists")
                    .isInstanceOf(SessionResetException.class);
        }
    }

    @Test
    void aRESETLeavesTheConsumerRECEIVINGUntilItReEstablishes() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        Consumer consumer = new Consumer();

        try (SubscriptionHub.Session session = hub.openSession(consumer, List.of(A))) {
            session.reset();

            // ⚠️ THE RESET REALLY HAPPENED, asserted first. Without this the
            // case passes with `reset()` a no-op and cannot tell "the
            // registrations survived a reset" from "there was no reset".
            assertThatThrownBy(() -> session.resume(1L, List.of(), List.of()))
                    .as("PREMISE: the session is gone server-side")
                    .isInstanceOf(SessionResetException.class);

            hub.publish(delta(1, A, 1, 10L), "seg-1", segment(), serving(store));

            assertThat(consumer.completed)
                    .as("a reset is a SIGNAL, not a disconnect: tearing the registrations "
                            + "down would make it a silent gap as well, and the consumer is "
                            + "still there -- it is about to re-send full state")
                    .hasSize(1);
        }
    }

    /**
     * Re-establishing full state is the answer, and it is neither a duplicate
     * nor a gap (M5.15c, round 1's major).
     *
     * <p>⚠️ THE OBVIOUS ANSWER IS THE WRONG ONE, MEASURED: opening a NEW
     * session for the same consumer leaves the old registrations in place and
     * {@code subscribe} does not dedupe, so the consumer is handed every run
     * TWICE. Closing the old one first opens a window registered for nothing,
     * which is the gap the reset exists to avoid. Reconciling onto the handle
     * the consumer already holds touches only the difference.
     */
    @Test
    void reESTABLISHINGFullStateDeliversEachRunONCE() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        Consumer consumer = new Consumer();

        try (SubscriptionHub.Session session = hub.openSession(consumer, List.of(A))) {
            session.reset();
            session.reestablish(List.of(A, B));

            assertThat(hub.subscriberCount(A))
                    .as("A was carried across, so it is registered ONCE -- re-subscribing it "
                            + "is the duplicate this method exists to avoid")
                    .isEqualTo(1);
            assertThat(hub.subscriberCount(B))
                    .as("and B is new, so it is registered")
                    .isEqualTo(1);

            hub.publish(new CommitDelta(2, List.of(
                            new SegmentCommit("seg-2", List.of(new RunCommit(A, 1, 10L),
                                    new RunCommit(B, 1, 20L))))),
                    "seg-2", segment(), serving(store));

            assertThat(consumer.completed)
                    .as("each run arrives exactly once")
                    .hasSize(2);
            assertThat(session.epoch())
                    .as("and the session starts again at epoch 0: the ingester knows nothing "
                            + "about what this consumer received, so it must not pretend to")
                    .isZero();
            assertThat(session.resume(1L, List.of(), List.of()).nextOffset())
                    .as("the resume points are what the re-established session has delivered "
                            + "since, and nothing from before it")
                    .isEqualTo(Map.of(A, 11L, B, 21L));
        }
    }

    @Test
    void reESTABLISHINGWithFEWERStreamsUnregistersTheRest() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        Consumer consumer = new Consumer();

        try (SubscriptionHub.Session session = hub.openSession(consumer, List.of(A, B))) {
            session.reset();
            session.reestablish(List.of(A));

            assertThat(hub.subscriberCount(B))
                    .as("full state is FULL: a stream the consumer did not re-send is one it "
                            + "no longer holds, and leaving it registered pushes a consumer "
                            + "bytes for a stream it stopped following")
                    .isZero();
            assertThat(session.keys()).containsExactly(A);
        }
    }

    @Test
    void aRESETOfONESessionLeavesTheOthersALONE() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        Consumer one = new Consumer();
        Consumer two = new Consumer();

        try (SubscriptionHub.Session first = hub.openSession(one, List.of(A));
                SubscriptionHub.Session second = hub.openSession(two, List.of(B))) {
            first.reset();

            assertThat(second.keys())
                    .as("the registry is per session, and a reset that took its neighbours "
                            + "with it would turn one consumer's restart into a fleet-wide "
                            + "re-send")
                    .containsExactly(B);
            assertThatThrownBy(() -> first.resume(1L, List.of(), List.of()))
                    .isInstanceOf(SessionResetException.class);
        }
    }

    @Test
    void reESTABLISHINGAHeldSessionIsREFUSED() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        Consumer consumer = new Consumer();

        try (SubscriptionHub.Session session = hub.openSession(consumer, List.of(A))) {
            session.resume(3L, List.of(), List.of());

            assertThatThrownBy(() -> session.reestablish(List.of(B)))
                    .as("re-establishing is the answer to a RESET. Onto a session the "
                            + "registry still holds it discards the epoch and every resume "
                            + "point silently, and a consumer doing it has misread a refusal "
                            + "as a reset")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("still held");
            assertThat(session.epoch())
                    .as("and the session it refused to overwrite is intact")
                    .isEqualTo(3L);
            assertThat(hub.subscriberCount(B))
                    .as("with nothing registered by the refused call")
                    .isZero();
        }
    }

    private static CommitDelta delta(long sequence, RunKey key, int count, long first) {
        return new CommitDelta(sequence,
                List.of(new SegmentCommit("seg-" + sequence, List.of(
                        new RunCommit(key, count, first)))));
    }

    private static byte[] segment() {
        byte[] b = new byte[2048];
        for (int i = 0; i < b.length; i++) {
            b[i] = (byte) (i * 17 + 3);
        }
        return b;
    }

    private static SegmentServing serving(CountingBinStore store) {
        return new SegmentServing(
                new FetchPolicy(FetchPolicyConfig.defaultsFor(store.capabilities().costs())),
                store.capabilities(), new SegmentProxy(store));
    }
}
