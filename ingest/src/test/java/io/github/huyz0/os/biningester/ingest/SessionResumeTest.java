// SPDX-License-Identifier: Apache-2.0
package io.github.huyz0.os.biningester.ingest;

import io.github.huyz0.os.biningester.binstore.IndexCostLedger;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.huyz0.os.biningester.binstore.CountingBinStore;
import io.github.huyz0.os.biningester.binstore.backend.MemoryBinStore;
import io.github.huyz0.os.biningester.format.CommitDelta;
import io.github.huyz0.os.biningester.format.RunCommit;
import io.github.huyz0.os.biningester.format.RunKey;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A session resumes at the NEXT record — no duplicate, no skip (M5.15b, SPEC
 * criterion 11's first half).
 *
 * <p>⚠️ "THE NEXT RECORD" IS THE PROPERTY, and "roughly where it was" is the
 * answer it exists to refuse. Both failure directions are real and neither is
 * loud: one past the last delivered offset re-delivers records the consumer
 * already applied, and one short of it loses a window the consumer will never
 * ask for again.
 *
 * <p>⚠️ THE RESUME POINT IS WHAT WAS DELIVERED, NOT WHAT WAS PUSHED. A sink
 * that threw mid-stream is not completed, so its session must not advance past
 * a segment it received only a prefix of.
 */
class SessionResumeTest {

    private static final UUID INDEX = UUID.fromString("0b1e5f2a-1111-4222-8333-444455556666");
    private static final RunKey A = new RunKey(INDEX, 0);
    private static final RunKey B = new RunKey(INDEX, 1);
    private static final RunKey C = new RunKey(INDEX, 2);

    /** A subscriber that takes everything and remembers the pushes it completed. */
    private static final class Consumer implements SubscriptionHub.Subscriber {
        private final List<SubscriptionHub.Push> completed = new ArrayList<>();
        private final boolean breaks;

        Consumer(boolean breaks) {
            this.breaks = breaks;
        }

        @Override
        public SegmentSink open(List<SubscriptionHub.Push> pushes) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            return (buffer, offset, length) -> {
                if (breaks) {
                    throw new java.io.IOException("this consumer takes a prefix and dies");
                }
                out.write(buffer, offset, length);
            };
        }

        @Override
        public void complete(List<SubscriptionHub.Push> pushes, SegmentSink sink) {
            completed.addAll(pushes);
        }
    }

    @Test
    void aRESUMEAnswersONEPastTheLastOffsetDELIVEREDOnEveryStreamItKeeps() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        Consumer consumer = new Consumer(false);

        try (SubscriptionHub.Session session = hub.openSession(consumer, List.of(A, B))) {
            assertThat(session.epoch())
                    .as("PREMISE: a fresh session starts at epoch 0, so the first resume is 1")
                    .isZero();

            // A carries 3 records from offset 10, B carries 2 from offset 100.
            hub.publish(new CommitDelta(1, "seg-1",
                            List.of(new RunCommit(A, 3, 10L), new RunCommit(B, 2, 100L))),
                    "seg-1", segment(), serving(store));

            assertThat(consumer.completed)
                    .as("PREMISE: both streams really were delivered, or the offsets below "
                            + "are answers about nothing")
                    .hasSize(2);

            SubscriptionHub.Resumed resumed = session.resume(1L, List.of(C), List.of(B));

            assertThat(resumed.nextOffset())
                    .as("A resumes ONE PAST its last delivered offset -- 10+3-1 is 12, so 13. "
                            + "12 re-delivers the last record, 14 skips one, and the consumer "
                            + "can detect neither. B is GONE because the resume removed it, "
                            + "and C is ABSENT because nothing has been delivered on it -- 0 "
                            + "would be a real offset pointing at a run that may be long gone")
                    .isEqualTo(Map.of(A, 13L));
            assertThat(resumed.sessionEpoch())
                    .as("and the answer carries the epoch it served")
                    .isEqualTo(1L);
            assertThat(session.keys())
                    .as("the session now holds exactly the streams the delta left it")
                    .containsExactly(A, C);
        }
    }

    @Test
    void aRETRIEDResumeAtTheSameEpochIsANSWEREDNotREAPPLIED() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        Consumer consumer = new Consumer(false);

        try (SubscriptionHub.Session session = hub.openSession(consumer, List.of(A))) {
            hub.publish(new CommitDelta(1, "seg-1", List.of(new RunCommit(A, 3, 10L))),
                    "seg-1", segment(), serving(store));

            SubscriptionHub.Resumed first = session.resume(1L, List.of(B), List.of());

            // ⚠️ THE WORLD MOVES BETWEEN THE ANSWER AND THE RETRY, which is
            // what makes this case constrain anything. A retry that merely
            // recomputes would now answer with B's offset too -- a DIFFERENT
            // answer to the same request -- and the consumer would resume B
            // from a point it never asked about. Re-applying the key deltas is
            // idempotent by itself, so a fixture that only resent them would
            // pass with the epoch check deleted; this one does not.
            hub.publish(new CommitDelta(2, "seg-2", List.of(new RunCommit(B, 2, 500L))),
                    "seg-2", segment(), serving(store));
            SubscriptionHub.Resumed retry = session.resume(1L, List.of(B), List.of());

            assertThat(retry)
                    .as("a resume's reply can be lost, and a consumer that resends must get "
                            + "the SAME answer -- recomputing it against newer state is what "
                            + "the epoch exists to prevent (KIP-227)")
                    .isEqualTo(first);
            assertThat(first.nextOffset())
                    .as("PREMISE: the first answer did NOT carry B, so a recomputed retry "
                            + "would visibly differ from it")
                    .containsOnlyKeys(A);
            assertThat(session.keys())
                    .as("and the key set is applied ONCE, not twice")
                    .containsExactly(A, B);
        }
    }

    @Test
    void aResumeBEHINDTheEpochAlreadyServedIsREFUSED() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        Consumer consumer = new Consumer(false);

        try (SubscriptionHub.Session session = hub.openSession(consumer, List.of(A))) {
            session.resume(4L, List.of(), List.of());

            assertThatThrownBy(() -> session.resume(3L, List.of(), List.of(A)))
                    .as("a reordered or replayed request is not a retry, and answering it "
                            + "would move the session backwards -- here it would also drop a "
                            + "stream the consumer still holds")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("backwards");
            assertThat(session.keys())
                    .as("and the refused request applied none of its deltas")
                    .containsExactly(A);
        }
    }

    @Test
    void aConsumerThatTOOKAPREFIXDoesNotAdvanceItsResumePoint() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        Consumer broken = new Consumer(true);

        try (SubscriptionHub.Session session = hub.openSession(broken, List.of(A))) {
            hub.publish(new CommitDelta(1, "seg-1", List.of(new RunCommit(A, 3, 10L))),
                    "seg-1", segment(), serving(store));

            assertThat(broken.completed)
                    .as("PREMISE: the sink threw, so nothing was completed for it")
                    .isEmpty();
            assertThat(session.resume(1L, List.of(), List.of()).nextOffset())
                    .as("the session must NOT advance past a segment this consumer received "
                            + "only a prefix of -- recording at push time instead of after "
                            + "`complete` answers 13 here and the window is lost silently")
                    .isEmpty();
        }
    }

    /**
     * The key deltas change what is DELIVERED, not only what is bookkept
     * (M5.15b, the second half of criterion 11's first half).
     *
     * <p>⚠️ THE OTHER CASES READ THE REGISTRY, NOT THE HUB, which review
     * measured: with the registration rebuild in {@code Session.resume} deleted
     * — both loops, separately — `session.keys()` still answers correctly and
     * every case stayed green, because nothing published AFTER a resume. A
     * session that bookkeeps a stream it is not subscribed for is the silent
     * version of this feature doing nothing at all.
     */
    @Test
    void aResumesDELTASChangeWhatIsDELIVERED() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        Consumer consumer = new Consumer(false);

        try (SubscriptionHub.Session session = hub.openSession(consumer, List.of(A, B))) {
            session.resume(1L, List.of(C), List.of(B));

            assertThat(hub.subscriberCount(C))
                    .as("the added stream is REGISTERED, not merely recorded")
                    .isEqualTo(1);
            assertThat(hub.subscriberCount(B))
                    .as("and the removed one is unregistered, or the consumer keeps being "
                            + "handed a stream it told the hub to stop sending")
                    .isZero();

            hub.publish(new CommitDelta(2, "seg-2",
                            List.of(new RunCommit(B, 1, 700L), new RunCommit(C, 1, 800L))),
                    "seg-2", segment(), serving(store));

            assertThat(consumer.completed)
                    .as("C arrives because the resume added it, B does not because the resume "
                            + "removed it -- and this is the assertion the registry's own "
                            + "bookkeeping cannot make")
                    .extracting(SubscriptionHub.Push::key)
                    .containsExactly(C);
            assertThat(session.resume(2L, List.of(), List.of()).nextOffset())
                    .as("and the delivery moved C's resume point, which is the two halves "
                            + "agreeing: registration and bookkeeping are about the same stream")
                    .isEqualTo(Map.of(C, 801L));
        }
    }

    /**
     * A session issues no object-store request of its own.
     *
     * <p>⚠️ THE FIXTURES WERE BUILT AND NEVER ASSERTED ON, which review named:
     * every other case in this file constructs a {@code CountingBinStore} and
     * reads nothing off it, so the claim that a session is pure bookkeeping was
     * prose. A segment this pod HOLDS is served from memory, so the whole
     * exchange must cost zero.
     */
    @Test
    void aSessionAndItsRESUMECostNOObjectStoreRequest() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        Consumer consumer = new Consumer(false);
        long before = store.counts().total();

        try (SubscriptionHub.Session session = hub.openSession(consumer, List.of(A))) {
            hub.publish(new CommitDelta(1, "seg-1", List.of(new RunCommit(A, 3, 10L))),
                    "seg-1", segment(), serving(store));
            session.resume(1L, List.of(B), List.of());
            session.resume(2L, List.of(), List.of(B));
        }

        assertThat(store.counts().total() - before)
                .as("opening, delivering from held bytes, resuming twice and closing is all "
                        + "in-memory -- a session that reached the store would put a request "
                        + "on the reconnect path, which is a consumer-driven rate")
                .isZero();
    }

    /**
     * The refusal branches this task added are each pinned (M5.15b, round 2).
     *
     * <p>⚠️ FIVE FIXES FROM ROUND 1 LANDED UNPINNED, which round 2 measured:
     * reverting each -- the request comparison to epoch-only keying, the
     * second-open refusal, the rollback, {@code Math::max}, the {@code
     * retainAll} -- left all 263 ingest tests green. Every one of them exists
     * because review called the behaviour it prevents a defect, so a one-line
     * revert brings that defect back with nothing to say so. This case and the
     * three below are what stop that.
     */
    @Test
    void aDIFFERENTRequestAtAnAlreadySERVEDEpochIsREFUSED() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        Consumer consumer = new Consumer(false);

        try (SubscriptionHub.Session session = hub.openSession(consumer, List.of(A))) {
            session.resume(1L, List.of(B), List.of());

            assertThatThrownBy(() -> session.resume(1L, List.of(C), List.of()))
                    .as("a retry resends the SAME request; anything else at an epoch already "
                            + "served is a protocol error. Keyed on the epoch alone this "
                            + "returns the old answer and drops the deltas silently -- the "
                            + "consumer believes its `add` took effect and waits forever")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("different deltas");
            assertThat(hub.subscriberCount(C))
                    .as("and the refused request registered nothing, or the rollback is what "
                            + "is missing rather than the refusal")
                    .isZero();
            assertThat(session.keys())
                    .as("the session is exactly what the accepted request left it")
                    .containsExactly(A, B);
        }
    }

    @Test
    void aSECONDSessionOnOneSubscriberIsREFUSED() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        Consumer consumer = new Consumer(false);

        try (SubscriptionHub.Session first = hub.openSession(consumer, List.of(A))) {
            assertThatThrownBy(() -> hub.openSession(consumer, List.of(B)))
                    .as("the identity mapping holds ONE session per subscriber, and "
                            + "overwriting it leaves the first recording nothing -- it would "
                            + "then resume BEHIND the truth, which is a duplicate with no "
                            + "symptom")
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("already holds session");
            assertThat(first.keys())
                    .as("and the first session is untouched by the refusal")
                    .containsExactly(A);
        }
    }

    @Test
    void anOUTOFORDERDeliveryDoesNotMoveAResumePointBACKWARDS() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        Consumer consumer = new Consumer(false);

        try (SubscriptionHub.Session session = hub.openSession(consumer, List.of(A))) {
            hub.publish(new CommitDelta(1, "seg-late", List.of(new RunCommit(A, 2, 900L))),
                    "seg-late", segment(), serving(store));
            hub.publish(new CommitDelta(2, "seg-early", List.of(new RunCommit(A, 2, 100L))),
                    "seg-early", segment(), serving(store));

            assertThat(session.resume(1L, List.of(), List.of()).nextOffset())
                    .as("the HIGHEST offset delivered wins, not the LATEST delivery. Taking "
                            + "the latest answers 102 here and re-delivers everything from "
                            + "there to 901 -- the duplicate half of the criterion, and a "
                            + "resume point that depends on push ordering is not a property")
                    .isEqualTo(Map.of(A, 902L));
        }
    }

    @Test
    void aStreamREMOVEDAndREADDEDResumesAsIfNEWNotWhereItLeftOff() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        Consumer consumer = new Consumer(false);

        try (SubscriptionHub.Session session = hub.openSession(consumer, List.of(A))) {
            hub.publish(new CommitDelta(1, "seg-1", List.of(new RunCommit(A, 3, 10L))),
                    "seg-1", segment(), serving(store));

            session.resume(1L, List.of(), List.of(A));
            assertThat(session.resume(2L, List.of(A), List.of()).nextOffset())
                    .as("a consumer that dropped a stream and asked for it again is asking "
                            + "fresh: keeping the old offset answers 13 for a stream this "
                            + "session stopped following, which is a claim about records "
                            + "nobody was tracking")
                    .isEmpty();
        }
    }

    @Test
    void aREFUSEDResumeCLOSESTheRegistrationsItOpenedEARLY() throws Exception {
        SubscriptionHub hub = new SubscriptionHub();
        Consumer consumer = new Consumer(false);

        try (SubscriptionHub.Session session = hub.openSession(consumer, List.of(A))) {
            session.resume(5L, List.of(), List.of());

            // ⚠️ THE ADD IS NOT EMPTY, which is what makes this different from
            // the other refusal case: an added stream is registered BEFORE the
            // registry is told, so a refusal must take that registration back.
            assertThatThrownBy(() -> session.resume(4L, List.of(B), List.of()))
                    .isInstanceOf(IllegalStateException.class);

            assertThat(hub.subscriberCount(B))
                    .as("a refused resume applies NONE of its deltas, and a registration is a "
                            + "delta the consumer was never told about -- left open, the hub "
                            + "pushes a stream no session holds and nothing will ever close it")
                    .isZero();
        }
    }

    @Test
    void aKeyLISTEDTWICEIsRegisteredONCE() throws Exception {
        CountingBinStore store = new CountingBinStore(new MemoryBinStore());
        SubscriptionHub hub = new SubscriptionHub();
        Consumer consumer = new Consumer(false);

        try (SubscriptionHub.Session session = hub.openSession(consumer, List.of(A, A))) {
            assertThat(hub.subscriberCount(A))
                    .as("the registry dedupes and the session did not, so `[A, A]` subscribed "
                            + "twice against one registry entry")
                    .isEqualTo(1);

            hub.publish(new CommitDelta(1, "seg-1", List.of(new RunCommit(A, 1, 10L))),
                    "seg-1", segment(), serving(store));

            assertThat(consumer.completed)
                    .as("and the consumer is handed the run ONCE -- twice is the duplicate "
                            + "delivery this whole task exists to make impossible")
                    .hasSize(1);
        }
    }

    private static byte[] segment() {
        byte[] b = new byte[4096];
        for (int i = 0; i < b.length; i++) {
            b[i] = (byte) (i * 31 + 7);
        }
        return b;
    }

    private static SegmentServing serving(CountingBinStore store) {
        return new SegmentServing(
                new FetchPolicy(FetchPolicyConfig.defaultsFor(store.capabilities().costs())),
                store.capabilities(), new SegmentProxy(store, SegmentProxy.DEFAULT_CHUNK_BYTES, new SegmentCache(0), new IndexCostLedger()));
    }
}
